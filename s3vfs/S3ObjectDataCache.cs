using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace s3vfs
{
    public partial class S3ObjectNode
    {
        public class S3ObjectDataCache : IS3ObjectData
        {
            private const int MEMCACHE_BLOCKSIZE = 64 * S3Filesystem.ALLOCATION_UNIT;
            private byte[] ZERO_BLOCK = new byte[MEMCACHE_BLOCKSIZE];

            private Dictionary<UInt64, Block> memoryCache = new();
            private S3ObjectNode s3Node;
            private AmazonS3Client s3Client;
            private readonly SemaphoreSlim persistingSemaphore = new SemaphoreSlim(1, 1);

            internal S3ObjectDataCache(S3ObjectNode s3Node, AmazonS3Client s3Client)
            {
                this.s3Node = s3Node;
                this.s3Client = s3Client;
                var fileInfo = s3Node.GetFileInfo();
                AllocatedSize = fileInfo.AllocationSize;
                OriginalSize = s3Node.Status == S3NodeStatus.New ? 0 : fileInfo.FileSize;
            }

            public ulong OriginalSize { get; }

            public ulong FileSize
            {
                get => s3Node.Size;
                set { s3Node.Size = value; }
            }

            public ulong AllocatedSize { get; private set; }

            public byte[] Read(UInt64 offset, UInt32 length)
            {
                var requestedLargeBlock = RoundToBlocksize(offset, length);
                var missingBlocks = CalculateMissingBlocks(requestedLargeBlock);

                foreach (var missingBlock in missingBlocks)
                {
                    var downloadedBlock = DownloadBlock(missingBlock).Result;
                    memoryCache.Add(downloadedBlock.Offset, downloadedBlock);
                }

                return ReadFromMemoryCache(offset, length);
            }

            public void Write(ulong offset, byte[] data)
            {
                uint blockIndex = (uint) (offset / MEMCACHE_BLOCKSIZE);
                uint blockOffset = (uint) (offset % MEMCACHE_BLOCKSIZE);
                uint remainingLength = (uint)data.Length;
                int dataOffset = 0;
                while (remainingLength > 0)
                {
                    uint blockBytes = Math.Min(remainingLength, MEMCACHE_BLOCKSIZE - blockOffset);
                    byte[] targetData = null;
                    if (memoryCache.TryGetValue(blockIndex * MEMCACHE_BLOCKSIZE, out Block cacheBlock))
                    {
                        targetData = cacheBlock.Data ??= new byte[MEMCACHE_BLOCKSIZE];
                    }

                    if (targetData == null)
                    {
                        var missingBlock = new Block() { Offset = blockIndex * MEMCACHE_BLOCKSIZE, Length = MEMCACHE_BLOCKSIZE };
                        if (blockOffset > 0 || (blockBytes < MEMCACHE_BLOCKSIZE && blockIndex * MEMCACHE_BLOCKSIZE + blockBytes < FileSize))
                        {
                            missingBlock = DownloadBlock(missingBlock).Result;
                        }
                        memoryCache.Add(missingBlock.Offset, missingBlock);
                        targetData = missingBlock.Data ??= new byte[MEMCACHE_BLOCKSIZE];
                    }

                    Buffer.BlockCopy(data, dataOffset, targetData, (int)blockOffset, (int)blockBytes);

                    remainingLength -= blockBytes;
                    blockIndex++;
                    blockOffset = 0;
                    dataOffset += (int)blockBytes;
                }
            }

            public void SetFileSize(UInt64 size, bool setAllocatedSize)
            {
                var prevAllocatedSize = AllocatedSize;
                var prevFileSize = FileSize;
                if (setAllocatedSize)
                {
                    AllocatedSize = size;
                    FileSize = Math.Min(FileSize, AllocatedSize);
                }
                else
                {
                    FileSize = size;
                    AllocatedSize = ((size + S3Filesystem.ALLOCATION_UNIT - 1) / S3Filesystem.ALLOCATION_UNIT) * S3Filesystem.ALLOCATION_UNIT;
                }

                if (AllocatedSize < prevAllocatedSize)
                {
                    if (AllocatedSize == 0) memoryCache.Clear();
                    else memoryCache.RemoveAll(e => e.Key >= AllocatedSize);
                }

                if (prevFileSize != FileSize && s3Node.Status == S3NodeStatus.Active)
                {
                    s3Node.Status = S3NodeStatus.Modified;
                }
            }

            public async Task Persist()
            {
                if (await persistingSemaphore.WaitAsync(-1))
                {
                    try
                    {
                        if (s3Node.Status != S3NodeStatus.New && s3Node.Status != S3NodeStatus.Modified) return;

                        // we need to upload the whole file again
                        // for larger files we could reuse unmodified parts (>= 5 MB, max. 10.000 parts)
                        // see https://stackoverflow.com/questions/38069985/replacing-bytes-of-an-uploaded-file-in-amazon-s3
                        byte[] data = Read(0, (uint) FileSize);

                        await using MemoryStream ms = new MemoryStream(data);

                        var putObjectRequest = new PutObjectRequest()
                        {
                            BucketName = s3Node.bucket.Name,
                            Key = s3Node.Key,
                            InputStream = ms,
                        };
                        var putObjectResponse = await s3Client.PutObjectAsync(putObjectRequest);

                        s3Node.Status = S3NodeStatus.Active;
                    }
                    finally
                    {
                        persistingSemaphore.Release();
                    }
                }
            }

            public void ClearCache()
            {
                memoryCache.Clear();
            }

            private async Task<Block> DownloadBlock(Block block)
            {
                if (block.Offset < OriginalSize)
                {
                    var endOffset = Math.Min(block.Offset + block.Length, OriginalSize);
                    var getRequest = new GetObjectRequest()
                    {
                        BucketName = s3Node.Path.BucketName,
                        Key = s3Node.Path.ObjectKey,
                        ByteRange = new ByteRange((long) block.Offset, (long) (endOffset - 1)),
                    };
                    var getObjectResponse = await s3Client.GetObjectAsync(getRequest);
                    block.Data ??= new byte[MEMCACHE_BLOCKSIZE];
                    await using MemoryStream ms = new MemoryStream(block.Data, true);
                    await getObjectResponse.ResponseStream.CopyToAsync(ms);
                }

                return block;
            }

            private byte[] ReadFromMemoryCache(ulong offset, uint length)
            {
                using MemoryStream ms = new MemoryStream();
                uint blockIndex = (uint) (offset / MEMCACHE_BLOCKSIZE);
                uint blockOffset = (uint) (offset % MEMCACHE_BLOCKSIZE);
                uint remainingLength = length;
                while (remainingLength > 0)
                {
                    uint blockBytes = Math.Min(remainingLength, MEMCACHE_BLOCKSIZE - blockOffset);
                    byte[] sourceData = ZERO_BLOCK;
                    if (memoryCache.TryGetValue(blockIndex * MEMCACHE_BLOCKSIZE, out Block cacheBlock) && cacheBlock.Data != null)
                    {
                        sourceData = cacheBlock.Data;
                    }

                    ms.Write(sourceData, (int) blockOffset, (int) blockBytes);
                    remainingLength -= blockBytes;
                    blockIndex++;
                    blockOffset = 0;
                }

                return ms.ToArray();
            }

            private Block RoundToBlocksize(UInt64 offset, UInt32 length, uint blocksize = MEMCACHE_BLOCKSIZE)
            {
                UInt32 additionalOffset = (UInt32) (offset % blocksize);
                UInt64 roundedOffset = offset - additionalOffset;
                uint nBlocks = (length + additionalOffset + blocksize - 1) / blocksize;
                UInt32 roundedLength = nBlocks * blocksize;
                return new Block {Offset = roundedOffset, Length = roundedLength};
            }

            private List<Block> SplitBlock(Block block, uint blocksize = MEMCACHE_BLOCKSIZE)
            {
                uint nBlocks = block.Length / blocksize;
                var result = new List<Block>();
                for (uint i = 0; i < nBlocks; i++)
                {
                    result.Add(new Block {Offset = block.Offset + blocksize * i, Length = blocksize});
                }

                return result;
            }

            private List<Block> CalculateMissingBlocks(Block requestedBlock)
            {
                List<Block> missingBlocks = SplitBlock(requestedBlock);
                missingBlocks.RemoveAll(b => memoryCache.ContainsKey(b.Offset));
                return missingBlocks;
            }

            private class Block
            {
                private UInt32 length;

                public UInt64 Offset;

                public UInt32 Length
                {
                    get => (uint) (Data?.LongLength ?? length);
                    set
                    {
                        if (Data == null) length = value;
                    }
                }

                public byte[] Data;
            }
        }
    }
}