using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace s3vfs
{
    public class S3ObjectDataCache : IS3ObjectData
    {
        private const int MEMCACHE_BLOCKSIZE = 256 * 1024;
        private const int FILECACHE_BLOCKSIZE = 1024 * 1024;
        private const int MAX_BLOCKS = 4;

        private SortedList<UInt64, Block> memoryCache = new();
        private IS3Node s3Node;
        private AmazonS3Client s3Client;

        internal S3ObjectDataCache(IS3Node s3Node, AmazonS3Client s3Client)
        {
            this.s3Node = s3Node;
            this.s3Client = s3Client;
        }

        public byte[] Read(UInt64 offset, UInt32 length)
        {
            var requestedLargeBlock = RoundToBlocksize(offset, length);
            var missingBlocks = CalculateMissingBlocks(requestedLargeBlock);
            if (missingBlocks.Count == 0) return ReadFromMemoryCache(offset, length);
            if (missingBlocks.Count < MAX_BLOCKS)
            {
                foreach (var missingBlock in missingBlocks)
                {
                    var downloadedBlock = DownloadBlock(missingBlock).Result;
                    memoryCache.Add(downloadedBlock.Offset, downloadedBlock);
                }

                return ReadFromMemoryCache(offset, length);
            }

            throw new NotImplementedException();
        }

        private async Task<Block> DownloadBlock(Block block)
        {
            var getRequest = new GetObjectRequest()
            {
                BucketName = s3Node.Path.BucketName,
                Key = s3Node.Path.ObjectKey,
                ByteRange = new ByteRange((long)block.Offset, (long)(block.Offset + block.Length - 1)),
            };
            var getObjectResponse = await s3Client.GetObjectAsync(getRequest);
            await using MemoryStream ms = new MemoryStream();
            await getObjectResponse.ResponseStream.CopyToAsync(ms);
            block.Data = ms.ToArray();
            return block;
        }

        private byte[] ReadFromMemoryCache(ulong offset, uint length)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                foreach (var (memOffset, memData) in memoryCache)
                {
                    if (memOffset < offset + length && memOffset + memData.Length > offset)
                    {
                        int start = (int)Math.Clamp((long)offset - (long)memOffset, 0, memData.Data.Length);
                        int end = (int)Math.Clamp((long) (offset + length) - (long) memOffset, start, memData.Data.Length);
                        ms.Write(memData.Data[start..end]);
                    }
                }

                return ms.ToArray();
            }
        }

        private Block RoundToBlocksize(UInt64 offset, UInt32 length, uint blocksize = MEMCACHE_BLOCKSIZE)
        {
            UInt32 additionalOffset = (UInt32)(offset % blocksize);
            UInt64 roundedOffset = offset - additionalOffset;
            uint nBlocks = (length + additionalOffset + blocksize - 1) / blocksize;
            UInt32 roundedLength = nBlocks * blocksize;
            return new Block { Offset = roundedOffset, Length = roundedLength };
        }

        private List<Block> SplitBlock(Block block, uint blocksize = MEMCACHE_BLOCKSIZE)
        {
            uint nBlocks = block.Length / blocksize;
            var result = new List<Block>();
            for (uint i = 0; i < nBlocks; i++)
            {
                result.Add(new Block { Offset = block.Offset + blocksize * i, Length = blocksize });
            }

            return result;
        }

        private List<Block> CalculateMissingBlocks(Block requestedBlock)
        {
            List<Block> missingBlocks = SplitBlock(requestedBlock);
            missingBlocks.RemoveAll(b => memoryCache.ContainsKey(b.Offset));
            return missingBlocks;
        }

        private struct Block
        {
            public UInt64 Offset;
            public UInt32 Length;
            public byte[] Data;
        }
    }
}