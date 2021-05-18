using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3.Model;
using Fsp.Interop;

namespace s3vfs
{
    public partial class S3ObjectNode : IS3Node
    {
        private S3BucketNode bucket;
        private S3ObjectDataCache dataCache;

        public S3ObjectNode(S3BucketNode bucket, string key, ulong size, DateTime lastModified, S3NodeStatus status = S3NodeStatus.Active)
        {
            Key = key;
            Size = size;
            LastModified = lastModified;
            this.bucket = bucket;
            this.Status = status;
        }

        public S3NodeStatus Status { get; private set; }
        public string Name => Path.Name;
        public string Key { get; private set; }
        public ulong Size { get; private set; }
        public DateTime LastModified { get; private set; }

        public S3Path Path => new S3Path(bucket.Name, Key);

        public FileInfo GetFileInfo()
        {
            ulong lastModified = (UInt64) LastModified.ToFileTimeUtc();
            ulong allocationSize = dataCache?.AllocatedSize ??
                                   ((Size + S3Filesystem.ALLOCATION_UNIT - 1) / S3Filesystem.ALLOCATION_UNIT) * S3Filesystem.ALLOCATION_UNIT;
            return new FileInfo()
            {
                CreationTime = lastModified,
                LastWriteTime = lastModified,
                ChangeTime = lastModified,
                FileAttributes = 0,
                AllocationSize = allocationSize,
                FileSize = Size,
            };
        }

        public async Task<List<IS3Node>> GetChildren(string afterMarkerName = null)
        {
            return new List<IS3Node>();
        }

        public IS3Node LookupNode(S3Path path)
        {
            if (bucket.Name == path.BucketName && Key == path.ObjectKey)
            {
                return this;
            }

            return null;
        }

        public IS3ObjectData GetObjectData()
        {
            if (dataCache == null)
            {
                dataCache = new S3ObjectDataCache(this, this.bucket.S3Root.S3Client);
            }

            return dataCache;
        }

        public async Task Move(S3Path newPath, bool replaceIfExists)
        {
            if (newPath == null || string.IsNullOrEmpty(newPath.BucketName) || string.IsNullOrEmpty(newPath.ObjectKey))
                throw new ArgumentException($"cannot move {Path} to {newPath}");

            var oldParent = bucket.LookupNode(Path.Parent);
            var newParent = bucket.S3Root.LookupNode(newPath.Parent);
            var newBucket = bucket.S3Root.LookupNode(new S3Path(newPath.BucketName, "")) as S3BucketNode;
            if (oldParent == null || newParent == null || newBucket == null)
                throw new ArgumentException($"cannot move {Path} to {newPath}");

            if (Status == S3NodeStatus.Active || Status == S3NodeStatus.Modified)
            {
                var copyRequest = new CopyObjectRequest()
                {
                    SourceBucket = bucket.Name,
                    SourceKey = Key,
                    DestinationBucket = newPath.BucketName,
                    DestinationKey = newPath.ObjectKey,
                };
                await bucket.S3Root.S3Client.CopyObjectAsync(copyRequest);
                await bucket.S3Root.S3Client.DeleteObjectAsync(Path.BucketName, Path.ObjectKey);
            }

            bucket = newBucket;
            Key = newPath.ObjectKey;

            (oldParent as S3StructureNode)?.CacheRemove(this);
            (newParent as S3StructureNode)?.CacheAdd(this);
        }

        public async Task DeleteRecursive()
        {
            await Delete();
        }

        public async Task Delete()
        {
            if (Status != S3NodeStatus.Deleted)
            {
                if (Status == S3NodeStatus.Active || Status == S3NodeStatus.Modified)
                {
                    var deleteResponse = await bucket.S3Root.S3Client.DeleteObjectAsync(bucket.Name, Key);
                }
                var parent = bucket.LookupNode(Path.Parent) as S3StructureNode;
                parent?.CacheRemove(this);
            }
        }

        public void MarkForDeletion()
        {
            if (Status != S3NodeStatus.Deleted)
            {
                Status = S3NodeStatus.MarkedForDeletion;
            }
        }

        public IS3Node CreateFile(S3Path path)
        {
            throw new InvalidOperationException("cannot create sub-element for file");
        }

        public IS3Node CreateDirectory(S3Path path)
        {
            throw new InvalidOperationException("cannot create sub-element for file");
        }

        public async Task PersistChanges()
        {
            if (Status == S3NodeStatus.New || Status == S3NodeStatus.Modified)
            {
                GetObjectData();        // create dataCache, if necessary
                await dataCache.Persist();
            }
        }

        public async Task PersistChangesRecursive()
        {
            await PersistChanges();
        }

        internal void SetDeletedFromBulkDeletion()
        {
            Status = S3NodeStatus.Deleted;
        }
    }
}