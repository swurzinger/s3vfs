using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3.Model;
using Fsp.Interop;

namespace s3vfs
{
    public class S3ObjectNode : IS3Node
    {
        private S3BucketNode bucket;
        private S3ObjectDataCache dataCache;

        public S3ObjectNode(S3BucketNode bucket, string key, ulong size, DateTime lastModified)
        {
            Name = key.Split("/", StringSplitOptions.RemoveEmptyEntries).LastOrDefault() ?? "";
            Key = key;
            Size = size;
            LastModified = lastModified;
            this.bucket = bucket;
            this.Status = S3NodeStatus.Active;
        }

        public S3NodeStatus Status { get; private set; }
        public string Name { get; private set; }
        public string Key { get; private set; }
        public ulong Size { get; private set; }
        public DateTime LastModified { get; private set; }

        public S3Path Path => new S3Path(bucket.Name, Key);

        public FileInfo GetFileInfo()
        {
            ulong lastModified = (UInt64) LastModified.ToFileTimeUtc();
            return new FileInfo()
            {
                CreationTime = lastModified,
                LastWriteTime = lastModified,
                ChangeTime = lastModified,
                FileAttributes = 0,
                AllocationSize = Size,
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

        public async Task Move(S3Path newPath)
        {
            if (newPath == null || string.IsNullOrEmpty(newPath.BucketName) || string.IsNullOrEmpty(newPath.ObjectKey))
                throw new ArgumentException($"cannot move {Path} to {newPath}");

            var oldParent = bucket.LookupNode(Path.Parent);
            var newParent = bucket.S3Root.LookupNode(newPath.Parent);
            var newBucket = bucket.S3Root.LookupNode(new S3Path(newPath.BucketName, "")) as S3BucketNode;
            if (oldParent == null || newParent == null || newBucket == null)
                throw new ArgumentException($"cannot move {Path} to {newPath}");

            var copyRequest = new CopyObjectRequest()
            {
                SourceBucket = bucket.Name,
                SourceKey = Key,
                DestinationBucket = newPath.BucketName,
                DestinationKey = newPath.ObjectKey,
            };
            await bucket.S3Root.S3Client.CopyObjectAsync(copyRequest);
            await bucket.S3Root.S3Client.DeleteObjectAsync(Path.BucketName, Path.ObjectKey);

            bucket = newBucket;
            Key = newPath.ObjectKey;
            Name = Key.Split("/", StringSplitOptions.RemoveEmptyEntries).LastOrDefault() ?? "";

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

        public IS3Node CreateFile(string name)
        {
            throw new InvalidOperationException("cannot create sub-element for file");
        }

        public IS3Node CreateDirectory(string name)
        {
            throw new InvalidOperationException("cannot create sub-element for file");
        }

        public Task PersistChanges()
        {
            throw new NotImplementedException();
        }

        internal void SetDeletedFromBulkDeletion()
        {
            Status = S3NodeStatus.Deleted;
        }
    }
}