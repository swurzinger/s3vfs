using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Fsp.Interop;

namespace s3vfs
{
    public class S3RootNode : IS3DirectoryNode
    {
        private List<S3BucketNode> buckets;
        internal readonly AmazonS3Client S3Client;


        public S3RootNode(AmazonS3Client s3Client)
        {
            S3Client = s3Client;
        }

        public S3NodeStatus Status => S3NodeStatus.Active;
        public string Name => "S3 Volume";

        public S3Path Path => new S3Path("", "");

        public async Task<List<IS3Node>> GetChildren(string afterMarkerName = null)
        {
            if (buckets == null)
            {
                buckets = await GetBuckets();
            }

            if (afterMarkerName != null)
            {
                int afterIndex = buckets.FindIndex(n => n.Name == afterMarkerName);
                if (afterIndex >= 0)
                {
                    return buckets.Skip(afterIndex + 1).Cast<IS3Node>().ToList();
                }
            }

            return new List<IS3Node>(buckets);
        }

        public FileInfo GetFileInfo()
        {
            return new FileInfo()
            {
                FileAttributes = (uint)System.IO.FileAttributes.Directory,
            };
        }

        private async Task<List<S3BucketNode>> GetBuckets()
        {
            var buckets = await S3Client.ListBucketsAsync(new ListBucketsRequest());
            return buckets.Buckets.Select(it => new S3BucketNode(it.BucketName, it.CreationDate, this)).ToList();
        }

        public IS3Node LookupNode(S3Path path)
        {
            if (path == null) return this;
            return LookupNodeAsync(path).Result;
        }

        private async Task<IS3Node> LookupNodeAsync(S3Path path)
        {
            var buckets = await GetChildren();
            var bucket = buckets.FirstOrDefault(b => b.Name == path.BucketName);
            return bucket?.LookupNode(path);
        }

        public IS3ObjectData GetObjectData()
        {
            throw new NotSupportedException();
        }

        public Task Move(S3Path newPath)
        {
            throw new NotSupportedException();
        }

        public async Task DeleteRecursive()
        {
            // do nothing
        }

        public async Task Delete()
        {
            // do nothing
        }

        public void MarkForDeletion()
        {
            // do nothing
        }

        public IS3Node CreateDirectory(string name)
        {
            throw new NotImplementedException();
        }

        public IS3Node CreateFile(string name)
        {
            throw new NotSupportedException();
        }

        public async Task PersistChanges()
        {
            if (buckets != null)
            {
                foreach (var s3BucketNode in buckets)
                {
                    await s3BucketNode.PersistChanges();
                }
            }
        }

        internal void CacheRemove(S3BucketNode bucket)
        {
            buckets?.Remove(bucket);
        }
    }
}