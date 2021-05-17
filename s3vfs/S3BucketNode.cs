using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Fsp.Interop;

namespace s3vfs
{
    public class S3BucketNode : S3StructureNode
    {
        // private S3StructureNode bucketRoot;
        public S3BucketNode(string name, DateTime creationDate, S3RootNode s3Root)
        : base(null, "")
        {
            Name = name;
            CreationDate = creationDate;
            S3Root = s3Root;
            base.bucket = this;
            // bucketRoot = new S3StructureNode(this, "");
        }

        // public string Name { get; private set; }

        public DateTime CreationDate { get; private set; }

        // public uint Attributes => (uint)System.IO.FileAttributes.Directory;

        // public S3Path Path => new S3Path(Name, "");

        internal S3RootNode S3Root { get; private set; }

        // public Task<List<IS3Node>> GetChildren(string afterMarkerName = null)
        // {
        //     return bucketRoot.GetChildren(afterMarkerName);
        // }

        public override FileInfo GetFileInfo()
        {
            return new FileInfo()
            {
                CreationTime = (UInt64) CreationDate.ToFileTimeUtc(),
                FileAttributes = Attributes,
            };
        }

        // public IS3Node LookupNode(S3Path path)
        // {
        //     if (path.BucketName != Name) return null;
        //     if (string.IsNullOrEmpty(path.ObjectKey)) return this;
        //     return bucketRoot.LookupNode(path);
        //     // return LookupNodeAsync(path).Result;
        // }

        // public IS3ObjectData GetObjectData()
        // {
        //     throw new NotSupportedException();
        // }

        public override Task Move(S3Path newPath)
        {
            throw new NotImplementedException();
        }

        public override async Task DeleteRecursive()
        {
            foreach (var child in await GetChildren())
            {
                await child.DeleteRecursive();
            }

            var deleteBucketRequest = new DeleteBucketRequest()
            {
                BucketName = Name,
            };
            await S3Root.S3Client.DeleteBucketAsync(deleteBucketRequest);

            S3Root.CacheRemove(this);
        }


        // private async Task<IS3Node> LookupNodeAsync(S3Path path)
        // {
        //     try
        //     {
        //         var s3obj = await S3Root.S3Client.GetObjectMetadataAsync(Name, path.ObjectKey);
        //         return new S3ObjectNode(this, path.ObjectKey, (ulong) s3obj.ContentLength, s3obj.LastModified);
        //     }
        //     catch (AmazonS3Exception s3Exception)
        //     {
        //         return null;
        //     }
        // }
    }
}