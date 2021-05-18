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
        public S3BucketNode(string name, DateTime creationDate, S3RootNode s3Root, S3NodeStatus status = S3NodeStatus.Active)
        : base(null, "", status)
        {
            Name = name;
            CreationDate = creationDate;
            S3Root = s3Root;
            base.bucket = this;
        }

        public DateTime CreationDate { get; private set; }

        internal S3RootNode S3Root { get; private set; }


        public override FileInfo GetFileInfo()
        {
            return new FileInfo()
            {
                CreationTime = (UInt64) CreationDate.ToFileTimeUtc(),
                FileAttributes = Attributes,
            };
        }

        public override Task Move(S3Path newPath, bool replaceIfExists)
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
    }
}