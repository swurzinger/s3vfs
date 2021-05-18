using System;
using System.Linq;

namespace s3vfs
{
    public class S3Path
    {
        public S3Path(string bucket, string key)
        {
            BucketName = bucket;
            ObjectKey = key;
        }

        public static S3Path FromPath(string path)
        {
            string[] bucketPath = path.Replace('\\', '/')
                .Split('/', StringSplitOptions.RemoveEmptyEntries);
            string bucket = bucketPath.FirstOrDefault();
            if (string.IsNullOrEmpty(bucket)) return null;
            string objectKey = string.Join('/', bucketPath.Skip(1));
            return new S3Path(bucket, objectKey);
        }

        public string BucketName { get; }
        public string ObjectKey { get; }

        public S3Path Parent
        {
            get
            {
                if (string.IsNullOrEmpty(BucketName) || string.IsNullOrEmpty(ObjectKey)) return null;
                string parentObject = string.Join('/', ObjectKey.Split('/').SkipLast(1));
                return new S3Path(BucketName, parentObject);
            }
        }

        public string[] Parts => (ObjectKey ?? "").Split('/');

        public string Name => Parts.LastOrDefault() ?? BucketName;

        public S3Path Append(string name)
        {
            if (string.IsNullOrEmpty(BucketName)) return new S3Path(name, "");
            if (string.IsNullOrEmpty(ObjectKey)) return new S3Path(BucketName, name);
            return new S3Path(BucketName, ObjectKey + "/" + name);
        }

        public override string ToString()
        {
            return BucketName + "/" + ObjectKey;
        }

        protected bool Equals(S3Path other)
        {
            return BucketName == other.BucketName && ObjectKey == other.ObjectKey;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((S3Path) obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(BucketName, ObjectKey);
        }
    }
}