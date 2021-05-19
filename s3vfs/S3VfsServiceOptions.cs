namespace s3vfs
{
    public class S3VfsServiceOptions
    {
        public string S3Url { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string VolumePrefix { get; set; }
        public string MountPoint { get; set; }
    }
}