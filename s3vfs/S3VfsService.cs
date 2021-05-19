using System;
using System.IO;
using Fsp;

namespace s3vfs
{
    public class S3VfsService : Service
    {
        private FileSystemHost _Host;
        private S3VfsServiceOptions options;

        public S3VfsService(S3VfsServiceOptions options) : base("S3VFSService")
        {
            this.options = options;
        }

        protected override void OnStart(String[] args)
        {
            if (string.IsNullOrEmpty(options.VolumePrefix) && string.IsNullOrEmpty(options.MountPoint))
            {
                throw new ArgumentException("either volumePrefix or mountPoint is required");
            }

            try
            {
                var s3Filesystem = new S3Filesystem(options.S3Url, options.AccessKey, options.SecretKey);
                FileSystemHost.SetDebugLogFile("-");
                var host = new FileSystemHost(s3Filesystem) { Prefix = options.VolumePrefix };
                int rc = host.Mount(options.MountPoint);
                if (0 > rc)
                {
                    throw new IOException("cannot mount file system; rc = " + rc);
                }
                Log(EVENTLOG_INFORMATION_TYPE, "mounted on: " + host.MountPoint() + "\n");
                _Host = host;
            }
            catch (Exception ex)
            {
                Log(EVENTLOG_ERROR_TYPE, ex.Message);
                throw;
            }
        }

        protected override void OnStop()
        {
            _Host.Unmount();
            _Host = null;
        }
    }
}