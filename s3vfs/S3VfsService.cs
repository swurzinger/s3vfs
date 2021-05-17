using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Linq;
using Fsp;

namespace s3vfs
{
    public class S3VfsService : Service
    {
        private FileSystemHost _Host;

        public S3VfsService() : base("S3VFSService")
        {
        }

        protected override void OnStart(String[] args)
        {
            var cmd = new RootCommand
            {
                new Option<string>(new[] { "--s3Url", "-e" }, "S3 service endpoint url"),
                new Option<string>(new[] { "--accessKey", "-a" }, "S3 access key"),
                new Option<string>(new[] { "--secretKey", "-s" }, "S3 secret key"),
                new Option<string>(new[] { "--volumePrefix", "-u" }, "Volume Prefix (e.g. \\prefix\\service)"),
                new Option<string>(new[] { "--mountPoint", "-m" }, "MountPoint (e.g. Z:)"),
            };

            cmd.Handler = CommandHandler.Create<string, string, string, string, string, IConsole>(RunService);
            int rc = cmd.Invoke(args.Skip(1).ToArray());
            this.ExitCode = rc;
            if (rc != 0) throw new Exception("return code " + rc);
        }

        protected override void OnStop()
        {
            _Host.Unmount();
            _Host = null;
        }

        private void RunService(string s3Url, string accessKey, string secretKey, string volumePrefix, string mountPoint, IConsole console)
        {
            try
            {
                var s3Filesystem = new S3Filesystem(s3Url, accessKey, secretKey);
                FileSystemHost.SetDebugLogFile("-");
                var host = new FileSystemHost(s3Filesystem) { Prefix = volumePrefix };
                int rc = host.Mount(mountPoint);
                if (0 > rc)
                {
                    throw new IOException("cannot mount file system; rc = " + rc);
                }
                console.Out.Write("mounted: " + host.MountPoint() + "\n");
                _Host = host;
            }
            catch (Exception ex)
            {
                Log(EVENTLOG_ERROR_TYPE, ex.Message);
                throw;
            }
        }
    }
}