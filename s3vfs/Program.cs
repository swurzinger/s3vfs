using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Linq;

namespace s3vfs
{
    class Program
    {
        static void Main(string[] args)
        {
            var cmd = new RootCommand
            {
                new Option<string>(new[] { "--s3Url", "-e" }, "S3 service endpoint url")
                {
                    IsRequired = true,
                },
                new Option<string>(new[] { "--accessKey", "-a" }, "S3 access key")
                {
                    IsRequired = true,
                },
                new Option<string>(new[] { "--secretKey", "-s" }, "S3 secret key")
                {
                    IsRequired = true,
                },
                new Option<string>(new[] { "--volumePrefix", "-u" }, "Volume Prefix (e.g. \\prefix\\service)"),
                new Option<string>(new[] { "--mountPoint", "-m" }, "MountPoint (e.g. Z:)"),
            };

            cmd.Handler = CommandHandler.Create<S3VfsServiceOptions, IConsole>(RunService);
            Environment.ExitCode = cmd.Invoke(args);
        }

        private static void RunService(S3VfsServiceOptions options, IConsole console)
        {
            new S3VfsService(options).Run();
        }
    }
}