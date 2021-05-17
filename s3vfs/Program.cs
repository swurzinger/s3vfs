using System;

namespace s3vfs
{
    class Program
    {
        static void Main(string[] args)
        {
            Environment.ExitCode = new S3VfsService().Run();
        }
    }
}