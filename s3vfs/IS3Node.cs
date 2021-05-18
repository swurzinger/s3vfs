using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Fsp.Interop;

namespace s3vfs
{
    public interface IS3Node
    {
        public string Name { get; }

        public S3NodeStatus Status { get; }

        public S3Path Path { get; }
        public FileInfo GetFileInfo();
        public Task<List<IS3Node>> GetChildren(string afterMarkerName = null);

        public IS3Node LookupNode(S3Path path);

        public IS3ObjectData GetObjectData();

        public Task Move(S3Path newPath, bool replaceIfExists);

        public Task DeleteRecursive();

        public Task Delete();

        public void MarkForDeletion();

        public IS3Node CreateFile(S3Path path);

        public IS3Node CreateDirectory(S3Path path);

        public Task PersistChanges();
        public Task PersistChangesRecursive();
    }
}