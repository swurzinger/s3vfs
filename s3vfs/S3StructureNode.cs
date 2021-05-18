using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3.Model;
using Fsp.Interop;

namespace s3vfs
{
    public class S3StructureNode : IS3DirectoryNode
    {
        protected S3BucketNode bucket;
        private readonly List<S3StructureNode> directories = new();
        private readonly List<S3ObjectNode> files = new();
        private bool fetchedContents = false;
        private readonly SemaphoreSlim fetchingSemaphore = new SemaphoreSlim(1, 1);

        public S3StructureNode(S3BucketNode bucket, string pathPrefix, S3NodeStatus status = S3NodeStatus.Active)
        {
            Name = pathPrefix.Split("/", StringSplitOptions.RemoveEmptyEntries).LastOrDefault() ?? "";
            CommonPrefix = pathPrefix;
            this.bucket = bucket;
            Status = status;
        }

        public S3NodeStatus Status { get; protected set; }
        public string Name { get; protected set; }

        public string CommonPrefix { get; private set; }

        public uint Attributes => (uint)System.IO.FileAttributes.Directory;

        public S3Path Path => new S3Path(bucket.Name, CommonPrefix.TrimEnd('/'));

        public async Task<List<IS3Node>> GetChildren(string afterMarkerName = null)
        {
            if (!fetchedContents)
            {
                await FetchContents();
            }

            if (afterMarkerName != null)
            {
                int afterIndex = directories.FindIndex(n => n.Name == afterMarkerName);
                if (afterIndex >= 0)
                {
                    return directories.Skip(afterIndex + 1).Cast<IS3Node>().Concat(files).ToList();
                }

                afterIndex = files.FindIndex(n => n.Name == afterMarkerName);
                if (afterIndex >= 0)
                {
                    return files.Skip(afterIndex + 1).Cast<IS3Node>().ToList();
                }
            }

            return new List<IS3Node>(directories).Concat(files).ToList();
        }

        public virtual FileInfo GetFileInfo()
        {
            return new FileInfo()
            {
                FileAttributes = Attributes,
            };
        }

        private async Task FetchContents()
        {
            if (Status == S3NodeStatus.New || Status == S3NodeStatus.Deleted) return;
            if (await fetchingSemaphore.WaitAsync(-1))
            {
                try
                {
                    if (fetchedContents) return;
                    ListObjectsV2Response listResponse = new ListObjectsV2Response()
                    {
                        IsTruncated = true,
                    };
                    while (listResponse.IsTruncated)
                    {
                        var listRequest = new ListObjectsV2Request()
                        {
                            BucketName = bucket.Name,
                            ContinuationToken = listResponse.NextContinuationToken,
                            Delimiter = "/",
                            Prefix = CommonPrefix
                        };
                        listResponse = await bucket.S3Root.S3Client.ListObjectsV2Async(listRequest);
                        var newDirs = listResponse.CommonPrefixes
                            .Select(commonPrefix => new S3StructureNode(bucket, commonPrefix)).ToList();
                        var newFiles = listResponse.S3Objects
                            .Select(obj => new S3ObjectNode(bucket, obj.Key, (ulong) obj.Size, obj.LastModified)).ToList();
                        var duplicateDirs = newDirs.Intersect(directories).ToList();
                        var duplicateFiles = newFiles.Intersect(files).ToList();
                        if (duplicateDirs.Any())
                        {
                            throw new InvalidOperationException("trying to add duplicate directories " + duplicateDirs.Select(d => d.Path).JoinToString());
                        }

                        if (duplicateFiles.Any())
                        {
                            throw new InvalidOperationException("trying to add duplicate files " + duplicateFiles.Select(d => d.Path).JoinToString());
                        }

                        directories.AddRange(newDirs);
                        files.AddRange(newFiles);
                    }

                    fetchedContents = true;
                }
                finally
                {
                    fetchingSemaphore.Release();
                }
            }
        }

        public virtual IS3Node LookupNode(S3Path path)
        {
            if (!path.ObjectKey.StartsWith(CommonPrefix.TrimEnd('/'))) return null;
            if (path.ObjectKey == CommonPrefix.TrimEnd('/')) return this;

            string subPath = path.ObjectKey.Substring(Math.Clamp(CommonPrefix.Length, 0, path.ObjectKey.Length));
            string[] subPathParts = subPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (subPathParts.Length == 0) return null;

            // search cached entries
            IS3Node result = LookupInternal(path, subPathParts);
            if (result != null) return result;

            // fetch and repeat (if necessary)
            if (!fetchedContents)
            {
                FetchContents().Wait();
                return LookupInternal(path, subPathParts);
            }

            return null;
        }

        protected IS3Node LookupInternal(S3Path path, string[] subPathParts)
        {
            S3StructureNode directory = directories.FirstOrDefault(d => d.Name == subPathParts[0]);
            if (directory != null) return directory.LookupNode(path);
            if (subPathParts.Length == 1)
            {
                S3ObjectNode file = files.FirstOrDefault(f => f.Name == subPathParts[0]);
                if (file != null) return file;
            }

            return null;
        }

        public IS3ObjectData GetObjectData()
        {
            throw new NotSupportedException();
        }

        public virtual async Task Move(S3Path newPath, bool replaceIfExists)
        {
            var newParent = bucket.S3Root.LookupNode(newPath.Parent);
            if (newParent == null) throw new ArgumentException("move: cannot find new parent node");
            var existingNode = newParent.LookupNode(newPath);

            if (existingNode != null)
            {
                if (replaceIfExists) await existingNode.DeleteRecursive();
                else throw new InvalidOperationException("cannot move because target already exists");
            }

            var newNode = newParent.CreateDirectory(newPath);

            var children = await GetChildren();
            foreach (var child in children)
            {
                await child.Move(newPath.Append(child.Name), replaceIfExists);
            }

            children = await GetChildren();
            if (children.Count != 0) throw new InvalidOperationException("could not move all children");

            await this.Delete();
        }

        public async Task Delete()
        {
            var children = await GetChildren();
            if (children.Count == 0)
            {
                Status = S3NodeStatus.Deleted;
                var parent = bucket.LookupNode(Path.Parent) as S3StructureNode;
                parent?.CacheRemove(this);
            }
        }

        public virtual async Task DeleteRecursive()
        {
            await DeleteRecursive(true);
        }
        protected virtual async Task DeleteRecursive(bool unregisterFromParent)
        {
            if (Status == S3NodeStatus.Deleted) return;

            FetchContents().Wait();
            foreach (var directory in directories.ToList())
            {
                await directory.DeleteRecursive(false);
            }

            var activeFiles = files
                .Where(f => f.Status == S3NodeStatus.Active || f.Status == S3NodeStatus.Modified);
            foreach (var filesChunk in activeFiles.Chunked(1000))
            {
                var deleteObjectsRequest = new DeleteObjectsRequest()
                {
                    BucketName = bucket.Name,
                };
                foreach (var file in filesChunk)
                {
                    deleteObjectsRequest.AddKey(file.Key);
                    file.SetDeletedFromBulkDeletion();
                }

                if (deleteObjectsRequest.Objects.Count > 0)
                {
                    await bucket.S3Root.S3Client.DeleteObjectsAsync(deleteObjectsRequest);
                }
            }

            if (unregisterFromParent)
            {
                var parent = bucket.LookupNode(Path.Parent) as S3StructureNode;
                parent?.CacheRemove(this);
            }

            Status = S3NodeStatus.Deleted;
        }

        public void MarkForDeletion()
        {
            if (Status != S3NodeStatus.Deleted) Status = S3NodeStatus.MarkedForDeletion;
        }

        public IS3Node CreateFile(S3Path path)
        {
            if (bucket.Name != path.BucketName || !path.ObjectKey.StartsWith(CommonPrefix))
            {
                throw new InvalidOperationException($"trying to create {path} on {Path}");
            }
            var newNode = new S3ObjectNode(bucket, path.ObjectKey, 0, DateTime.Now, S3NodeStatus.New);
            files.Add(newNode);
            return newNode;
        }

        public IS3Node CreateDirectory(S3Path path)
        {
            if (bucket.Name != path.BucketName || !path.ObjectKey.StartsWith(CommonPrefix))
            {
                throw new InvalidOperationException($"trying to create {path} on {Path}");
            }
            var newDir = new S3StructureNode(bucket, path.ObjectKey + "/", S3NodeStatus.New);
            directories.Add(newDir);
            return newDir;
        }

        public async Task PersistChanges()
        {
            // do nothing
        }

        public async Task PersistChangesRecursive()
        {
            foreach (var child in directories.Cast<IS3Node>().Concat(files))
            {
                await child.PersistChangesRecursive();
            }
        }

        internal void CacheRemove(IS3Node s3Node)
        {
            if (s3Node is S3StructureNode s3Dir)
            {
                if (!directories.Remove(s3Dir))
                {
                    throw new KeyNotFoundException($"cannot remove dir {s3Node.Path} from {Path}");
                }
            }
            else if (s3Node is S3ObjectNode s3Obj)
            {
                if (!files.Remove(s3Obj))
                {
                    throw new KeyNotFoundException($"cannot remove file {s3Node.Path} from {Path}");
                }
            }
            else
            {
                throw new ArgumentException("unexpected node type " + s3Node);
            }
        }

        internal void CacheAdd(IS3Node s3Node)
        {
            if (s3Node is S3StructureNode s3Dir)
            {
                if (directories.Contains(s3Node))
                {
                    throw new InvalidOperationException($"trying to add duplicate dir {s3Node.Path} to {Path}");
                }
                directories.Add(s3Dir);
            }
            else if (s3Node is S3ObjectNode s3Obj)
            {
                if (files.Contains(s3Node))
                {
                    throw new InvalidOperationException($"trying to add duplicate file {s3Node.Path} to {Path}");
                }
                files.Add(s3Obj);
            }
            else
            {
                throw new ArgumentException("unexpected node type " + s3Node);
            }
        }
    }
}