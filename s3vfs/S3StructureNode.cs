using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3.Model;
using Fsp.Interop;

namespace s3vfs
{
    public class S3StructureNode : IS3DirectoryNode
    {
        protected S3BucketNode bucket;
        private List<S3StructureNode> directories;
        private List<S3ObjectNode> files;

        public S3StructureNode(S3BucketNode bucket, string pathPrefix)
        {
            Name = pathPrefix.Split("/", StringSplitOptions.RemoveEmptyEntries).LastOrDefault() ?? "";
            CommonPrefix = pathPrefix;
            this.bucket = bucket;
            this.Status = S3NodeStatus.Active;
        }

        public S3NodeStatus Status { get; protected set; }
        public string Name { get; protected set; }

        public string CommonPrefix { get; private set; }

        public uint Attributes => (uint)System.IO.FileAttributes.Directory;

        public S3Path Path => new S3Path(bucket.Name, CommonPrefix.TrimEnd('/'));

        public async Task<List<IS3Node>> GetChildren(string afterMarkerName = null)
        {
            if (directories == null || files == null)
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
                directories = listResponse.CommonPrefixes
                    .Select(commonPrefix => new S3StructureNode(bucket, commonPrefix))
                    .ToList();
                files = listResponse.S3Objects
                    .Select(obj => new S3ObjectNode(bucket, obj.Key, (ulong) obj.Size, obj.LastModified))
                    .ToList();
            }
        }

        public virtual IS3Node LookupNode(S3Path path)
        {
            if (!path.ObjectKey.StartsWith(CommonPrefix.TrimEnd('/'))) return null;
            if (path.ObjectKey == CommonPrefix.TrimEnd('/')) return this;
            // if (directories == null || files == null) return null;
            if (directories == null || files == null)
            {
                var children = GetChildren().Result;
                if (directories == null || files == null) return null;
            }
            string subPath = path.ObjectKey.Substring(Math.Clamp(CommonPrefix.Length, 0, path.ObjectKey.Length));
            string[] subPathParts = subPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (subPathParts.Length == 0) return null;
            S3StructureNode directory = directories.FirstOrDefault(d => d.Name == subPathParts[0]);
            if (directory != null) return directory.LookupNode(path);
            if (subPathParts.Length > 1) return null;
            return files.FirstOrDefault(f => f.Name == subPathParts[0]);
        }

        public IS3ObjectData GetObjectData()
        {
            throw new NotSupportedException();
        }

        public virtual Task Move(S3Path newPath)
        {
            throw new NotImplementedException();
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
            await DeleteRecursive();
        }
        protected virtual async Task Delete(bool unregisterFromParent = true)
        {
            if (Status == S3NodeStatus.Deleted) return;

            var children = await GetChildren();
            foreach (var directory in directories.ToList())
            {
                await directory.Delete(false);
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

        public IS3Node CreateFile(string name)
        {
            throw new NotImplementedException();
        }

        public IS3Node CreateDirectory(string name)
        {
            throw new NotImplementedException();
        }

        public async Task PersistChanges()
        {
            List<IS3Node> children = new();
            if (directories != null) children.AddRange(directories);
            if (files != null) children.AddRange(files);

            foreach (var child in children)
            {
                await child.PersistChanges();
            }
        }

        internal void CacheRemove(IS3Node s3Node)
        {
            if (s3Node is S3StructureNode s3Dir)
            {
                directories?.Remove(s3Dir);
            }
            else if (s3Node is S3ObjectNode s3Obj)
            {
                files?.Remove(s3Obj);
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
                directories?.Add(s3Dir);
            }
            else if (s3Node is S3ObjectNode s3Obj)
            {
                files?.Add(s3Obj);
            }
            else
            {
                throw new ArgumentException("unexpected node type " + s3Node);
            }
        }
    }
}