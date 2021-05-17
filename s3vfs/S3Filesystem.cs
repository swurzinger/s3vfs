using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Fsp;
using Fsp.Interop;
using FileInfo = Fsp.Interop.FileInfo;

namespace s3vfs
{
    public class S3Filesystem : FileSystemBase
    {
        private const int ALLOCATION_UNIT = 4096;
        private const int SECTORS_PER_ALLOCATION_UNIT = 1;
        private static readonly Byte[] EmptyByteArray = new Byte[0];
        private static readonly Byte[] DefaultSecurity;

        private S3RootNode S3RootNode;
        private FileSystemHost Host;

        static S3Filesystem()
        {
            RawSecurityDescriptor rootSecurityDescriptor = new RawSecurityDescriptor(
                "O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)");
            DefaultSecurity = new Byte[rootSecurityDescriptor.BinaryLength];
            rootSecurityDescriptor.GetBinaryForm(DefaultSecurity, 0);
        }

        public S3Filesystem(string serviceUrl, string accessKey, string secretKey)
        {
            AmazonS3Config config = new AmazonS3Config()
            {
                ServiceURL = serviceUrl,
                UseHttp = true,
                ForcePathStyle = true,
            };

            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            var s3Client = new AmazonS3Client(credentials, config);
            S3RootNode = new S3RootNode(s3Client);
        }

        public override Int32 Init(Object hostObject)
        {
            Host = (FileSystemHost)hostObject;
            Host.SectorSize = ALLOCATION_UNIT;
            Host.SectorsPerAllocationUnit = SECTORS_PER_ALLOCATION_UNIT;
            Host.SectorsPerAllocationUnit = 1;
            Host.CaseSensitiveSearch = true;
            Host.CasePreservedNames = true;
            Host.UnicodeOnDisk = true;
            Host.PersistentAcls = false;
            Host.PostCleanupWhenModifiedOnly = true;
            Host.VolumeCreationTime = 0;
            Host.VolumeSerialNumber = 0;

            return STATUS_SUCCESS;
        }


        public override Int32 Mounted(Object host)
        {
            return STATUS_SUCCESS;
        }

        public override void Unmounted(Object host)
        {
        }

        public override Int32 GetVolumeInfo(out VolumeInfo volumeInfo)
        {
            volumeInfo = default;
            volumeInfo.FreeSize = 1024 * 1024 * 1024;
            volumeInfo.TotalSize = 1024 * 1024 * 1024;
            volumeInfo.SetVolumeLabel(S3RootNode.Name);

            return STATUS_SUCCESS;
        }

        public override Int32 GetSecurityByName(
            String fileName,
            out UInt32 fileAttributes /* or ReparsePointIndex */,
            ref Byte[] securityDescriptor)
        {
            IS3Node node = FileLookup(fileName);
            if (node == null)
            {
                fileAttributes = default;
                return STATUS_OBJECT_NAME_NOT_FOUND;
            }

            fileAttributes = node.GetFileInfo().FileAttributes;
            if (null != securityDescriptor)
                securityDescriptor = DefaultSecurity;

            return STATUS_SUCCESS;
        }


        public override Int32 Open(
            String fileName,
            UInt32 createOptions,
            UInt32 grantedAccess,
            out Object fileNode,
            out Object fileDesc,
            out FileInfo fileInfo,
            out String normalizedName)
        {
            fileNode = default;
            fileDesc = default;
            fileInfo = default;
            normalizedName = default;

            var s3Node = FileLookup(fileName);
            if (s3Node == null)
                return STATUS_OBJECT_NAME_NOT_FOUND;

            fileNode = s3Node;
            fileInfo = s3Node.GetFileInfo();

            return STATUS_SUCCESS;
        }


        public override Int32 Read(
            Object fileNode,
            Object fileDesc,
            IntPtr buffer,
            UInt64 offset,
            UInt32 length,
            out UInt32 bytesTransferred)
        {
            IS3Node s3Node = (IS3Node)fileNode;
            ulong fileSize = s3Node.GetFileInfo().FileSize;

            Console.Out.WriteLine($"Read {s3Node.Path} from offset {offset} length {length} bytes");

            if (offset >= fileSize)
            {
                bytesTransferred = 0;
                return STATUS_END_OF_FILE;
            }

            var endOffset = offset + length;
            if (endOffset > fileSize)
                endOffset = fileSize;

            bytesTransferred = (UInt32)(endOffset - offset);
            // Marshal.Copy(contents, (int)offset, buffer, (int)bytesTransferred);

            var requestedContent = s3Node.GetObjectData().Read(offset, bytesTransferred);
            bytesTransferred = (uint)requestedContent.Length;

            Marshal.Copy(requestedContent, 0, buffer, requestedContent.Length);

            return STATUS_SUCCESS;
        }


        public override Int32 GetFileInfo(
            Object fileNode,
            Object fileDesc,
            out FileInfo fileInfo)
        {
            IS3Node s3Node = (IS3Node)fileNode;

            fileInfo = s3Node.GetFileInfo();

            return STATUS_SUCCESS;
        }

#if false
        public override int ReadDirectory(object fileNode, object fileDesc, string pattern, string marker, IntPtr buffer, uint length, out uint bytesTransferred)
        {
            var operationRequestHint = Host.GetOperationRequestHint();
            try
            {
                var task = ReadDirectoryAsync(fileNode, fileDesc, pattern, marker, buffer, length, operationRequestHint);
                task.Start();
                bytesTransferred = 0;
                return STATUS_PENDING;
            }
            catch
            {
                bytesTransferred = 0;
                return STATUS_IO_TIMEOUT;
            }
        }

        private async Task ReadDirectoryAsync(object fileNode, object fileDesc, string pattern, string marker, IntPtr buffer, uint length, ulong operationRequestHint)
        {
            UInt32 bytesTransferred;
            var Status = SeekableReadDirectory(fileNode, fileDesc, pattern, marker, buffer, length, out bytesTransferred);
            Host.SendReadDirectoryResponse(operationRequestHint, Status, bytesTransferred);
        }
#endif

        public override Boolean ReadDirectoryEntry(
            Object fileNode,
            Object fileDesc,
            String pattern,
            String marker,
            ref Object context,
            out String fileName,
            out FileInfo fileInfo)
        {

            IS3Node s3Node = (IS3Node)fileNode;
            IEnumerator<object> enumerator = (IEnumerator<object>)context;

            if (enumerator == null)
            {
                List<String> dotEntries = new List<String>();
                if (s3Node != S3RootNode)
                {
                    /* if this is not the root directory add the dot entries */
                    if (null == marker)
                        dotEntries.Add(".");
                    if (null == marker || "." == marker)
                        dotEntries.Add("..");
                }

                var children = s3Node.GetChildren("." != marker && ".." != marker ? marker : null).Result;
                context = enumerator = dotEntries.Cast<object>().Concat(children).GetEnumerator();
            }

            while (enumerator.MoveNext())
            {
                if (enumerator.Current is String dirName)
                {
                    fileName = dirName;
                    if (dirName == ".") {
                        fileInfo = s3Node.GetFileInfo();
                        return true;
                    }
                    else if (dirName == "..")
                    {
                        var parent = FileLookup(s3Node.Path.Parent);
                        if (parent != null)
                        {
                            fileInfo = parent.GetFileInfo();
                            return true;
                        }
                    }
                }
                else if (enumerator.Current is IS3Node childS3Node)
                {
                    fileName = childS3Node.Name;
                    fileInfo = childS3Node.GetFileInfo();
                    return true;
                }
            }

            fileName = default;
            fileInfo = default;
            return false;
        }


        public override int Rename(object fileNode, object fileDesc, string fileName, string newFileName, bool replaceIfExists)
        {
            IS3Node s3Node = (IS3Node)fileNode;
            return STATUS_IO_DEVICE_ERROR;
        }

        public override int CanDelete(object fileNode, object fileDesc, string fileName)
        {
            IS3Node s3Node = (IS3Node)fileNode;
            if (s3Node.GetChildren().Result.Any())
                return STATUS_DIRECTORY_NOT_EMPTY;

            return STATUS_SUCCESS;
        }

        public override void Cleanup(object fileNode, object fileDesc, string fileName, uint flags)
        {
            IS3Node s3Node = (IS3Node)fileNode;

            if (0 != (flags & CleanupDelete) && CanDelete(fileNode, fileDesc, fileName) == 0)
            {
                s3Node.DeleteRecursive();
            }
        }

        public override int SetDelete(object fileNode, object fileDesc, string fileName, bool deleteFile)
        {
            IS3Node s3Node = (IS3Node)fileNode;
            if (deleteFile)
            {
                s3Node.MarkForDeletion();
                return STATUS_SUCCESS;
            }

            return STATUS_NOT_IMPLEMENTED;
        }

        public override Int32 CreateEx(
            String fileName,
            UInt32 createOptions,
            UInt32 grantedAccess,
            UInt32 fileAttributes,
            Byte[] securityDescriptor,
            UInt64 allocationSize,
            IntPtr extraBuffer,
            UInt32 extraLength,
            Boolean extraBufferIsReparsePoint,
            out Object fileNode,
            out Object fileDesc,
            out FileInfo fileInfo,
            out String normalizedName)
        {
            fileNode = default(Object);
            fileDesc = default(Object);
            fileInfo = default(FileInfo);
            normalizedName = default(String);

            // Int32 result = STATUS_SUCCESS;
            // return STATUS_CANNOT_MAKE;
            // return STATUS_DISK_FULL;

            var filePath = S3Path.FromPath(fileName);
            var s3Node = FileLookup(filePath);
            if (null != s3Node)
                return STATUS_OBJECT_NAME_COLLISION;
            IS3Node parentNode = FileLookup(filePath.Parent);
            if (null == parentNode)
                return STATUS_OBJECT_NAME_NOT_FOUND;

            if (0 != (createOptions & FILE_DIRECTORY_FILE))
            {
                s3Node = parentNode.CreateDirectory(filePath.Name);
                allocationSize = 0;
            }
            else
            {
                s3Node = parentNode.CreateFile(filePath.Name);
            }

            if (s3Node == null) return STATUS_IO_DEVICE_ERROR;

            // TODO: implement write and resize
            // if (0 != allocationSize)
            // {
            //     result = SetFileSizeInternal(FileNode, allocationSize, true);
            //     if (0 > result)
            //         return result;
            // }

            // Interlocked.Increment(ref s3Node.OpenCount);
            fileNode = s3Node;
            fileInfo = s3Node.GetFileInfo();
            normalizedName = s3Node.Name;

            return STATUS_SUCCESS;
        }


        private IS3Node FileLookup(String fileName)
        {
            var path = S3Path.FromPath(fileName);
            return FileLookup(path);
        }

        private IS3Node FileLookup(S3Path path)
        {
            return S3RootNode.LookupNode(path);
        }

    }
}