using System;

namespace s3vfs
{
    public interface IS3ObjectData
    {
        public byte[] Read(UInt64 offset, UInt32 length);

        public void Write(UInt64 offset, byte[] data);

        public void SetFileSize(UInt64 size, bool setAllocatedSize = false);
    }
}