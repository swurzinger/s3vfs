using System;

namespace s3vfs
{
    public interface IS3ObjectData
    {
        public byte[] Read(UInt64 offset, UInt32 length);
    }
}