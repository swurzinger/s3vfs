using Microsoft.Extensions.Caching.Memory;

namespace s3vfs
{
    public static class S3CacheManager
    {
        public static readonly IMemoryCache ObjectDataCache;

        static S3CacheManager()
        {
            var cacheOptions = new MemoryCacheOptions()
            {
                // ~ 4GB cache size; cache block size = 64 * 4096 bytes = 256 KB
                SizeLimit = 4 * 1024 * 4,
            };
            ObjectDataCache = new MemoryCache(cacheOptions);
        }
    }
}