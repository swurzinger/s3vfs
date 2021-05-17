namespace s3vfs
{
    public enum S3NodeStatus
    {
        Active,
        MarkedForDeletion,
        Deleted,
        New,
        Modified,
    }
}