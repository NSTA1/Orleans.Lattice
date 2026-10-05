namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Reads only the recorded digest, so the eviction and partitioning tests above
/// stay about the index's shape; the recorded source identity has its own tests.
/// </summary>
internal static class ReceiverAppliedContentIndexTestExtensions
{
    public static bool TryGetContentHash(this ReceiverAppliedContentIndex index, string treeName, string key, out ulong contentHash)
    {
        var found = index.TryGetContent(treeName, key, out var held);
        contentHash = held.ContentHash;
        return found;
    }
}
