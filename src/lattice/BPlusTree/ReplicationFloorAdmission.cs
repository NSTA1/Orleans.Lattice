using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The bootstrap drop-floor epoch a replicated write was admitted under (issue
/// #4549), carried on the Orleans request context from the replication applier
/// through the routing tier to the shard root that applies it.
/// <para>
/// Installing a bootstrap drop floor bumps the tree's floor epoch. A write the
/// applier admitted before the install was never checked against the floor, so
/// once every shard root holds the new epoch it refuses a write carrying an
/// older one with <see cref="ReplicationFloorAdmissionStaleException"/>, and the
/// sender re-ships it against the floor. Writes that carry no epoch - local
/// writes, and the rows a bootstrap drain applies - are never refused. Distinct
/// from <see cref="ReplicationAdmissionEpoch"/>, the restore receive fence's
/// epoch (#4593).
/// </para>
/// <para>
/// Stamp it only from inside an <see langword="async"/> method: the request
/// context is flow-scoped, so the value reaches the calls that method makes and
/// is discarded when it returns.
/// </para>
/// </summary>
internal static class ReplicationFloorAdmission
{
    /// <summary>The request-context key that carries the floor epoch.</summary>
    internal const string EpochRequestContextKey = "lattice.replication.floor-epoch";

    /// <summary>Stamps the current flow with the floor epoch its write was admitted under.</summary>
    /// <param name="epoch">The floor epoch the admission observed.</param>
    internal static void Stamp(long epoch) => RequestContext.Set(EpochRequestContextKey, epoch);

    /// <summary>Removes the floor-epoch stamp from the current flow.</summary>
    internal static void Clear() => RequestContext.Remove(EpochRequestContextKey);

    /// <summary>Reads the current flow's floor-epoch stamp.</summary>
    /// <param name="epoch">The floor epoch the admission observed.</param>
    /// <returns><see langword="true"/> when the flow carries a stamp.</returns>
    internal static bool TryGet(out long epoch)
    {
        if (RequestContext.Get(EpochRequestContextKey) is long observed)
        {
            epoch = observed;
            return true;
        }

        epoch = 0;
        return false;
    }
}
