using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The registry registration a bulk write was issued under (issue #5813), carried
/// from its issuer to the shard root's purged-copy bulk gate in the
/// <see cref="RequestContext"/>, bound to the tree it was issued for.
/// <para>
/// A restore builds its shadow under a deterministic id and stamps every shard bulk
/// load with the shadow registration's <see cref="State.TreeRegistryEntry.Lineage"/>.
/// When the orphan-shadow GC purges and unregisters that shadow while a load whose
/// caller gave up is still in flight, and a re-issued restore registers the same id
/// again under a fresh lineage, the stale load reaches a purged copy the registry
/// names live once more. The gate admits a stamped bulk write to a purged copy only
/// when the registry still names the copy under the stamped lineage, and the
/// re-seed it then runs never registers the id itself.
/// </para>
/// <para>
/// The stamp restricts and never grants: an unstamped call, or one whose stamp names
/// another tree, takes the unchanged #4503 rule. Both values are primitives, so a
/// silo that predates the stamp deserializes and ignores them.
/// </para>
/// </summary>
internal static class BulkWriteRegistrationLineage
{
    /// <summary>The request-context key that carries the tree the stamp was issued for.</summary>
    internal const string TreeRequestContextKey = "ol.bulk.regtree";

    /// <summary>The request-context key that carries the registration's lineage.</summary>
    internal const string LineageRequestContextKey = "ol.bulk.reglin";

    /// <summary>
    /// Stamps the current flow with the registration <paramref name="lineage"/> of
    /// <paramref name="treeId"/> for the lifetime of the returned scope, restoring the
    /// prior stamp on disposal.
    /// </summary>
    /// <param name="treeId">The physical tree the bulk write addresses.</param>
    /// <param name="lineage">The lineage of the registration the write was issued under.</param>
    internal static IDisposable Stamp(string treeId, Guid lineage)
    {
        var scope = new Scope(
            RequestContext.Get(TreeRequestContextKey),
            RequestContext.Get(LineageRequestContextKey));
        RequestContext.Set(TreeRequestContextKey, treeId);
        RequestContext.Set(LineageRequestContextKey, lineage);
        return scope;
    }

    /// <summary>
    /// The lineage the current flow was stamped with for <paramref name="treeId"/>, or
    /// <see langword="null"/> when it carries no stamp for that tree.
    /// </summary>
    /// <param name="treeId">The physical tree of the shard root asking.</param>
    internal static Guid? For(string treeId) =>
        RequestContext.Get(LineageRequestContextKey) is Guid lineage
        && RequestContext.Get(TreeRequestContextKey) is string stamped
        && string.Equals(stamped, treeId, StringComparison.Ordinal)
            ? lineage
            : null;

    private sealed class Scope(object? previousTree, object? previousLineage) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Restore(TreeRequestContextKey, previousTree);
            Restore(LineageRequestContextKey, previousLineage);
        }

        private static void Restore(string key, object? previous)
        {
            if (previous is null)
            {
                RequestContext.Remove(key);
            }
            else
            {
                RequestContext.Set(key, previous);
            }
        }
    }
}
