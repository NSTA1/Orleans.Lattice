using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IShardCountProvider"/> implementation backed by
/// the core library''s
/// <see cref="LatticeOptionsResolver"/>. Forwards
/// <see cref="GetShardCountAsync"/> to
/// <see cref="LatticeOptionsResolver.ResolveAsync(string)"/> and
/// returns the <c>ShardCount</c> field of the result. The resolver
/// chains through <see cref="ILatticeRegistry"/> for non-system trees
/// and applies lazy first-use seeding so the call is idempotent across
/// callers and silos.
/// </summary>
internal sealed class DefaultShardCountProvider(LatticeOptionsResolver resolver)
    : IShardCountProvider
{
    private readonly LatticeOptionsResolver _resolver =
        resolver ?? throw new ArgumentNullException(nameof(resolver));

    /// <inheritdoc />
    public async Task<int> GetShardCountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var resolved = await _resolver.ResolveAsync(treeId).ConfigureAwait(false);
        return resolved.ShardCount;
    }
}