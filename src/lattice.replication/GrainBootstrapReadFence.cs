using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IBootstrapReadFence"/>: the shard-root read fence and the
/// migration and resize interlock in <see cref="TreeBootstrapReadFence"/>.
/// </summary>
internal sealed class GrainBootstrapReadFence(IGrainFactory grainFactory, LatticeOptionsResolver optionsResolver)
    : IBootstrapReadFence
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly LatticeOptionsResolver _optionsResolver = optionsResolver ?? throw new ArgumentNullException(nameof(optionsResolver));

    /// <inheritdoc />
    public Task<TreeBootstrapReadFence.Shards> ResolveAsync(string treeName) =>
        TreeBootstrapReadFence.ResolveAsync(_grainFactory, _optionsResolver, treeName);

    /// <inheritdoc />
    public Task SetAsync(TreeBootstrapReadFence.Shards shards, bool fenced) =>
        TreeBootstrapReadFence.SetAsync(_grainFactory, shards, fenced);

    /// <inheritdoc />
    public Task<string?> FindBlockerAsync(string treeName, TreeBootstrapReadFence.Shards shards) =>
        TreeBootstrapReadFence.FindBlockerAsync(_grainFactory, treeName, shards);
}
