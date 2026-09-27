using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Apps;

/// <summary>
/// <see cref="IAppTreeProvisioner"/> over the core tree registry (idempotent registration with
/// the declared structural pins) and the tree soft-delete seam. Runs system-origin: the app is
/// being provisioned by the platform, not by a caller.
/// </summary>
internal sealed class LatticeAppTreeProvisioner : IAppTreeProvisioner
{
    private readonly IGrainFactory _grainFactory;

    public LatticeAppTreeProvisioner(IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        _grainFactory = grainFactory;
    }

    public async Task EnsureAsync(string treeId, AppTreeDeclaration declaration, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(declaration);
        cancellationToken.ThrowIfCancellationRequested();

        using (LatticeSystemOrigin.Enter())
        {
            var registry = _grainFactory.GetLatticeRegistry();
            if (await registry.ExistsAsync(treeId).ConfigureAwait(false))
            {
                // An existing tree keeps its pinned structure; only a soft-deleted one (left by
                // an earlier uninstall or a dropped tree) is brought back within its window.
                if (await _grainFactory.GetGrain<ITreeDeletionGrain>(treeId).IsDeletedAsync().ConfigureAwait(false))
                {
                    await _grainFactory.GetGrain<ILattice>(treeId).RecoverTreeAsync(cancellationToken).ConfigureAwait(false);
                }

                return;
            }

            await registry.RegisterAsync(treeId, BuildEntry(declaration)).ConfigureAwait(false);
        }
    }

    public async Task SoftDeleteAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        using (LatticeSystemOrigin.Enter())
        {
            if (!await _grainFactory.GetLatticeRegistry().ExistsAsync(treeId).ConfigureAwait(false))
            {
                return;
            }

            await _grainFactory.GetGrain<ILattice>(treeId).DeleteTreeAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal static TreeRegistryEntry? BuildEntry(AppTreeDeclaration declaration)
    {
        if (declaration.ShardCount is null
            && declaration.VirtualShardCount is null
            && declaration.MaxLeafKeys is null
            && declaration.MaxInternalChildren is null
            && declaration.WalPartitions is null)
        {
            return null;
        }

        return new TreeRegistryEntry
        {
            ShardCount = declaration.ShardCount,
            MaxLeafKeys = declaration.MaxLeafKeys,
            MaxInternalChildren = declaration.MaxInternalChildren,
            WalPartitions = declaration.WalPartitions,
            // The virtual routing space is pinned on first registration only; the manifest
            // validator forbids changing it on upgrade, and an existing tree is never re-pinned.
            ShardMap = declaration.VirtualShardCount is { } virtualShards
                ? ShardMap.CreateDefault(virtualShards, declaration.ShardCount ?? LatticeConstants.DefaultShardCount)
                : null,
        };
    }
}
