using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaVersionStore"/>. Dogfoods the reserved
/// <c>sys-schema-version</c> <c>ILattice</c> tree: each tree's version config is
/// stored as a value under the governed tree id, so a config read is a single point
/// read and <see cref="ListConfigsAsync"/> is a full-tree scan. Mirrors
/// <c>LatticeSchemaPolicyStore</c> exactly.
/// </summary>
/// <remarks>
/// The store is versioning <b>infrastructure</b>: it reads and writes the version
/// tree that feeds the write interceptor and value decoder, so every operation runs
/// under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>. This avoids a
/// bootstrap paradox (the config tree itself must never be versioned) and keeps the
/// interceptor from re-entering itself when it stamps a config write. Authorizing
/// <i>who</i> may edit version config is a higher-layer concern
/// (<see cref="LatticeOperation.SchemaAdmin"/>), not the store's.
/// </remarks>
internal sealed class LatticeSchemaVersionStore(IGrainFactory grainFactory) : ILatticeSchemaVersionStore
{
    private ILattice Config => grainFactory.GetGrain<ILattice>(SchemaConstants.VersionConfigTree);

    /// <inheritdoc />
    public async Task SetConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Config.SetAsync(treeId, config, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaVersionConfig?> GetConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Config.GetAsync<LatticeSchemaVersionConfig>(treeId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<bool> ClearConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Config.DeleteAsync(treeId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<KeyValuePair<string, LatticeSchemaVersionConfig>> ListConfigsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Config
                .ScanEntriesAsync<LatticeSchemaVersionConfig>(cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is { } config)
                {
                    yield return new KeyValuePair<string, LatticeSchemaVersionConfig>(entry.Key, config);
                }
            }
        }
    }
}
