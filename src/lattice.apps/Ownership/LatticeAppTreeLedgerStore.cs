using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The production <see cref="IAppTreeLedgerStore"/>: dogfoods the reserved <c>sys-app-trees</c>
/// <see cref="ILattice"/> tree, storing each <see cref="AppTreeClaim"/> in the Orleans binary wire
/// format. Every read and write runs under system origin, both to skip the access gate and to
/// satisfy the reserved-prefix write guard, exactly as the app registry store does.
/// </summary>
internal sealed class LatticeAppTreeLedgerStore : IAppTreeLedgerStore
{
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer<AppTreeClaim> _serializer;

    /// <summary>Initializes a new <see cref="LatticeAppTreeLedgerStore"/>.</summary>
    /// <param name="grainFactory">The grain factory used to open the ledger tree.</param>
    /// <param name="serializer">The Orleans serializer for claims.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeAppTreeLedgerStore(IGrainFactory grainFactory, Serializer<AppTreeClaim> serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _serializer = serializer;
    }

    private ILattice Ledger => _grainFactory.GetGrain<ILattice>(AppRegistryTreeNames.TreeLedgerTree);

    /// <inheritdoc />
    public async Task<AppTreeLedgerRead> GetAsync(string treeId, CancellationToken cancellationToken)
    {
        VersionedValue read;
        using (LatticeSystemOrigin.Enter())
        {
            read = await Ledger.GetWithVersionAsync(treeId, cancellationToken).ConfigureAwait(false);
        }

        return read.Value is { } bytes
            ? new AppTreeLedgerRead(_serializer.Deserialize(bytes), read.Version)
            : new AppTreeLedgerRead(null, HybridLogicalClock.Zero);
    }

    /// <inheritdoc />
    public async Task<bool> TrySetAsync(
        string treeId,
        AppTreeClaim claim,
        HybridLogicalClock expectedVersion,
        CancellationToken cancellationToken)
    {
        var bytes = _serializer.SerializeToArray(claim);
        using (LatticeSystemOrigin.Enter())
        {
            return await Ledger.SetIfVersionAsync(treeId, bytes, expectedVersion, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<KeyValuePair<string, AppTreeClaim>> ScanAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await foreach (var entry in Ledger
                .ScanEntriesAsync(null, null, cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                yield return new(entry.Key, _serializer.Deserialize(entry.Value));
            }
        }
    }
}
