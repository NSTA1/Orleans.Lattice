using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The production <see cref="IAppRegistryStore"/>. Dogfoods the reserved
/// <c>sys-app-registry</c> <see cref="ILattice"/> tree, storing each
/// <see cref="AppRegistryRecord"/> in the Orleans binary wire format. Every read and
/// write runs under system-origin, both to skip the access gate and to satisfy the
/// reserved-prefix write guard, exactly as the tenant registry does; authorization of
/// the caller happens in the registry before this store is reached.
/// </summary>
internal sealed class LatticeAppRegistryStore : IAppRegistryStore
{
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer<AppRegistryRecord> _serializer;

    /// <summary>Initializes a new <see cref="LatticeAppRegistryStore"/>.</summary>
    /// <param name="grainFactory">The grain factory used to open the registry tree.</param>
    /// <param name="serializer">The Orleans serializer for registry records.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeAppRegistryStore(IGrainFactory grainFactory, Serializer<AppRegistryRecord> serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _serializer = serializer;
    }

    private ILattice Registry => _grainFactory.GetGrain<ILattice>(AppRegistryTreeNames.RegistryTree);

    /// <inheritdoc />
    public async Task<AppRegistryStoreRead> GetAsync(string key, CancellationToken cancellationToken)
    {
        VersionedValue read;
        using (LatticeSystemOrigin.Enter())
        {
            read = await Registry.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        }

        return read.Value is { } bytes
            ? new AppRegistryStoreRead(_serializer.Deserialize(bytes), read.Version)
            : new AppRegistryStoreRead(null, HybridLogicalClock.Zero);
    }

    /// <inheritdoc />
    public async Task<bool> TrySetAsync(
        string key,
        AppRegistryRecord record,
        HybridLogicalClock expectedVersion,
        CancellationToken cancellationToken)
    {
        var bytes = _serializer.SerializeToArray(record);
        using (LatticeSystemOrigin.Enter())
        {
            return await Registry.SetIfVersionAsync(key, bytes, expectedVersion, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<AppRegistryRecord> ScanAsync(
        string? startInclusive,
        string? endExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using (LatticeSystemOrigin.Enter())
        {
            // The resilient ScanEntriesAsync wrapper reopens the scan from the last
            // yielded key when the tree grain deactivates mid-stream, so a snapshot
            // rebuild sees one deterministic enumeration with no duplicates or gaps.
            await foreach (var entry in Registry
                .ScanEntriesAsync(startInclusive, endExclusive, cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                yield return _serializer.Deserialize(entry.Value);
            }
        }
    }
}
