using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ISnapshotProvider"/> implementation. Enumerates
/// every live entry in the source tree via the public
/// <see cref="ILattice.EntriesAsync"/> surface and stamps each with its
/// commit-time <see cref="HybridLogicalClock"/> via
/// <see cref="ILattice.GetWithVersionAsync"/>. The producer's
/// causal-stable frontier is read once up-front from the per-tree
/// <see cref="IReplicationHighWaterMarkGrain"/> so the consumer pins
/// the receiver's local vector clock to a stable value before
/// draining the entry stream.
/// <para>
/// <b>Performance note.</b> The default implementation pays one
/// per-key <see cref="ILattice.GetWithVersionAsync"/> round-trip on
/// top of the leaf-chain enumeration. This is correct but not
/// optimal at large key counts; a future revision can swap to a
/// streaming HLC-threshold leaf scan once the core library exposes
/// a version-bearing entries-newer-than primitive in a single pass.
/// Hosts that need a faster export today can register their own <see cref="ISnapshotProvider"/>
/// via DI before calling
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// </para>
/// </summary>
internal sealed class LatticeSnapshotProvider(IGrainFactory grainFactory) : ISnapshotProvider
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        // Read the producer's per-tree local vector clock up-front so
        // the receiver can pin it before draining the entry stream.
        // GetVectorAsync returns a defensive copy; SnapshotStream takes
        // ownership of the reference.
        var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
        var frontier = await hwm.GetVectorAsync(cancellationToken).ConfigureAwait(false);

        var entries = EnumerateAsync(treeName, asOfHlc, cancellationToken);
        return new SnapshotStream(treeName, asOfHlc, frontier, entries);
    }

    private async IAsyncEnumerable<SnapshotEntry> EnumerateAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lattice = _grainFactory.GetGrain<ILattice>(treeName);
        var hasUpperBound = asOfHlc != HybridLogicalClock.Zero;

        await foreach (var pair in lattice
            .EntriesAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var versioned = await lattice
                .GetWithVersionAsync(pair.Key, cancellationToken)
                .ConfigureAwait(false);

            if (versioned.Value is null)
            {
                // Tombstoned between EntriesAsync emitting the key and
                // the per-key version read; skip - the snapshot reflects
                // the live state at that read point.
                continue;
            }

            if (hasUpperBound && versioned.Version > asOfHlc)
            {
                continue;
            }

            yield return new SnapshotEntry
            {
                Key = pair.Key,
                Value = versioned.Value,
                Timestamp = versioned.Version,
            };
        }
    }
}

