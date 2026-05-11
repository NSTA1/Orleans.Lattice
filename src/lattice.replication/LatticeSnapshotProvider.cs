using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
///
/// Default <see cref="ISnapshotProvider"/> implementation. Enumerates
/// every live entry in the source tree via the public
/// <see cref="ILattice.EntriesAsync"/> surface and stamps each with its
/// commit-time <see cref="HybridLogicalClock"/> via
/// <see cref="ILattice.GetWithVersionAsync"/>. The snapshot's
/// <see cref="SnapshotStream.CausalStableFrontier"/> is read once
/// up-front from the
/// <see cref="IWalCursorRegistry"/> via
/// <see cref="IWalCursorRegistry.GetCausalStableAsync"/>:
/// the snapshot is cut at the producer's causal-stable frontier
/// (<c>min(consumer VC)</c>), so a receiver pinning that frontier on
/// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/> can
/// safely accept the first incremental entry under the dependency
/// check without parking it. When no consumer has reported a vector
/// yet (the common case for a single-peer cluster, a fresh deployment
/// before the first ack-with-VC, or a host that has not wired up the
/// causal+ overload), the provider falls back to the producer's
/// per-tree local vector clock from
/// <see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/>; this
/// is a strict superset of the causal-stable meet and is safe as a
/// snapshot cut-point because there are no entries above the
/// producer's local VC at snapshot time.
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
internal sealed class LatticeSnapshotProvider(
    IGrainFactory grainFactory,
    IWalCursorRegistry cursors,
    IOptionsMonitor<LatticeReplicationOptions> options) : ISnapshotProvider
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IWalCursorRegistry _cursors = cursors ?? throw new ArgumentNullException(nameof(cursors));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <inheritdoc />
    public async Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        // Read the producer's causal-stable frontier once up-front.
        // The cursor registry's GetCausalStableAsync is the canonical
        // snapshot cut-point per the causal+ design (snapshot_frontier
        // = causal_stable). When the registry has not yet observed a
        // VC-shaped report from any consumer (new deployment, single-
        // peer cluster, host using the legacy HLC-only overload), fall
        // back to the producer's per-tree local vector clock - a strict
        // superset of the meet that is safe as a snapshot cut because
        // no entry can have a VC component above the producer's own
        // local VC at the moment of capture.
        _ = _options.Get(treeName);
        var frontier = await _cursors
            .GetCausalStableAsync(treeName, cancellationToken)
            .ConfigureAwait(false);

        if (frontier is null)
        {
            var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
            frontier = await hwm.GetVectorAsync(cancellationToken).ConfigureAwait(false);
        }

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

