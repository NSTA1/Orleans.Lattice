using System.Diagnostics;
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
/// <see cref="ILatticeReplicationCursorRegistry"/> via
/// <see cref="ILatticeReplicationCursorRegistry.GetCausalStableAsync"/>:
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
    ILatticeReplicationCursorRegistry cursors,
    IInFlightSagaTracker sagaTracker,
    IOptionsMonitor<LatticeReplicationOptions> options) : ISnapshotProvider
{
    /// <summary>
    /// Polling interval used by the saga-quiesce loop. Sized so the
    /// loop wakes often enough to catch a quickly-completing saga
    /// without busy-spinning the silo when sagas are long-running.
    /// </summary>
    private static readonly TimeSpan QuiescePollInterval = TimeSpan.FromMilliseconds(50);

    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ILatticeReplicationCursorRegistry _cursors = cursors ?? throw new ArgumentNullException(nameof(cursors));
    private readonly IInFlightSagaTracker _sagaTracker = sagaTracker ?? throw new ArgumentNullException(nameof(sagaTracker));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <inheritdoc />
    public async Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        // Producer-side atomic-batch saga quiesce: before reading
        // the tree state, snapshot the set of sagas currently
        // mid-emission (observed by the producer-side
        // ReplicationMutationObserver) and wait up to
        // SnapshotSagaQuiesceTimeout for them to drain. Sagas that
        // complete during the wait are no longer mid-emission by
        // the time the entry scan begins so every per-key emit is
        // either fully visible in the producer's tree state (and
        // therefore included in the snapshot) or fully post-AsOfHlc
        // (and therefore delivered intact on the post-snapshot
        // incremental stream where the receiver-side staging buffer
        // can recognise the complete batch). Sagas that exceed the
        // quiesce window are stamped on the snapshot stream's
        // SagaBlacklist; the receiver bypasses the staging buffer
        // for those specific transaction ids, applying their
        // entries as point writes — atomic visibility is degraded
        // to causal+ for those sagas, the steady-state guarantee
        // for every other saga is preserved.
        var resolved = _options.Get(treeName);
        var blacklist = await QuiesceInFlightSagasAsync(
            treeName,
            resolved.SnapshotSagaQuiesceTimeout,
            cancellationToken).ConfigureAwait(false);

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
        var frontier = await _cursors
            .GetCausalStableAsync(treeName, cancellationToken)
            .ConfigureAwait(false);

        if (frontier is null)
        {
            var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
            frontier = await hwm.GetVectorAsync(cancellationToken).ConfigureAwait(false);
        }

        var entries = EnumerateAsync(treeName, asOfHlc, cancellationToken);
        return new SnapshotStream(treeName, asOfHlc, frontier, entries, blacklist);
    }

    /// <summary>
    /// Polls the in-flight saga tracker until either every saga
    /// observed at the start of the wait has completed emission, or
    /// <paramref name="quiesceTimeout"/> elapses. Returns the set of
    /// transaction ids still in flight when the wait completes.
    /// <para>
    /// The wait is intentionally a polling loop rather than an
    /// event-driven wait: the tracker is a process-local proxy for
    /// "the producer's tree state has every per-key emit committed
    /// for this saga", and the polling cadence (50 ms) is short
    /// enough to catch a sub-second saga without busy-spinning when
    /// sagas are long-running. A quiesce window with no in-flight
    /// sagas at entry returns immediately with an empty blacklist.
    /// </para>
    /// </summary>
    private async Task<IReadOnlyList<Guid>> QuiesceInFlightSagasAsync(
        string treeName,
        TimeSpan quiesceTimeout,
        CancellationToken cancellationToken)
    {
        // Capture the initial in-flight set: only sagas observed
        // *now* (or sagas that show up later but were captured
        // mid-emission by the same wall-clock reading) are eligible
        // for the wait. A saga that starts after the quiesce window
        // begins is treated the same as a saga that starts after
        // the snapshot's AsOfHlc — its keys are entirely
        // post-snapshot and the receiver's incremental path
        // recognises the complete batch.
        var initial = _sagaTracker.GetInFlightTransactions(treeName);
        if (initial.Count == 0)
        {
            return Array.Empty<Guid>();
        }

        // Tracking set keyed by transaction id, used to test
        // membership cheaply on every poll. Sized for the captured
        // set; the snapshot's blacklist is computed from this set
        // intersected with the post-poll in-flight reading so a
        // saga that started during the wait does not pollute the
        // blacklist (the observer's intent is "wait for the sagas
        // that were mid-emission *at quiesce start*").
        var tracked = new HashSet<Guid>(initial);

        // Stopwatch-based deadline rather than DateTime.UtcNow:
        // monotonic, NTP-step-immune, and avoids the wall-clock
        // failure mode where an NTP correction during the wait
        // either truncates the window to zero (clock jumps forward)
        // or hangs the loop (clock jumps backward). The 50 ms
        // poll cadence dominates the call cost; Stopwatch.Elapsed
        // is sub-microsecond on modern hardware.
        var sw = Stopwatch.StartNew();

        // Hoisted out of the loop so the post-timeout blacklist
        // computation reuses the loop's last reading instead of
        // calling GetInFlightTransactions a third time (the prior
        // shape did one read per iteration plus a redundant final
        // read after the timeout fired).
        IReadOnlyList<Guid> current = initial;

        while (sw.Elapsed < quiesceTimeout)
        {
            cancellationToken.ThrowIfCancellationRequested();

            current = _sagaTracker.GetInFlightTransactions(treeName);

            // Allocation-free overlap probe: AnyInFlight (default
            // method on IInFlightSagaTracker, overridden by the
            // in-process tracker for an O(N) hashed scan) avoids
            // building a fresh List<Guid> per poll tick the way
            // GetInFlightTransactions + linear-scan would.
            if (!_sagaTracker.AnyInFlight(treeName, tracked))
            {
                return Array.Empty<Guid>();
            }

            // Plain await on Task.Delay: cancellation propagates
            // naturally as OperationCanceledException via the
            // ThrowIfCancellationRequested guard at the top of the
            // loop and via Task.Delay's own ct propagation. The
            // earlier shape wrapped this in a try/catch that only
            // re-threw the same exception — cosmetic dead code that
            // suggested a swallow path that did not exist.
            await Task.Delay(QuiescePollInterval, cancellationToken).ConfigureAwait(false);
        }

        // Timeout reached: emit the still-in-flight subset of the
        // initially-tracked set as the blacklist. Reuse the loop's
        // last `current` reading rather than doing a third
        // tracker scan — the loop necessarily set current to a
        // tracker reading on its last iteration, and any saga
        // that completed between that read and the elapsed-time
        // check would have caused the loop to return inside the
        // !AnyInFlight branch.
        if (current.Count == 0)
        {
            return Array.Empty<Guid>();
        }

        List<Guid>? blacklist = null;
        for (var i = 0; i < current.Count; i++)
        {
            if (tracked.Contains(current[i]))
            {
                blacklist ??= new List<Guid>(capacity: tracked.Count);
                blacklist.Add(current[i]);
            }
        }

        return (IReadOnlyList<Guid>?)blacklist ?? Array.Empty<Guid>();
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

