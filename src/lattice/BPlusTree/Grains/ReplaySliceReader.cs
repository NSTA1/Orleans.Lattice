using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The single owner of the per-replay WAL slice width and of the
/// narrow-and-retry mechanism that moves it (issues #2742, #2899).
/// <para>
/// <b>Why this is a type rather than a constant.</b> Every WAL replay in this
/// library reads its commit log through
/// <see cref="ILeafReplayCoordinatorGrain.ReadSliceAsync"/> in fixed-width
/// slices, and the width is the second of the two factors that set peak replay
/// memory - peak is the product of how many replays run at once and how much
/// each one buffers (issue #2867). Issue #2742 taught the activation-time
/// replay to spend width to stay alive: a read that fails for memory pressure
/// is retried at a quarter of the width rather than unwinding the whole
/// partition. That change was applied at one of three sites. The other two kept
/// a constant of the same name and the same value, so every check that compared
/// the two <i>values</i> passed while the <i>behaviour</i> had diverged, and the
/// divergence went unnoticed from #2742 until issue #2899.
/// </para>
/// <para>
/// Sharing the mechanism is therefore the point of this type, not a tidying of
/// it. A constant can be copied to a fourth site by anyone; a reader that owns
/// the retry, the widening, the counter and the counter's zero-priming together
/// cannot be partially adopted. <c>ReplaySliceReaderInventoryTests</c> pins that
/// property from the other side by failing any <c>ReadSliceAsync</c> call site
/// in <c>src/</c> outside this file.
/// </para>
/// <para>
/// <b>The two sites that lacked the mechanism are the ones with the least other
/// protection, not the most.</b> The activation-time replay - the site that
/// already narrowed - is the only one of the three that holds a permit from the
/// per-silo replay concurrency gate. The snapshot-cursor rebuild
/// (<c>SnapshotLeafGrain.ReplayWalAsync</c>) and the frozen-baseline tail fold
/// (<c>BPlusLeafGrain.FoldTailOntoFrozenAsync</c>) take no permit at all, so
/// before this change both terms of the product were unbounded at exactly those
/// two sites: no ceiling on how many run at once, and no floor on the width each
/// one demands. The snapshot rebuild compounds it by replaying from offset 0
/// rather than from a checkpoint, over a WAL prefix a cursor pin is holding
/// alive - so on a tree whose log has grown it is the site reading the most
/// bytes with the least protection.
/// </para>
/// <para>
/// One reader instance owns the width for one partition's replay, matching the
/// scope the activation-time local had. Instances are not thread-safe and are
/// not intended to be shared across concurrent replays.
/// </para>
/// </summary>
internal sealed class ReplaySliceReader
{
    /// <summary>
    /// Width a replay starts at, and the ceiling
    /// <see cref="ReadSliceAsync"/> widens back towards after a narrowing, when
    /// a caller supplies no configured width.
    /// <para>
    /// Bound to <see cref="LatticeOptions.DefaultWalReplaySliceBudget"/> rather
    /// than repeating the literal (issue #2898). The binding is a compile-time
    /// dependency, so the two cannot drift apart at all - deleting or renaming
    /// the option's default is a build error here, where an equality test
    /// between two independently declared constants could be deleted and a
    /// comparison of their values is the detector that already failed between
    /// issues #2742 and #2899.
    /// </para>
    /// </summary>
    internal const int InitialBudget = LatticeOptions.DefaultWalReplaySliceBudget;

    private readonly ILeafReplayCoordinatorGrain _coordinator;
    private readonly string _treeId;
    private readonly int _partition;
    private readonly int _initialBudget;

    private int _budget;

    /// <summary>
    /// Creates a reader for one partition's replay and zero-primes the
    /// narrowing counter for it.
    /// <para>
    /// Priming in the constructor is deliberate: it is what makes the counter's
    /// coverage a property of <i>using the reader</i> rather than of remembering
    /// to prime beside it. A <see cref="System.Diagnostics.Metrics.Counter{T}"/>
    /// exports no series until its first <c>Add</c>, and the healthy steady
    /// state here is never to narrow at all - so an unprimed site leaves its
    /// common case indistinguishable from a build that cannot narrow, which is
    /// the precise ambiguity issue #2867 added the counter to remove. Were
    /// priming left to the call sites, a fourth site could adopt the retry and
    /// silently reintroduce that ambiguity for its own tree and partition.
    /// Adding zero mints the series with the exact tag set a later narrowing
    /// carries and cannot perturb the value.
    /// </para>
    /// </summary>
    /// <param name="coordinator">Per-shard WAL read coordinator to read through.</param>
    /// <param name="treeId">Tree whose log is being replayed; a metric dimension.</param>
    /// <param name="partition">WAL partition being replayed; a metric dimension.</param>
    /// <param name="initialBudget">
    /// Configured starting width, from
    /// <see cref="LatticeOptions.WalReplaySliceBudget"/> (issue #2898). Omitted
    /// by call sites that have no resolved options to hand, which fall back to
    /// <see cref="InitialBudget"/>. Values below one are clamped to one rather
    /// than rejected: the validator refuses them at configuration time, and a
    /// reader that threw here would turn a misconfiguration into an
    /// unreplayable partition at activation, which is a far worse failure than
    /// a narrow read.
    /// </param>
    internal ReplaySliceReader(
        ILeafReplayCoordinatorGrain coordinator,
        string treeId,
        int partition,
        int initialBudget = InitialBudget)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(treeId);

        _coordinator = coordinator;
        _treeId = treeId;
        _partition = partition;
        // Clamp rather than throw, and do not "tidy" this into agreement with
        // the validator. The two guards sit on the same value at seams where
        // failure costs differently, so their postures differ deliberately.
        // LatticeOptionsValidator rejects a width below one at CONFIGURATION
        // time, where failing is free and precise: nothing has activated, the
        // operator is handed the property name, and the process does not start.
        // This runs during ACTIVATION on the recovery path, where throwing
        // converts a misconfiguration into an unreplayable partition whose
        // frozen projection checkpoint holds the whole-tree WAL GC pin - the
        // self-reinforcing failure issues #2742 and #2867 exist to break. A
        // too-narrow read is recoverable; a dead partition pinning the log is
        // not. So: fail closed where failing is cheap, fail open into a correct
        // degraded mode where failing is catastrophic. Unreachable through
        // supported configuration, and covered by a test regardless, because an
        // unreachable guard with no test is indistinguishable from one that was
        // never correct.
        _initialBudget = initialBudget < 1 ? 1 : initialBudget;
        _budget = _initialBudget;

        LatticeMetrics.WalReplaySliceNarrowings.Add(
            0,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition),
            LatticeTenantLabel.ForTree(treeId));
    }

    /// <summary>
    /// The width the next read will ask for. Exposed for diagnostics and for
    /// the tests that assert the narrowing happened, never to be assigned by a
    /// call site - the whole defect this type exists to prevent was two call
    /// sites owning their own width.
    /// </summary>
    internal int Budget => _budget;

    /// <summary>
    /// Reads one slice of <c>(fromExclusive, toInclusive]</c>, narrowing the
    /// width and retrying the same range while the read fails for memory
    /// pressure, and widening back towards the configured starting width on
    /// success.
    /// <para>
    /// Retrying the same range is safe at every site because the throwing
    /// <c>await</c> has applied nothing - the caller's fold runs over the
    /// returned slice, which does not exist when the read throws - and because
    /// a narrower budget over an unchanged range returns a strict prefix of
    /// what the wider read would have returned. Ordering and completeness are
    /// therefore untouched; only the number of round trips changes.
    /// </para>
    /// <para>
    /// Note what the retry does <b>not</b> change: the range. Narrowing spends
    /// width, not span, so a retry re-presents the identical
    /// <c>(fromExclusive, toInclusive]</c> pair. The comment that arrived with
    /// issue #2742 claimed the opposite - that "the narrower range is a
    /// different key on the coordinator's slice cache" - and that was wrong on
    /// both halves: the range is not narrower, and the budget is not part of
    /// that key. The conclusion happened to hold anyway, for a different
    /// reason, which is that the coordinator caches only after a read succeeds,
    /// so a read that failed for pressure leaves no entry to be re-served. The
    /// gap the false reason hid was a <i>sibling's</i> entry: the cache exists
    /// so that several leaves replaying one shard share a read, so a leaf that
    /// had narrowed could be handed another leaf's full-width slice for the
    /// same range. That is repaired at the coordinator, which now truncates a
    /// cache hit to the caller's budget.
    /// </para>
    /// <para>
    /// The retry is bounded rather than merely convergent: each narrowing is an
    /// integer quarter floored at one, and the guard excludes a width of one, so
    /// a read can narrow at most four times from the default starting width
    /// before the failure is allowed to propagate. A single entry is the
    /// narrowest read there is, so at that point there is no cheaper attempt to
    /// make and the caller must decide what an unreplayable partition means for
    /// it - which differs by site, and is why this method reports the terminal
    /// failure by rethrowing rather than by handling it.
    /// </para>
    /// </summary>
    /// <param name="fromExclusive">Exclusive lower bound of the range to read.</param>
    /// <param name="toInclusive">Inclusive upper bound of the range to read.</param>
    /// <param name="onNarrowed">
    /// Invoked after each narrowing with the absorbed exception and the new
    /// width, so a site can log the event in its own terms. The narrowing has
    /// already been counted when this runs; a site that wants no log passes
    /// <see langword="null"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the read and the retry loop.</param>
    /// <returns>At most <see cref="Budget"/> entries, ascending by offset.</returns>
    internal async Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
        long fromExclusive,
        long toInclusive,
        Action<Exception, int>? onNarrowed,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IReadOnlyList<CommitLogSliceEntry> slice;
            try
            {
                slice = await _coordinator.ReadSliceAsync(
                    fromExclusive,
                    toInclusive,
                    _budget,
                    cancellationToken);
            }
            catch (Exception ex) when (_budget > 1 && BPlusLeafGrain.IsReadMemoryPressure(ex))
            {
                var narrowed = _budget / 4;
                _budget = narrowed < 1 ? 1 : narrowed;

                // Issue #2867. Counted before the site's log runs, because the
                // log is throttled by the sink's own configuration and this has
                // to be the exact census of a factor that is otherwise
                // invisible. Since issue #2898 the width is configurable via
                // LatticeOptions.WalReplaySliceBudget, so the census is no
                // longer the ONLY thing behind the width - but it remains the
                // only thing that reports the width actually in force, because
                // an option records what was asked for and this records what
                // pressure made of it.
                LatticeMetrics.WalReplaySliceNarrowings.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, _treeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, _partition),
                    LatticeTenantLabel.ForTree(_treeId));

                onNarrowed?.Invoke(ex, _budget);
                continue;
            }

            // Widen back on success. Without this a single pressure blip would
            // pin the replay at one entry per slice for the rest of the range,
            // which converges so slowly it is indistinguishable from the stall
            // being fixed. Doubling recovers full width in a handful of slices
            // while still backing off immediately if pressure returns.
            if (_budget < _initialBudget)
            {
                var widened = _budget * 2;
                _budget = widened > _initialBudget ? _initialBudget : widened;
            }

            return slice;
        }
    }
}
