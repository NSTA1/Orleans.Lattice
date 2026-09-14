using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice;

/// <summary>
/// Per-silo background service that drives the WAL garbage collector
/// (<see cref="ILatticeWalGc"/>) for every registered tree on a
/// backlog-responsive cadence, so a durable-WAL host gets bounded WAL
/// retention without any caller invoking
/// <see cref="ILatticeWalGc.RunOnceAsync"/> and without depending on the
/// replication package.
/// <para>
/// The core library ships the WAL GC but, before this scheduler, the
/// only production driver of <see cref="ILatticeWalGc.RunOnceAsync"/>
/// was the replication package's per-tree maintenance grain. That left
/// two retention gaps: a durable-WAL host without replication never
/// trimmed its WAL at all, and every <i>non-replicated</i> tree in a
/// replicated host was never collected. Both grew without bound and
/// made <see cref="LatticeOptions.WalRetention"/> inert. This scheduler
/// closes the gap by running a GC pass for every tree the registry
/// reports, replicated or not.
/// </para>
/// <para>
/// <b>Startup stagger.</b> The first pass is deliberately not run at
/// activation time: it is offset by a random delay in
/// <c>[<see cref="LatticeOptions.WalGcStartupDelay"/> / 2,
/// <see cref="LatticeOptions.WalGcStartupDelay"/>)</c> (30 seconds by
/// default, so 15 to 30 seconds) so the silo finishes activating before
/// the scheduler adds WAL scan/trim I/O, and so a rolling cluster restart
/// does not align every silo's full-tree fan-out into a correlated I/O
/// storm. The window is capped at <see cref="LatticeOptions.WalGcInterval"/>
/// so a host configured with a short cadence is never made to wait longer
/// than one interval. Before this knob existed the stagger was drawn from
/// <c>[interval / 2, interval)</c>, which at the default hourly cadence put
/// the first pass 30 to 60 minutes out - so a box recreated more often than
/// that never reclaimed a single WAL entry.
/// </para>
/// <para>
/// <b>Backlog-responsive cadence.</b> Each tree carries an independent
/// interval inside the closed band
/// <c>[<see cref="LatticeOptions.WalGcMinInterval"/>,
/// <see cref="LatticeOptions.WalGcInterval"/>]</c>. A pass that trims at
/// least one entry - the direct observation that the tree had backlog above
/// the trim floor - snaps that tree back to the floor so a fast-growing log
/// keeps being collected; a pass that trims nothing doubles the tree's
/// interval up to the configured ceiling, so an idle tree geometrically
/// relaxes and costs nothing. Because the state is per tree, a busy tree
/// never drags a quiet one into a tight loop and a quiet one never delays a
/// busy one. Setting <see cref="LatticeOptions.WalGcMinInterval"/> to zero
/// collapses the band to a single value and restores a fixed-interval tick.
/// </para>
/// <para>
/// This changes only <i>when</i> a pass runs. What a pass may reclaim is
/// unchanged: trim eligibility and the coverage-gated trim floor live in the
/// GC itself and are neither consulted nor relaxed from here.
/// </para>
/// <para>
/// Running on every silo is safe and composes with the replication
/// maintenance grain: <see cref="ILatticeWalGc.RunOnceAsync"/> and the
/// underlying <see cref="IWalStorageProvider.TrimAsync"/> are
/// idempotent, the GC scan is conservative (it stops at the first
/// non-eligible entry and never trims past the minimum consumer cursor
/// or the leaf-materialiser checkpoint floor), and a silo that cannot
/// resolve a partition's pinned provider skips it. A redundant pass
/// from a sibling silo therefore at worst issues a duplicate trim that
/// the provider collapses to a no-op.
/// </para>
/// <para>
/// Enablement is controlled by <see cref="LatticeOptions.WalGcInterval"/>,
/// a global knob read from the default (unnamed) options; set
/// <see cref="TimeSpan.Zero"/> (or any non-positive value) to disable the
/// scheduler and restore the historical caller-driven behaviour. All three
/// cadence knobs are read once at start.
/// </para>
/// </summary>
internal sealed class LatticeWalGcScheduler(
    IGrainFactory grainFactory,
    ILatticeWalGc gc,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeWalGcScheduler> logger,
    TimeProvider? timeProvider = null,
    BPlusTree.Grains.SnapshotPinCensus? snapshotPins = null,
    [FromKeyedServices(LatticeOptions.StorageProviderName)] IGrainStorage? leafStateStorage = null) : BackgroundService
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// Longest delay <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>
    /// accepts. A larger value throws <see cref="ArgumentOutOfRangeException"/>
    /// synchronously, so every delay this scheduler arms is clamped to it
    /// first. None of the three cadence knobs
    /// (<see cref="LatticeOptions.WalGcInterval"/>,
    /// <see cref="LatticeOptions.WalGcMinInterval"/>,
    /// <see cref="LatticeOptions.WalGcStartupDelay"/>) carries an upper bound or
    /// is validated, so a startup stagger, cadence wait, or quiet backoff longer
    /// than this (a configured interval above roughly 49 days) would otherwise
    /// throw out of <see cref="SafeDelayAsync"/> and, because this is a
    /// <see cref="BackgroundService"/> under the default
    /// <see cref="BackgroundServiceExceptionBehavior.StopHost"/>, take the whole
    /// silo host down. Clamping wakes the scheduler at least this often to
    /// re-evaluate, which is strictly more collection than the extreme interval
    /// asked for, never less. This mirrors the guard
    /// <see cref="LatticeStorageUsagePoller"/> already applies to its
    /// <see cref="System.Threading.PeriodicTimer"/> cadence.
    /// </summary>
    private static readonly TimeSpan MaxSchedulableDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// Per-tree cadence state, keyed by tree id. Bounded by the number of
    /// registered trees: an entry is seeded the first time a tree is seen and
    /// dropped once the registry stops reporting it, so a deleted tree cannot
    /// leak an entry for the life of the silo.
    /// <para>
    /// This and the two fields below are confined to the single
    /// <see cref="ExecuteAsync"/> loop - the only thing that ever runs a pass -
    /// so they need no synchronisation.
    /// </para>
    /// </summary>
    private readonly Dictionary<string, TreeCadence> _cadence = new(StringComparer.Ordinal);

    /// <summary>
    /// Wait applied when a pass observed no collectable tree at all - an empty
    /// registry, a registry that faulted, or a registry reporting only blank
    /// ids. Relaxes on each such pass exactly as a per-tree interval does, so a
    /// silo whose registry is briefly unavailable during startup retries soon
    /// while a permanently empty one settles at the configured ceiling.
    /// </summary>
    private TimeSpan _quietWait;

    /// <summary>
    /// Pass counter used to drop cadence state for trees the registry no longer
    /// reports. Pre-incremented, so a live generation is always 1 or greater and
    /// <c>0</c> is free to mark a never-observed entry.
    /// </summary>
    private int _generation;

    /// <summary>
    /// Trees whose zero-primed WAL-retention counter series have already been
    /// emitted. Confined to the <see cref="ExecuteAsync"/> loop like the fields
    /// above, and pruned alongside <see cref="_cadence"/> so a deleted tree
    /// cannot leak an entry for the life of the silo. Re-priming a tree that
    /// was retired and re-registered is harmless - the prime adds zero.
    /// </summary>
    private readonly HashSet<string> _primedTrees = new(StringComparer.Ordinal);

    /// <summary>
    /// Minimum time a tree must have been continuously blocked by the same
    /// consumer before the sweep will reactivate its leaf.
    /// </summary>
    /// <remarks>
    /// A blocked tree polls at the interval floor, so without this delay the
    /// very first blocked pass after a silo restart would reactivate leaves -
    /// exactly when a deployment is least able to absorb the load, and before
    /// the ordinary activation traffic that would have healed them for free has
    /// had any chance to. Blocked-ness that clears on its own inside this
    /// window costs nothing.
    /// </remarks>
    private static readonly TimeSpan ReactivationMinBlockAge = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Minimum interval between reactivation attempts for the same blocking
    /// consumer.
    /// </summary>
    /// <remarks>
    /// A touch that heals clears the block within a pass or two, so anything
    /// still blocked after this long did not heal and will not heal by being
    /// touched again immediately. Without the cooldown a blocked tree polling
    /// at the 30s floor would reactivate the same leaf 120 times an hour.
    /// </remarks>
    private static readonly TimeSpan ReactivationRetryCooldown = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Reactivation attempts permitted per blocking consumer before the sweep
    /// gives up on it and reports it as abandoned.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The budget exists because reactivation is <b>not</b> guaranteed to heal.
    /// A leaf whose snapshot capture cannot complete - the live case is an
    /// <c>OutOfMemoryException</c> serialising an oversized leaf - activates,
    /// fails, and deactivates still holding its blocking pin. The
    /// activation-time repair already retries such a leaf without backpressure,
    /// so an unbounded sweep would stack a second retry loop on top of a first,
    /// aimed at precisely the leaves least able to absorb it.
    /// </para>
    /// <para>
    /// Exhausting the budget stops the sweep <b>for a cycle</b>, not for the
    /// life of the process. See <see cref="ReactivationRearmBaseBackoff"/> for
    /// why a permanent stop was wrong and what replaced it.
    /// </para>
    /// </remarks>
    private const int MaxReactivationAttempts = 3;

    /// <summary>
    /// How many blocking leaves a single pass may touch on one tree.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A blast-radius bound, not a tuning knob. Before issue #2768 this bound
    /// was 1 and was structural rather than chosen: the floor reported a single
    /// blocking consumer, so a pass could not discover a second blocker to act
    /// on even in principle. That made the sweep's throughput one leaf per
    /// <see cref="ReactivationRetryCooldown"/> per tree no matter how many
    /// leaves were blocked, which cannot converge on a tree with thousands of
    /// them - measured as 2 attempts and 0 heals across 46 blocked passes.
    /// </para>
    /// <para>
    /// The per-leaf limits are unchanged; only the number of leaves a pass may
    /// apply them to at once has moved off 1. Touches are issued concurrently,
    /// so a pass costs the same wall-clock as it did when it made one, and the
    /// real work they start is bounded downstream by the leaf replay-admission
    /// gate rather than by this number. Trees are swept sequentially, so this is
    /// also the whole silo's concurrent touch ceiling and not a per-tree ceiling
    /// multiplied by the tree count.
    /// </para>
    /// </remarks>
    private const int MaxReactivationTouchesPerPass = 4;

    /// <summary>
    /// How long a consumer's budget survives after the floor stops reporting it
    /// as blocking, before it is pruned from the episode's map.
    /// </summary>
    /// <remarks>
    /// Only ever applied to a consumer with no live attempt state, so pruning
    /// can never destroy the evidence that a heal is not working - which is the
    /// defect issue #2772 fixed and this must not reintroduce. It exists solely
    /// to bound the map on a tree whose reported blockers rotate for a long
    /// time; a consumer that has been touched, abandoned or re-armed is retained
    /// for the life of the episode regardless.
    /// </remarks>
    private static readonly TimeSpan BlockedConsumerRetention = TimeSpan.FromHours(1);

    /// <summary>
    /// How many faulted touches a cycle may refund before faults start
    /// consuming the attempt budget like any other outcome.
    /// </summary>
    /// <remarks>
    /// Refunding a faulted touch is correct (see the budget comment in
    /// <see cref="ObserveAndHealBlockedTreeAsync"/>) because a fault measures
    /// the silo, not the leaf. Refunding <i>without limit</i> is not: a leaf
    /// that faults every time would then be touched once per
    /// <see cref="ReactivationRetryCooldown"/> for the life of the process,
    /// never reaching abandonment and so never entering the escalating backoff
    /// that exists to make a hopeless tree cheap. The cap keeps the worst case
    /// finite at <c>MaxReactivationRefunds + MaxReactivationAttempts</c> touches
    /// per cycle, after which a permanently-faulting tree decays on exactly the
    /// same schedule as a permanently-blocked one.
    /// </remarks>
    private const int MaxReactivationRefunds = 3;

    /// <summary>
    /// Delay from a consumer's abandonment before its attempt budget is
    /// restored and the sweep tries again.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Abandonment used to be permanent, which was a defect (issue #2783): the
    /// conditions that make a reactivation futile - memory pressure, an ingest
    /// burst saturating the replay gate, a silo mid-recovery - are <b>transient
    /// by nature</b>, while the budget they consume was not. A budget spent
    /// during a burst is spent against precisely the window in which no touch
    /// could have worked, and the sweep then stayed extinguished through the
    /// quiet period in which it would have succeeded. That is a system that
    /// cannot converge without an operator restarting the silo, which is the
    /// one remedy this subsystem is not allowed to require.
    /// </para>
    /// <para>
    /// The replacement is a bounded periodic retry, not an unbounded one: the
    /// budget is restored, never widened, so each cycle still costs at most
    /// <see cref="MaxReactivationAttempts"/> touches spaced by
    /// <see cref="ReactivationRetryCooldown"/>, and the interval between cycles
    /// doubles toward <see cref="ReactivationRearmMaxBackoff"/>. A genuinely
    /// unfixable tree therefore converges on a handful of touches every few
    /// hours rather than either stopping forever or looping hot.
    /// </para>
    /// <para>
    /// This is per consumer, which matters given issue #2772's per-consumer
    /// budget map: a tree working through several distinct blocking leaves
    /// re-arms each on its own schedule, so one hopeless leaf decaying to the
    /// ceiling never slows the retry of a sibling that has only just been
    /// abandoned.
    /// </para>
    /// </remarks>
    private static readonly TimeSpan ReactivationRearmBaseBackoff = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Ceiling on the re-arm backoff, so a consumer that can never be healed
    /// settles at a floor rate instead of doubling away to never retrying.
    /// </summary>
    /// <remarks>
    /// The doubling is what keeps a hopeless tree cheap; the ceiling is what
    /// keeps it <i>alive</i>. Without one, a silo up for a week would push the
    /// interval past any window in which a repair could plausibly be noticed,
    /// which is permanent abandonment again with extra steps.
    /// </remarks>
    private static readonly TimeSpan ReactivationRearmMaxBackoff = TimeSpan.FromHours(6);

    /// <summary>
    /// Shortest interval a re-arm may ever wait, including when evidence says
    /// the blocking condition has lifted.
    /// </summary>
    /// <remarks>
    /// A completed capture elsewhere in the process collapses the escalated
    /// backoff (see <see cref="ReactivationRearmBaseBackoff"/>) to this floor,
    /// because it is direct evidence that whatever made this consumer's touches
    /// futile is no longer in force. The floor is what stops that evidence
    /// becoming a hot loop: a process healing many leaves in quick succession
    /// can re-arm a stranded one at most once per floor interval, no matter how
    /// much evidence arrives.
    /// </remarks>
    private static readonly TimeSpan ReactivationRearmMinBackoff = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Count of blocked leaves this silo has seen stop blocking after the sweep
    /// touched them. Read only as a change detector: a consumer whose budget was
    /// abandoned while this counter held one value, observing a different value,
    /// has direct evidence that some leaf completed a capture since - so
    /// whatever made its own touches futile is no longer in force.
    /// </summary>
    /// <remarks>
    /// An instance field rather than a static one, so it cannot leak between
    /// schedulers in a host running several, nor between test fixtures.
    /// </remarks>
    private long _reactivationHealEpoch;

    /// <summary>
    /// How long a tree's cursor floor may stay continuously blocked with the
    /// sweep having done nothing about it before the block is reported as one
    /// the sweep cannot act on at all.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This exists because every other give-up path is gated on an attempt
    /// having been made, and an attempt is itself gated on the reported blocker
    /// holding still for <see cref="ReactivationMinBlockAge"/>. A tree whose
    /// reported blocker changes faster than that is never touched at all, so an
    /// attempt-derived budget can never be reached on it however monotonic the
    /// budget is made. That is the one shape of permanently-blocked tree
    /// <see cref="MaxReactivationAttempts"/> cannot reach, and it is the shape
    /// with the least excuse for going unreported.
    /// </para>
    /// <para>
    /// The interval is derived rather than chosen: it is exactly the wall-clock
    /// a single stable blocker needs to spend its whole budget - one
    /// <see cref="ReactivationMinBlockAge"/> to become eligible, then one
    /// <see cref="ReactivationRetryCooldown"/> per permitted attempt. Past it, a
    /// stable blocker has certainly been abandoned already, so a tree still
    /// blocked and still untouched is one the sweep has no purchase on. Being a
    /// function of the gating constants and not of the GC cadence, it stays
    /// correct on a host collecting at any interval and moves on its own if the
    /// gating is ever retuned.
    /// </para>
    /// </remarks>
    private static readonly TimeSpan UnreachableBlockEscalation =
        ReactivationMinBlockAge + (MaxReactivationAttempts * ReactivationRetryCooldown);

    /// <summary>
    /// Per-tree record of the consumer currently blocking its cursor floor and
    /// what the sweep has done about it. Bounded by the number of blocked trees
    /// and pruned alongside <see cref="_cadence"/>.
    /// </summary>
    private readonly Dictionary<string, BlockedConsumerObservation> _blockedConsumers =
        new(StringComparer.Ordinal);

    /// <summary>
    /// One tree's blocked-floor episode: the episode-wide alarm state plus the
    /// rate-limiter state and attempt budget for every consumer the episode has
    /// seen reported as blocking.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why the budgets are a map and not a counter.</b> The reported blocker
    /// is not stable, and the sweep is what destabilises it: the floor skips any
    /// consumer present in the live cursor registry before it evaluates the pin,
    /// and a touched leaf reports a cursor as soon as it activates, so touching
    /// the reported blocker is precisely what moves the report onto a different
    /// one. Keyed on the reported consumer, the budget was therefore reset by
    /// the sweep's own remedy and the give-up branch was unreachable rather than
    /// merely slow (issue #2772). Keyed on the blocking leaf, a blocker that
    /// rotates off the head and returns resumes its count.
    /// </para>
    /// <para>
    /// A single per-tree counter would fix the same reset and break a tree that
    /// is genuinely draining, because each newly revealed leaf would inherit its
    /// predecessors' spent budget instead of getting its own. The map is what
    /// separates "this leaf will not heal" from "this tree has several leaves to
    /// work through".
    /// </para>
    /// <para>
    /// <b>Bound.</b> An entry is created for every consumer the floor reports as
    /// blocking, which is bounded per pass by
    /// <see cref="LatticeWalGc.MaxReportedBlockingConsumers"/> but accumulates
    /// across an episode as blockers rotate, so the map is pruned: an entry that
    /// has not been reported for <see cref="BlockedConsumerRetention"/> and has
    /// no live attempt state is dropped. Pruning is deliberately conservative
    /// about attempt state, because destroying the evidence that a heal is not
    /// working is exactly the defect issue #2772 fixed. The whole map is dropped
    /// when the episode ends, and the record itself is pruned alongside
    /// <see cref="_cadence"/> when the tree is retired.
    /// </para>
    /// </remarks>
    /// <param name="EpisodeStarted">When this tree's floor was first seen blocked in this episode.</param>
    /// <param name="LastAnyAttempt">When the sweep last touched any consumer on this tree, if ever.</param>
    /// <param name="AnyAbandoned">Whether some consumer on this tree has spent its budget and been reported.</param>
    /// <param name="Escalated">Whether the unreachable-block escalation has already fired for this episode.</param>
    /// <param name="Budgets">Per-consumer attempt budgets, shared across the episode and mutated in place.</param>
    /// <param name="WarnedBlocked">
    /// Whether the cannot-reclaim warning has already fired for this episode. It
    /// is what throttles that warning per episode rather than per change of
    /// reported blocker (issue #2815).
    /// </param>
    /// <param name="DistinctBlockers">
    /// How many consumers this episode has admitted to <paramref name="Budgets"/>.
    /// <para>
    /// It is a count of <i>admissions</i>, not of distinct identities, and the
    /// two differ in one direction only: pruning can drop a consumer that has
    /// not been reported for <see cref="BlockedConsumerRetention"/> and has no
    /// live attempt state, so an identity that rotates out for over an hour and
    /// returns is counted twice. Keeping an exact distinct count would need a
    /// set that is never pruned, which is the unbounded growth pruning exists to
    /// prevent - so this is deliberately the bounded approximation. It is read
    /// as a churn width, where an over-count by re-admission is still evidence
    /// of churn, and never as a leaf inventory.
    /// </para>
    /// </param>
    /// <param name="FirstBlocker">The first consumer admitted in this episode, which is the one the warning named.</param>
    /// <param name="LatestBlocker">The most recently admitted consumer, which is the one holding the floor now.</param>
    private readonly record struct BlockedConsumerObservation(
        DateTimeOffset EpisodeStarted,
        DateTimeOffset? LastAnyAttempt,
        bool AnyAbandoned,
        bool Escalated,
        Dictionary<string, ConsumerReactivationBudget> Budgets,
        bool WarnedBlocked = false,
        int DistinctBlockers = 0,
        string? FirstBlocker = null,
        string? LatestBlocker = null);

    /// <summary>
    /// One blocking leaf's reactivation budget and rate-limiter state, carried
    /// across changes in which consumers the floor happens to report.
    /// </summary>
    /// <remarks>
    /// <see cref="FirstObserved"/> and <see cref="LastAttempt"/> live here, per
    /// consumer, and not on the tree's observation (issue #2768). Held per tree
    /// they turned three limits that are all reasoned about per blocking leaf -
    /// the minimum block age, the retry cooldown and the attempt budget - into a
    /// single per-tree rate limit of roughly one leaf per cooldown, which cannot
    /// converge on a tree with many blocked leaves however long it runs.
    /// </remarks>
    /// <param name="FirstObserved">When this consumer was first reported as blocking in this episode.</param>
    /// <param name="LastObserved">When this consumer was last reported as blocking. Drives pruning only.</param>
    /// <param name="LastAttempt">When the sweep last touched this consumer, if ever.</param>
    /// <param name="Attempts">How many reactivations the sweep has issued for this consumer.</param>
    /// <param name="Abandoned">Whether the budget has been spent and reported.</param>
    /// <param name="AbandonedAt">When the budget was last spent, if it ever has been.</param>
    /// <param name="Cycles">How many times the budget has been restored after a backoff.</param>
    /// <param name="Refunds">Faulted touches excused in the current cycle.</param>
    /// <param name="HealEpochAtAbandonment">
    /// The value of <see cref="_reactivationHealEpoch"/> when the budget was
    /// last spent. A later value is evidence that some leaf has healed since,
    /// which collapses this consumer's backoff to
    /// <see cref="ReactivationRearmMinBackoff"/>.
    /// </param>
    private readonly record struct ConsumerReactivationBudget(
        DateTimeOffset FirstObserved,
        DateTimeOffset LastObserved,
        DateTimeOffset? LastAttempt = null,
        int Attempts = 0,
        bool Abandoned = false,
        DateTimeOffset? AbandonedAt = null,
        int Cycles = 0,
        int Refunds = 0,
        long HealEpochAtAbandonment = 0,
        bool PinStateClassified = false);

    /// <summary>
    /// What a single reactivation touch established about the blocking leaf.
    /// </summary>
    /// <remarks>
    /// Internal rather than private so the exhaustive-arming fixture can
    /// enumerate its members by reflection (issue #2938). A gate that took the
    /// member list from anywhere other than the enum itself could not detect the
    /// case it exists for - a member added without an arm - because it would be
    /// checking a copy that the new member was also missing from. This is the
    /// ordinary test-visible level in this assembly, which already exposes its
    /// grains to the test project the same way.
    /// </remarks>
    [InstrumentedEnum(
        typeof(LatticeWalGcScheduler),
        "orleans.lattice.wal.gc.blocked_leaf_reactivations",
        LatticeMetrics.TagOutcome)]
    internal enum ReactivationOutcome
    {
        /// <summary>The leaf was resolved and touched without error.</summary>
        Completed,

        /// <summary>
        /// The consumer id did not resolve to a leaf, so nothing was touched.
        /// A permanent property of the id, so it still consumes budget.
        /// </summary>
        Unresolvable,

        /// <summary>
        /// The touch was attempted and threw. Evidence about the silo, not
        /// about the leaf, so it is refundable up to
        /// <see cref="MaxReactivationRefunds"/>.
        /// </summary>
        Faulted,

        /// <summary>
        /// The touch was issued and its probe call did not return before the
        /// cluster response timeout (issue #2768).
        /// </summary>
        /// <remarks>
        /// <para>
        /// Distinguished from <see cref="Faulted"/> because it says something
        /// materially different, and the difference is the one the sweep's own
        /// effectiveness is judged on. A fault means the call failed; a timeout
        /// means the call is very probably still running - and the expected
        /// cause is the blocking leaf's activation being stuck exactly as
        /// hypothesised, so it is the signature of the condition the sweep
        /// exists to clear rather than an anomaly.
        /// </para>
        /// <para>
        /// It is deliberately treated as refundable in the same way as
        /// <see cref="Faulted"/>: neither establishes that activation would fail
        /// to heal the leaf, which is the only thing abandonment is entitled to
        /// conclude. Only the reporting differs.
        /// </para>
        /// </remarks>
        Undelivered,
    }

    /// <summary>
    /// The single mapping from a terminal outcome to its metric arm.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One choke point rather than a tag chosen at each recording site, so
    /// "every outcome is counted" is a property of one switch instead of a claim
    /// about scattered call sites that has to be re-audited after every change.
    /// The unmapped arm throws rather than falling back to a catch-all: an
    /// outcome nobody has classified must not be silently folded into a
    /// neighbouring bucket, because that produces a plausible wrong number in
    /// exactly the place a reader trusts one. Failing loudly is the same
    /// reasoning that governs metric field ordering in this repository.
    /// </para>
    /// <para>
    /// This function is also what the exhaustive-arming fixture drives: it calls
    /// it for every declared member, so a member added without a case here fails
    /// the suite rather than reaching production as an uncounted outcome.
    /// </para>
    /// </remarks>
    internal static KeyValuePair<string, object?> ReactivationOutcomeTag(ReactivationOutcome outcome) =>
        outcome switch
        {
            ReactivationOutcome.Completed => LatticeMetrics.BlockedLeafReactivationCompleted,
            ReactivationOutcome.Unresolvable => LatticeMetrics.BlockedLeafReactivationUnresolvable,
            ReactivationOutcome.Faulted => LatticeMetrics.BlockedLeafReactivationFaulted,
            ReactivationOutcome.Undelivered => LatticeMetrics.BlockedLeafReactivationUndelivered,
            _ => throw new ArgumentOutOfRangeException(
                nameof(outcome),
                outcome,
                "Every reactivation outcome must have a metric arm; an unmapped one would report as a structural zero indistinguishable from a measured one (issue #2938)."),
        };

    /// <summary>
    /// Whether an outcome is refunded against the attempt budget.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Extracted from the recording loop so that recording and refunding stop
    /// sharing one piece of control flow (issue #2938). They were one branch,
    /// which meant adding an arm for an uncounted outcome could not be done
    /// without editing the code that decides budgets.
    /// </para>
    /// <para>
    /// The split is by what the touch established. A fault or a timeout is
    /// evidence about the silo - the call failed, or has not answered yet -
    /// and neither shows that activation would fail to heal the leaf, which is
    /// the only thing abandonment is entitled to conclude. A completed touch is
    /// a real measurement that says the leaf did not heal, and an unresolvable
    /// id is a permanent property of the id that retrying cannot change; both
    /// are therefore charged.
    /// </para>
    /// <para>
    /// These classes are load-bearing beyond their behaviour. A refunded outcome
    /// costs <see cref="MaxReactivationAttempts"/> +
    /// <see cref="MaxReactivationRefunds"/> touches per abandonment and an
    /// unrefunded one costs <see cref="MaxReactivationAttempts"/>, and that
    /// ratio was used to diagnose two production trees at a time when three of
    /// the four outcomes had no counter. Moving an outcome between classes
    /// silently invalidates that reading, so the classes are pinned by
    /// <c>LatticeWalGcSchedulerCadenceTests.ReactivationRefundClass</c>.
    /// </para>
    /// </remarks>
    internal static bool IsRefundableReactivationOutcome(ReactivationOutcome outcome) =>
        outcome is ReactivationOutcome.Faulted or ReactivationOutcome.Undelivered;

    /// <summary>
    /// Every declared terminal outcome, cached once.
    /// </summary>
    /// <remarks>
    /// Cached because the priming path walks it per tree per pass and
    /// <see cref="Enum.GetValues{TEnum}"/> allocates a fresh array on each call.
    /// Derived from the enum rather than written out, so it cannot fall behind
    /// the type it describes - which is the failure this whole issue is about.
    /// </remarks>
    private static readonly ReactivationOutcome[] AllReactivationOutcomes =
        Enum.GetValues<ReactivationOutcome>();

    /// <summary>
    /// Every <see cref="WalGcEnumerationOutcome"/>, cached for the priming walk.
    /// </summary>
    /// <remarks>
    /// Derived from the enum rather than written out, so an arm added to the
    /// taxonomy is primed by construction. A hand-maintained list is a second
    /// declaration of the same population, and issue #2938 records what happens
    /// when the two drift: the new arm ships unprimed and its zero reads as
    /// absence forever.
    /// </remarks>
    private static readonly WalGcEnumerationOutcome[] AllEnumerationOutcomes =
        Enum.GetValues<WalGcEnumerationOutcome>();

    /// <summary>
    /// Every <see cref="WalGcSchedulerTermination"/>, cached for the priming
    /// walk. Derived from the enum for the reason
    /// <see cref="AllEnumerationOutcomes"/> gives.
    /// </summary>
    private static readonly WalGcSchedulerTermination[] AllSchedulerTerminations =
        Enum.GetValues<WalGcSchedulerTermination>();

    /// <summary>
    /// The <see cref="LatticeMetrics.TagOutcome"/> tag every measurement of
    /// <see cref="LatticeMetrics.WalGcSchedulerEnumerations"/> carries for
    /// <paramref name="outcome"/>.
    /// <para>
    /// Total over the enum with no discard arm, so a member added without an arm
    /// fails the arming gate instead of silently joining another member's series.
    /// </para>
    /// </summary>
    /// <param name="outcome">The enumeration outcome to name.</param>
    /// <returns>The tag for it.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The outcome has no arm.</exception>
    internal static KeyValuePair<string, object?> EnumerationOutcomeTag(WalGcEnumerationOutcome outcome) => outcome switch
    {
        WalGcEnumerationOutcome.Succeeded => LatticeMetrics.WalGcEnumerationSucceeded,
        WalGcEnumerationOutcome.Faulted => LatticeMetrics.WalGcEnumerationFaulted,
        WalGcEnumerationOutcome.Cancelled => LatticeMetrics.WalGcEnumerationCancelled,
        WalGcEnumerationOutcome.Empty => LatticeMetrics.WalGcEnumerationEmpty,
        WalGcEnumerationOutcome.AllBlank => LatticeMetrics.WalGcEnumerationAllBlank,
        WalGcEnumerationOutcome.TimedOut => LatticeMetrics.WalGcEnumerationTimedOut,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unmapped WAL GC enumeration outcome."),
    };

    /// <summary>
    /// The <see cref="LatticeMetrics.TagReason"/> tag every measurement of
    /// <see cref="LatticeMetrics.WalGcSchedulerTerminations"/> carries for
    /// <paramref name="reason"/>. Total over the enum, for the reason
    /// <see cref="EnumerationOutcomeTag"/> gives.
    /// </summary>
    /// <param name="reason">The termination reason to name.</param>
    /// <returns>The tag for it.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The reason has no arm.</exception>
    internal static KeyValuePair<string, object?> TerminationTag(WalGcSchedulerTermination reason) => reason switch
    {
        WalGcSchedulerTermination.Disabled => LatticeMetrics.WalGcSchedulerStoppedDisabled,
        WalGcSchedulerTermination.Cancelled => LatticeMetrics.WalGcSchedulerStoppedCancelled,
        WalGcSchedulerTermination.Faulted => LatticeMetrics.WalGcSchedulerStoppedFaulted,
        _ => throw new ArgumentOutOfRangeException(nameof(reason), reason, "Unmapped WAL GC scheduler termination."),
    };

    /// <summary>
    /// How long one registry enumeration is allowed to take before the pass
    /// abandons it.
    /// <para>
    /// A <b>liveness</b> bound, not a performance one. The enumeration is a
    /// single grain call returning a list of tree ids; anything approaching this
    /// is already pathological, and the value of the bound is not that it is
    /// tight but that it is finite - an await with no bound at all is how one
    /// stalled call ends every tree's collection on the silo permanently, with no
    /// event anywhere to explain it.
    /// </para>
    /// </summary>
    private static readonly TimeSpan EnumerationBudget = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How long one tree's collection is allowed to take before the pass
    /// abandons it and moves to the next tree.
    /// <para>
    /// A liveness bound, for the reason <see cref="EnumerationBudget"/> gives,
    /// and deliberately loose: a collection legitimately does storage work whose
    /// duration scales with the tree, and the observed cadence on the silo that
    /// motivated this was tens of seconds per <i>pass</i> across nineteen trees.
    /// A single tree exceeding this has left that range by two orders of
    /// magnitude.
    /// </para>
    /// <para>
    /// Expiring is not fatal to the pass: the tree records a failed pass, relaxes
    /// on its own timeline, and its siblings keep their schedules - which is the
    /// behaviour a tree that throws already gets. What changes is that a tree
    /// which <i>hangs</i> now gets it too.
    /// </para>
    /// </summary>
    private static readonly TimeSpan TreeCollectBudget = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Bounds one await so that an operation which never returns cannot stall
    /// the scheduler loop permanently.
    /// <para>
    /// <b>The bound is per await, not per pass or per tree.</b> A tree whose
    /// collect makes two bounded awaits can therefore spend two budgets before
    /// the pass moves on. That is deliberate: each await is a separate place the
    /// loop can stop, and the property being bought is that none of them is
    /// unbounded, not that their sum is small.
    /// </para>
    /// <para>
    /// <b>The operation is abandoned, not cancelled.</b> It keeps its own
    /// cancellation token - the host's stopping token - so shutdown still
    /// propagates, but a hang is by definition an operation not responding to
    /// its token, so waiting on it to notice one would be waiting on the thing
    /// that is already wrong. An abandoned operation's later fault is observed
    /// here so it cannot resurface as an unobserved task exception attributed to
    /// nothing.
    /// </para>
    /// <para>
    /// <b>No timer is armed for an operation that is already complete.</b>
    /// <see cref="Task.WaitAsync(TimeSpan, TimeProvider, CancellationToken)"/>
    /// returns the task itself on that fast path. This is load-bearing rather
    /// than incidental: the cadence fixtures drive a virtual clock whose
    /// next-timer signal completes on <i>any</i> arming, so a budget timer armed
    /// unconditionally would release every test's synchronisation part-way
    /// through a pass and make the whole harness race.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The operation's result.</typeparam>
    /// <param name="operation">The operation to bound.</param>
    /// <param name="budget">How long it is allowed to take.</param>
    /// <param name="stoppingToken">Abandons the wait early on shutdown.</param>
    /// <returns>The bounded operation.</returns>
    /// <exception cref="TimeoutException">The budget expired.</exception>
    /// <exception cref="OperationCanceledException">The silo is shutting down.</exception>
    private Task<T> Bounded<T>(Task<T> operation, TimeSpan budget, CancellationToken stoppingToken)
    {
        ObserveIfAbandoned(operation);
        return operation.WaitAsync(budget, _time, stoppingToken);
    }

    /// <inheritdoc cref="Bounded{T}(Task{T}, TimeSpan, CancellationToken)" />
    private Task Bounded(Task operation, TimeSpan budget, CancellationToken stoppingToken)
    {
        ObserveIfAbandoned(operation);
        return operation.WaitAsync(budget, _time, stoppingToken);
    }

    /// <summary>
    /// Observes the fault of an operation this scheduler may stop awaiting, so
    /// that abandoning one cannot surface later as an unobserved task exception
    /// with no context attached to it.
    /// </summary>
    /// <param name="operation">The operation that may be abandoned.</param>
    private static void ObserveIfAbandoned(Task operation)
    {
        if (operation.IsCompleted)
        {
            return;
        }

        _ = operation.ContinueWith(
            static faulted => _ = faulted.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Tier-2 reachability priming, and deliberately the first statement of
        // the method: above the disabled-by-configuration return, above the
        // startup stagger, and above every await. Priming anywhere below one of
        // those would make "this silo has the instrument but never got that far"
        // byte-identical to "this build does not have the instrument", which is
        // the ambiguity the whole issue is about. The phase gauge needs no
        // priming of its own - an observable instrument publishes from its
        // declaration site - which is what makes its presence the build witness
        // for every counter here.
        PrimeSchedulerLiveness();
        WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.Starting, tree: null, _time);

        var options = optionsMonitor.Get(Options.DefaultName);
        var interval = options.WalGcInterval;
        if (interval <= TimeSpan.Zero)
        {
            // Explicitly disabled: the WAL is trimmed only by an
            // explicit RunOnceAsync caller (an admin trigger or the
            // replication maintenance grain for replicated trees).
            logger.LogDebug(
                "WAL GC scheduler disabled (WalGcInterval <= 0).");
            Terminate(WalGcSchedulerTermination.Disabled, WalGcSchedulerPhase.Disabled);
            return;
        }

        // A non-positive floor disables the adaptive cadence, and a floor above
        // the ceiling is meaningless; both collapse the band to the configured
        // interval, which reproduces the historical fixed-interval tick exactly.
        var minInterval = options.WalGcMinInterval;
        if (minInterval <= TimeSpan.Zero || minInterval > interval)
        {
            minInterval = interval;
        }

        // Never make a host wait longer for its first pass than its own
        // configured ceiling.
        var startupWindow = options.WalGcStartupDelay;
        if (startupWindow > interval)
        {
            startupWindow = interval;
        }

        _quietWait = minInterval;

        try
        {
            if (!await SafeDelayAsync(RandomStartupDelay(startupWindow), stoppingToken).ConfigureAwait(false))
            {
                Terminate(WalGcSchedulerTermination.Cancelled, WalGcSchedulerPhase.Stopped);
                return;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                var wait = await RunPassAsync(minInterval, interval, stoppingToken).ConfigureAwait(false);

                // Observed here rather than inside RunPassAsync, because this
                // site post-dominates every return from it - the cancelled
                // enumerate, the faulted enumerate, the mid-loop cancellations,
                // the no-collectable-tree relax, and the normal return. The
                // relaxing quiet wait is the only positive signature a loop that
                // is alive and failing every pass has, so it must be recorded on
                // exactly the passes that write nothing else.
                LatticeMetrics.WalGcSchedulerWait.Record(wait.TotalSeconds, LatticeTenantLabel.Platform);

                WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.Waiting, tree: null, _time);
                if (!await SafeDelayAsync(wait, stoppingToken).ConfigureAwait(false))
                {
                    Terminate(WalGcSchedulerTermination.Cancelled, WalGcSchedulerPhase.Stopped);
                    return;
                }
            }

            Terminate(WalGcSchedulerTermination.Cancelled, WalGcSchedulerPhase.Stopped);
        }
        catch (Exception)
        {
            // Recorded and rethrown, never swallowed. Swallowing would convert a
            // fault the host is configured to act on into precisely the silent,
            // permanent stop this issue exists to make visible.
            Terminate(WalGcSchedulerTermination.Faulted, WalGcSchedulerPhase.Stopped);
            throw;
        }
    }

    /// <summary>
    /// Records that the loop has ended, on both halves of the liveness set: the
    /// counter says why, and the phase census says the silo now has no sweep and
    /// for how long.
    /// </summary>
    /// <param name="reason">Why the loop stopped.</param>
    /// <param name="phase">The terminal phase to park the census in.</param>
    private void Terminate(WalGcSchedulerTermination reason, WalGcSchedulerPhase phase)
    {
        LatticeMetrics.WalGcSchedulerTerminations.Add(
            1,
            TerminationTag(reason),
            LatticeTenantLabel.Platform);
        WalGcSchedulerPhaseCensus.Enter(phase, tree: null, _time);
    }

    /// <summary>
    /// Zero-primes every silo-scoped liveness series this scheduler owns, so
    /// that a zero on any of them is a measurement rather than an unpublished
    /// series.
    /// <para>
    /// The taxonomies are primed by <b>walking their enums</b> rather than by
    /// listing their arms. A list is a second declaration of the same
    /// population, and issue #2938 is the record of what happens when one of the
    /// two moves: an arm added to the enum and not to the list ships unprimed,
    /// and its zero reads as absence forever after.
    /// </para>
    /// <para>
    /// <see cref="LatticeMetrics.WalGcSchedulerWait"/> and
    /// <see cref="LatticeMetrics.WalGcSchedulerPassDuration"/> are deliberately
    /// absent from this method. The empty state of a duration distribution is
    /// undefined rather than zero - a scheduler that has not waited has not
    /// waited zero seconds - so a primed observation would be a fabricated
    /// sample that drags every percentile toward it. Their liveness is anchored
    /// to <see cref="LatticeMetrics.WalGcSchedulerPassesStarted"/> instead, which
    /// is primed here, and the one-observation-per-started-pass relation is
    /// asserted by fixture for both.
    /// </para>
    /// </summary>
    private static void PrimeSchedulerLiveness()
    {
        LatticeMetrics.WalGcSchedulerPassesStarted.Add(0, LatticeTenantLabel.Platform);

        foreach (var outcome in AllEnumerationOutcomes)
        {
            LatticeMetrics.WalGcSchedulerEnumerations.Add(
                0,
                EnumerationOutcomeTag(outcome),
                LatticeTenantLabel.Platform);
        }

        foreach (var reason in AllSchedulerTerminations)
        {
            LatticeMetrics.WalGcSchedulerTerminations.Add(
                0,
                TerminationTag(reason),
                LatticeTenantLabel.Platform);
        }
    }

    /// <summary>
    /// Computes the randomized delay before the first GC pass: a uniform value
    /// in <c>[window / 2, window)</c>. The floor of half a window keeps the
    /// first pass out of the silo's activation storm, and the random component
    /// spreads the first pass across silos so a rolling restart does not align
    /// every silo's fan-out. A non-positive window means "no stagger": the first
    /// pass runs immediately.
    /// </summary>
    private static TimeSpan RandomStartupDelay(TimeSpan window)
    {
        if (window <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var half = window / 2;
        return half + (half * Random.Shared.NextDouble());
    }

    private async Task<bool> SafeDelayAsync(TimeSpan delay, CancellationToken stoppingToken)
    {
        // Clamp before delegating: Task.Delay throws ArgumentOutOfRangeException
        // synchronously for a delay above MaxSchedulableDelay, and that throw
        // would escape ExecuteAsync and stop the host. An out-of-range cadence
        // knob must degrade the scheduler, never fault the silo.
        if (delay > MaxSchedulableDelay)
        {
            delay = MaxSchedulableDelay;
        }

        try
        {
            await Task.Delay(delay, _time, stoppingToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// Runs one scheduling pass: collects every registered tree whose adaptive
    /// interval has elapsed, updates each collected tree's next due time from
    /// what its pass reclaimed, and returns how long to sleep before the next
    /// pass (the earliest due time across every registered tree).
    /// <para>
    /// Per-tree failures are swallowed and logged so one wedged tree never
    /// stalls the cadence for the rest - a throwing tree relaxes on its own
    /// timeline while its siblings keep their own schedules. The registry
    /// enumeration is likewise guarded so a transient registry fault is retried
    /// rather than killing the scheduler.
    /// </para>
    /// </summary>
    private async Task<TimeSpan> RunPassAsync(TimeSpan minInterval, TimeSpan interval, CancellationToken stoppingToken)
    {
        // The heartbeat, and its placement is the whole point of it: above the
        // try, above the registry call, above anything that can fail. Every
        // other wal_gc series is written per tree after this enumeration
        // succeeds, so a pass that dies at the first await writes nothing else
        // at all - which is what makes a loop that is alive and failing
        // indistinguishable from a loop that has returned. This counter is the
        // one observable that separates them.
        LatticeMetrics.WalGcSchedulerPassesStarted.Add(1, LatticeTenantLabel.Platform);

        // Paired with the heartbeat above and recorded in a finally below, so
        // that exactly one observation exists per started pass however the pass
        // ends. That pairing is the anchor a fixture asserts, and it is what
        // keeps a duration histogram from going vacuous: "no new observations"
        // cannot be mistaken for "no passes" while a counter next to it is
        // independently visible and flat.
        //
        // Read off GetUtcNow rather than GetElapsedTime because a TimeProvider
        // is only obliged to virtualise the former, and a duration that silently
        // fell back to wall clock under a virtual clock would be untestable at
        // exactly the timescales worth testing.
        var startedAt = _time.GetUtcNow();

        try
        {
            return await RunPassCoreAsync(minInterval, interval, stoppingToken).ConfigureAwait(false);
        }
        finally
        {
            // A faulted or abandoned pass is the population whose duration
            // matters most: a pass that dies at a response timeout has a
            // duration which is itself the diagnosis, and the selected wait
            // cannot show it. Recording only on the success path would discard
            // precisely the interesting cases, so this sits in a finally.
            // The clamp is hoisted into a local deliberately. A relational '<'
            // inside the first argument of an emission call defeats the
            // repository's metric-emission scanner, whose argument splitter
            // treats '<' as a generic-argument open and so never finds the
            // comma: the tenant dimension it is checking for becomes invisible
            // and the site is reported as missing one. Math.Max says the same
            // thing with no angle bracket.
            var elapsed = _time.GetUtcNow() - startedAt;
            var seconds = Math.Max(0d, elapsed.TotalSeconds);
            LatticeMetrics.WalGcSchedulerPassDuration.Record(seconds, LatticeTenantLabel.Platform);
        }
    }

    /// <summary>
    /// The body of one pass. Split from <see cref="RunPassAsync"/> so that the
    /// heartbeat counter and the duration histogram bracket every exit from it,
    /// including the three that return early on a failed enumeration.
    /// </summary>
    /// <param name="minInterval">The adaptive floor.</param>
    /// <param name="interval">The adaptive ceiling.</param>
    /// <param name="stoppingToken">Cancelled when the silo is shutting down.</param>
    /// <returns>How long to sleep before the next pass.</returns>
    private async Task<TimeSpan> RunPassCoreAsync(
        TimeSpan minInterval,
        TimeSpan interval,
        CancellationToken stoppingToken)
    {
        WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.Enumerating, tree: null, _time);

        IReadOnlyList<string> treeIds;

        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            treeIds = await Bounded(registry.GetAllTreeIdsAsync(), EnumerationBudget, stoppingToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // An orderly silo shutdown. Recorded rather than returned silently:
            // before this arm existed, this was the quietest of the three exits
            // from this method, emitting neither a log line nor a metric, so a
            // shutdown mid-enumeration left no trace of any kind.
            //
            // Guarded on the stopping token rather than on the exception type
            // alone, so that a bound firing can never be absorbed as a shutdown.
            RecordEnumeration(WalGcEnumerationOutcome.Cancelled);
            return minInterval;
        }
        catch (TimeoutException)
        {
            // Our own bound fired. Kept apart from the faulted arm below because
            // a fault is a property of the registry and a timeout is a property
            // of the bound we chose, and folding the second into the first would
            // let a decision of ours present as a finding about the system.
            RecordEnumeration(WalGcEnumerationOutcome.TimedOut);
            logger.LogWarning(
                "WAL GC scheduler abandoned the registry enumeration after {Budget}; will retry on the next tick.",
                EnumerationBudget);
            return Quiet(minInterval, interval);
        }
        catch (Exception ex)
        {
            // A transient fan-out failure (silo restart, registry not
            // yet ready during startup) must not kill the scheduler; the
            // next tick retries the whole pass.
            //
            // Raised from LogDebug to a warning: this is candidate (A) of issue
            // #3060, and at debug level the one line that names the cause was
            // absent from every deployed log stream, leaving a silo whose sweep
            // had stopped with no evidence anywhere at any level.
            RecordEnumeration(WalGcEnumerationOutcome.Faulted);
            logger.LogWarning(
                ex,
                "WAL GC scheduler failed to enumerate trees; will retry on the next tick.");
            return Quiet(minInterval, interval);
        }

        RecordEnumeration(ClassifyEnumeration(treeIds));

        var generation = ++_generation;
        var nowTicks = _time.GetUtcNow().UtcTicks;
        var earliestDueTicks = long.MaxValue;
        var tracked = 0;

        // Indexed rather than foreach: enumerating an IReadOnlyList<string>
        // through its interface boxes the underlying struct enumerator, and this
        // loop runs on every pass for every registered tree.
        for (var i = 0; i < treeIds.Count; i++)
        {
            var treeId = treeIds[i];
            if (stoppingToken.IsCancellationRequested)
            {
                return minInterval;
            }
            if (string.IsNullOrEmpty(treeId))
            {
                continue;
            }

            if (!_cadence.TryGetValue(treeId, out var cadence))
            {
                // First sighting: due immediately and seeded at the responsive
                // floor, so a freshly registered tree is collected on this pass
                // rather than waiting out an interval it was never scheduled in.
                // Generation 0 is the never-observed sentinel; a real generation
                // always starts at 1.
                cadence = new TreeCadence(minInterval.Ticks, nowTicks, 0);
            }

            // Counted per distinct entry, so a registry that reports an id twice
            // cannot inflate the count and suppress pruning.
            if (cadence.Generation != generation)
            {
                tracked++;
            }

            if (cadence.NextDueTicks > nowTicks)
            {
                _cadence[treeId] = cadence with { Generation = generation };
                if (cadence.NextDueTicks < earliestDueTicks)
                {
                    earliestDueTicks = cadence.NextDueTicks;
                }
                continue;
            }

            var next = await CollectTreeAsync(
                treeId,
                TimeSpan.FromTicks(cadence.IntervalTicks),
                minInterval,
                interval,
                stoppingToken).ConfigureAwait(false);

            var dueTicks = SaturatingDueTicks(_time.GetUtcNow().UtcTicks, next.Ticks);
            _cadence[treeId] = new TreeCadence(next.Ticks, dueTicks, generation);
            if (dueTicks < earliestDueTicks)
            {
                earliestDueTicks = dueTicks;
            }
        }

        WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.Pruning, tree: null, _time);
        PruneRetiredTrees(generation, tracked);

        if (earliestDueTicks == long.MaxValue)
        {
            // No collectable tree is registered yet. Relax on the same schedule
            // a quiet tree would, so an empty silo costs nothing while a silo
            // whose first tree is about to register still picks it up promptly.
            return Quiet(minInterval, interval);
        }

        _quietWait = minInterval;
        var wait = earliestDueTicks - _time.GetUtcNow().UtcTicks;
        if (wait <= 0)
        {
            return TimeSpan.Zero;
        }

        return TimeSpan.FromTicks(wait > interval.Ticks ? interval.Ticks : wait);
    }

    /// <summary>
    /// Records one enumeration outcome on
    /// <see cref="LatticeMetrics.WalGcSchedulerEnumerations"/>.
    /// <para>
    /// A single emission site, so that the priming walk and the real recording
    /// cannot drift into carrying different tag sets - the failure that makes a
    /// primed series and a measured series fail to join.
    /// </para>
    /// </summary>
    /// <param name="outcome">What the enumeration produced.</param>
    private static void RecordEnumeration(WalGcEnumerationOutcome outcome) =>
        LatticeMetrics.WalGcSchedulerEnumerations.Add(
            1,
            EnumerationOutcomeTag(outcome),
            LatticeTenantLabel.Platform);

    /// <summary>
    /// Classifies a registry answer that did not throw.
    /// <para>
    /// The scheduler's quiet wait documents three conditions - an empty
    /// registry, a faulted registry, and a registry reporting only blank ids -
    /// that until now shared one silent path and one observable. This separates
    /// the two that are not faults, because a registry answering with ids that
    /// are all blank reports success, returns content, and collects nothing: on
    /// every other series it is indistinguishable from an idle silo, which is
    /// exactly why it needs an arm of its own.
    /// </para>
    /// </summary>
    /// <param name="treeIds">The ids the registry returned.</param>
    /// <returns>The arm to record.</returns>
    private static WalGcEnumerationOutcome ClassifyEnumeration(IReadOnlyList<string> treeIds)
    {
        if (treeIds.Count == 0)
        {
            return WalGcEnumerationOutcome.Empty;
        }

        // Indexed rather than foreach, for the reason the collection loop gives:
        // enumerating an IReadOnlyList<string> through its interface boxes the
        // underlying struct enumerator, and this runs on every pass.
        for (var i = 0; i < treeIds.Count; i++)
        {
            if (!string.IsNullOrEmpty(treeIds[i]))
            {
                return WalGcEnumerationOutcome.Succeeded;
            }
        }

        return WalGcEnumerationOutcome.AllBlank;
    }

    /// <summary>
    /// Runs one GC pass for a single tree, publishes its metering, and returns
    /// the interval to wait before collecting that tree again: the floor when
    /// the pass reclaimed entries (backlog was present above the trim floor), a
    /// relaxed interval otherwise.
    /// </summary>
    private async Task<TimeSpan> CollectTreeAsync(
        string treeId,
        TimeSpan currentInterval,
        TimeSpan minInterval,
        TimeSpan interval,
        CancellationToken stoppingToken)
    {
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var tenantTag = LatticeTenantLabel.ForTree(treeId);

        WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.CollectingPriming, treeId, _time);

        // Zero-prime the WAL-retention series a reader has to interpret an
        // absence on. A Counter publishes no series until its first Add, so a
        // tree that has never shed a pin report, never held a snapshot pin, and
        // never reclaimed is silent on all three - a shape indistinguishable
        // from a dead subsystem or a broken instrument (issues #2694, #2774).
        // Priming here, once per tree, turns that silence into an exported
        // zero, which is a measurement. The WAL GC scheduler is the right
        // primer because it enumerates exactly the trees whose retention is
        // being collected, which is the population the question is asked about.
        // Adding zero cannot perturb any of the values.
        //
        // The call site is load-bearing and must stay the first statement of
        // the pass: above the snapshot reconcile, above the try, and above
        // every early return. That is what makes "no series at all for a tree"
        // mean precisely "the scheduler never evaluated this tree", rather than
        // leaving it ambiguous with "evaluated and returned early".
        PrimeRetentionSeries(treeId, treeTag, tenantTag);

        // Re-derive the tree's live snapshot-pin set from the cursor registry.
        // The gauge's membership is asserted at the two sites that mutate a
        // registry entry, but an assertion can be lost - an activation torn
        // between the unregister and the mark, a registry shared across silos,
        // a future consumer that releases a snapshot pin without going through
        // the cursor grain. Re-deriving here makes the series self-healing: the
        // registry is the authority on which pins actually hold the trim floor
        // down, and this is the pass that evaluates that floor. Best-effort, and
        // deliberately outside the try below: a metering read must not be able
        // to mark a GC pass failed.
        if (snapshotPins is not null)
        {
            WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.CollectingReconciling, treeId, _time);
            try
            {
                await Bounded(snapshotPins.ReconcileAsync(treeId, stoppingToken), TreeCollectBudget, stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return currentInterval;
            }
            catch (Exception ex)
            {
                logger.LogDebug(
                    ex,
                    "Snapshot pin census reconcile failed for tree {Tree}; the gauge keeps its last derived value.",
                    treeId);
            }
        }

        TimeSpan next;
        try
        {
            WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.CollectingGcRun, treeId, _time);
            var report = await Bounded(gc.RunOnceAsync(treeId, stoppingToken), TreeCollectBudget, stoppingToken)
                .ConfigureAwait(false);

            // EntriesTrimmed is the count the pass found eligible under the GC's
            // own predicate, so a positive value is a direct observation of
            // backlog above the trim floor. Reading it here neither widens nor
            // narrows that predicate.
            var reclaimed = report.EntriesTrimmed > 0;

            // A pass that reclaimed nothing did so for one of several distinct
            // reasons, and until now all but one were labelled "idle". The tree
            // may have been quiet - nothing above the trim floor, which is the
            // healthy steady state - or the cursor branch may have been disabled
            // outright by an unusable durable materialiser pin, in which case the
            // tree cannot reclaim at all and its WAL is growing without bound
            // (issue #2702), or no consumer may have reported a cursor at all, in
            // which case the pass evaluated nothing and says nothing about the
            // tree's backlog (issue #2850).
            var blocked = !reclaimed
                && report.CursorFloorState == WalGcCursorFloorState.BlockedByUnusablePin;

            RecordPass(
                1,
                reclaimed ? LatticeMetrics.OutcomeReclaimed : ClassifyUnreclaimed(report.CursorFloorState),
                treeTag,
                tenantTag);

            // Backlog metering. Byte accounting is a provider capability, so this
            // is an explicit two-branch decision rather than a silent skip; see
            // PublishBacklogBytes for the contract a consumer relies on.
            PublishBacklogBytes(report.RetainedBytesAfter, report.ByteCeiling, treeTag, tenantTag);

            // Self-healing remedy for the blocked condition, not just a label
            // for it (issue #2710 Limitation 2). A blocked tree stays blocked
            // until the offending leaf activates and replays, and nothing on
            // the leaf can drive that: the floor skips every consumer present
            // in the live cursor registry before it evaluates the pin, so a
            // blocking consumer is by construction one whose leaf is NOT
            // activated. There is no activation for a leaf-local trigger to run
            // in, which is why the retention path has to be the one to act.
            // End (and drive) the blocked episode on the floor state, not on the
            // pass outcome. `blocked` above folds in `reclaimed`, and the TTL
            // branch of the trim predicate is independent of the cursor branch -
            // a tree whose floor is blocked still trims entries older than a
            // configured WalRetention. One such pass made `blocked` false, which
            // erased the attempt budget and the abandoned flag while the floor
            // was still blocked, and credited a heal to a tree that had never
            // unblocked (issue #2772). Reading the floor state directly says
            // what the block is doing rather than what the pass happened to
            // reclaim, so an incidental age-based trim neither resets the budget
            // nor suspends the remedy.
            //
            // `blocked` still drives the cadence floor, and the pass outcome
            // label is derived from the same floor state through
            // ClassifyUnreclaimed; only the episode reads the state directly.
            var floorBlocked = report.CursorFloorState == WalGcCursorFloorState.BlockedByUnusablePin;

            if (floorBlocked)
            {
                // Blocked but naming no consumer keeps the episode rather than
                // ending it. The GC always names the blocker it short-circuited
                // on, so this is unreachable from a real report; if it were
                // reached, a report still saying the floor is blocked is not
                // evidence that the block cleared.
                if (report.BlockingConsumerId is { Length: > 0 } blockingConsumerId)
                {
                    // Prefer the bounded set the floor now carries, and fall
                    // back to the single id when a report was built without one
                    // (issue #2768). The fallback is not dead code: the report
                    // is a public record whose BlockingConsumerIds is optional,
                    // so a caller-constructed report can still name exactly one.
                    var blockingConsumerIds = report.BlockingConsumerIds is { Count: > 0 } reported
                        ? reported
                        : [blockingConsumerId];

                    WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.CollectingHealing, treeId, _time);
                    await ObserveAndHealBlockedTreeAsync(
                        treeId, blockingConsumerIds, treeTag, tenantTag, stoppingToken).ConfigureAwait(false);
                }
            }
            else
            {
                WalGcSchedulerPhaseCensus.Enter(WalGcSchedulerPhase.CollectingHealing, treeId, _time);
                ClearBlockedObservation(treeId, treeTag, tenantTag);
            }

            // Hold a blocked tree at the floor instead of relaxing it. The same
            // boolean used to drive both the label and the backoff, so a starved
            // tree was backed off exactly like a quiet one and drifted toward the
            // ceiling - fewest passes precisely when it needed the most. The
            // backoff was therefore self-reinforcing: being unable to reclaim was
            // itself the evidence used to decide to look less often.
            //
            // This matters beyond presentation because a repair is only half the
            // story. Whatever unblocks the pin, the stranded bytes do not come
            // back until a GC pass runs and trims them, so this interval is the
            // time-to-reclaim even when it is not the time-to-unblock. At stock
            // defaults that is 30s here against up to 1h before, and against the
            // ~2h ceiling a tuned deployment can reach.
            //
            // The floor introduces no new load level: a reclaiming tree already
            // runs at minInterval indefinitely, so this is a cadence the system
            // sustains by construction. It is self-limiting - a healed tree stops
            // being blocked and either reclaims (floor, legitimately) or goes
            // idle (relaxes as before) - and genuinely quiet trees are untouched.
            // The residual is deliberate: a tree blocked and unable to heal polls
            // at the floor indefinitely. That is the alarm state, and its cost is
            // bounded by the floor while the damage it signals is not.
            next = reclaimed || blocked ? minInterval : Relax(currentInterval, minInterval, interval);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown, not a tree fault: leave the cadence where it was
            // and do not record a failed pass.
            return currentInterval;
        }
        catch (TimeoutException)
        {
            // Our own bound fired. Treated exactly as a throwing tree already
            // is - a failed pass, a relaxed cadence, and siblings untouched -
            // because the remedy is the same; what changes is that a tree which
            // hangs now gets that treatment instead of stalling the silo.
            //
            // Logged at warning rather than debug: unlike a throwing tree, this
            // one produced no exception of its own to explain it, so this line
            // is the only account of why the tree was abandoned.
            RecordPass(1, LatticeMetrics.OutcomeFailed, treeTag, tenantTag);
            logger.LogWarning(
                "WAL GC pass for tree {Tree} exceeded its {Budget} budget and was abandoned; will retry on the next tick.",
                treeId,
                TreeCollectBudget);

            next = Relax(currentInterval, minInterval, interval);
        }
        catch (Exception ex)
        {
            RecordPass(1, LatticeMetrics.OutcomeFailed, treeTag, tenantTag);
            logger.LogDebug(
                ex,
                "WAL GC pass failed for tree {Tree}; will retry on the next tick.",
                treeId);

            // A wedged tree relaxes on its own timeline rather than retrying at
            // the floor forever, and its siblings keep their own schedules.
            next = Relax(currentInterval, minInterval, interval);
        }

        LatticeMetrics.WalGcInterval.Record(next.TotalSeconds, treeTag, tenantTag);
        return next;
    }

    /// <summary>
    /// Publishes the post-pass retained-byte backlog for a tree, when the pass
    /// measured one, and otherwise publishes the positive "not measured" signal
    /// naming why.
    /// <para>
    /// Byte accounting is a capability of the configured
    /// <see cref="IWalStorageProvider"/> gated behind the byte-pressure policy
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>), so
    /// <see cref="LatticeWalGcReport.RetainedBytesAfter"/> is
    /// <see langword="null"/> on a host that has either turned off.
    /// </para>
    /// <para>
    /// That branch used to record nothing at all, leaving the absence knowable
    /// only by <i>inferring</i> it from <see cref="LatticeMetrics.WalGcBacklogBytes"/>
    /// having no series while <see cref="LatticeMetrics.WalGcPasses"/> did. An
    /// inference from silence is exactly the reasoning step that produced a
    /// wrong, retracted root-cause diagnosis on this repository (issue #2692),
    /// so the branch now states itself through
    /// <see cref="LatticeMetrics.WalGcBacklogBytesUnavailable"/>, tagged with the
    /// reason a reader would act on: <c>policy_disabled</c> when no ceiling is
    /// configured (set <see cref="LatticeOptions.WalMaxRetainedBytes"/>), or
    /// <c>provider_unsupported</c> when a ceiling <i>is</i> configured and the
    /// provider still reported no retained byte size (change provider).
    /// Reclaimed volume in that configuration remains observable in records
    /// through <see cref="LatticeMetrics.WalEntriesTrimmed"/> and the
    /// <see cref="LatticeMetrics.OutcomeReclaimed"/> pass outcome.
    /// </para>
    /// </summary>
    private static void PublishBacklogBytes(
        long? retainedBytesAfter,
        long? byteCeiling,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (retainedBytesAfter is not { } backlogBytes)
        {
            LatticeMetrics.WalGcBacklogBytesUnavailable.Add(
                1,
                treeTag,
                byteCeiling is null
                    ? LatticeMetrics.ReasonBytePolicyDisabled
                    : LatticeMetrics.ReasonByteProviderUnsupported,
                tenantTag);
            return;
        }

        LatticeMetrics.WalGcBacklogBytes.Record(backlogBytes, treeTag, tenantTag);
    }

    /// <summary>
    /// Emits a one-time zero observation for each WAL-retention series that a
    /// reader must be able to distinguish "measured, never happened" from "not
    /// reporting" on, so the series exists before its first real event.
    /// Idempotent per tree: the primed set is consulted on every pass and the
    /// instruments are touched only on the first.
    /// </summary>
    private void PrimeRetentionSeries(
        string treeId,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (!_primedTrees.Add(treeId))
        {
            return;
        }

        LatticeMetrics.MaterialiserPinReportsShed.Add(0, treeTag, tenantTag);

        // The snapshot-pin series is an observable gauge derived from the
        // cursor registry (issue #2700), so it is primed by registering the
        // tree rather than by adding zero: the callback then emits an explicit
        // 0 for a tree holding no pin, instead of no series at all.
        snapshotPins?.Track(treeId);

        // Every outcome arm of the pass counter is primed, not just one (issue
        // #2774). Two distinct reasons converge on the same remedy.
        //
        // The blocked arm is primed so it survives a return to zero. A repair
        // that unblocks a tree makes the counter stop advancing, and an
        // unprimed counter that never fired on this silo exports nothing at
        // all - so "healthy, never blocked" and "not reporting" would be
        // identical at exactly the moment a reader needs to tell them apart,
        // which is when confirming a fix held.
        //
        // The remaining arms are primed so an absent arm cannot be read as a
        // verdict. The reclaimed arm is the acute case, because it backs an
        // acceptance criterion: unprimed, "reclamation never happened" and
        // "the instrument is unwired" are the same reading, so a system that
        // reclaimed perfectly is indistinguishable from one that never ran - a
        // success misread as a failure, which is the most expensive wrong
        // answer a predicate can give.
        //
        // There is a second, subtler property this replaces. The non-failed
        // arms are selected by a single classifier feeding one Add call, so
        // today the presence of any one of them proves the site executed
        // for this tree - which is why a scrape carrying only blocked and idle
        // was still readable as evidence. That inference is incidental to how
        // the expression happens to be written: splitting the classifier into
        // separate calls would destroy it silently, with no test failing.
        // Priming each arm makes the guarantee structural, so a reader no longer
        // has to know the shape of the emission to interpret an absence.
        //
        // Priming per tree also keeps a single re-stranded tree visible rather
        // than averaged away across the fleet.
        RecordPass(0, LatticeMetrics.OutcomeReclaimed, treeTag, tenantTag);
        RecordPass(0, LatticeMetrics.OutcomeIdle, treeTag, tenantTag);
        RecordPass(0, LatticeMetrics.OutcomeBlocked, treeTag, tenantTag);
        RecordPass(0, LatticeMetrics.OutcomeFailed, treeTag, tenantTag);

        // The two arms split out of the former catch-all are primed on exactly
        // the same terms (issue #2850). `no_consumer` needs it for the reason
        // above - unprimed, "no consumer has ever reported" would be
        // indistinguishable from "this silo is not reporting", which is the
        // ambiguity the arm was created to remove, so leaving it unprimed would
        // reproduce the defect one level down.
        //
        // `unclassified` needs it for a different reason, and needs it more.
        // No pass can reach that arm against today's enum, so it will never
        // emit on its own: unprimed it would have no series at all, and a
        // reader asking "did any pass land in a state this build cannot name?"
        // would get silence - the one answer that is equally consistent with
        // "no" and with "the classifier is not running here". Primed, the arm
        // reads a measured zero for as long as the partition stays total, which
        // is precisely the assertion it exists to make.
        RecordPass(0, LatticeMetrics.OutcomeNoConsumer, treeTag, tenantTag);
        RecordPass(0, LatticeMetrics.OutcomeUnclassified, treeTag, tenantTag);

        // Zero-prime every blocked-leaf reactivation outcome (issue #2783).
        // Absence on this instrument has already been read as evidence twice on
        // this epic - once as "the sweep healed nothing" and once as "the sweep
        // never abandoned" - and neither reading was available, because a
        // Counter exports nothing at all until its first Add. An absent series
        // was equally consistent with the build not having landed.
        //
        // This is the reachability proof, and the site is what makes it one:
        // PrimeRetentionSeries is called at the top of CollectTreeAsync, above
        // every early return and before the collection itself, so a minted zero
        // says a GC pass ran and evaluated this tree. An absent series therefore
        // means the scheduler is not running here - a positive statement - and a
        // flat zero on 'abandoned' means measured-and-never-abandoned rather
        // than silence. 'rearmed' matters most of all: it is the series that
        // distinguishes a build carrying the re-arm from one that predates it,
        // which is precisely the question a reader will ask of a stranded tree.
        RecordBlockedLeafReactivation(LatticeMetrics.BlockedLeafReactivationAttempted, treeTag, tenantTag, 0);
        RecordBlockedLeafReactivation(LatticeMetrics.BlockedLeafReactivationHealed, treeTag, tenantTag, 0);
        RecordBlockedLeafReactivation(LatticeMetrics.BlockedLeafReactivationAbandoned, treeTag, tenantTag, 0);
        RecordBlockedLeafReactivation(LatticeMetrics.BlockedLeafReactivationRearmed, treeTag, tenantTag, 0);

        // Zero-prime the grain-side starvation-drive abandonment counter here,
        // beside 'attempted', rather than at the drive that records it (issue
        // #3065).
        //
        // This looks like the wrong file for it - the counter is emitted by
        // BPlusLeafGrain, not by the scheduler - and the reason it is here is
        // the whole value of the priming. Minted at the drive, the series would
        // exist only once a drive had been entered, so an absent series would be
        // equally consistent with "no drive has run on this silo" and with "this
        // build is not deployed". This epic has lost more time to that second
        // reading than to any other single cause: a zero-primed counter present
        // in source and absent from the running container made every downstream
        // reading uninterpretable, and there was no way to tell from the outside.
        //
        // Primed beside an instrument that is known to fire, absence becomes a
        // positive statement. 'attempted' > 0 with this series present and flat
        // reads as measured-and-never-abandoned; 'attempted' > 0 with this series
        // absent reads as this build not being deployed. The deployment proof is
        // free and it is the reason not to move this.
        //
        // Tagged tree-and-tenant, exactly as the leaf grain tags it at the
        // recording site (it derives the same LatticeTenantLabel.ForTree from the
        // same tree id). A prime whose tag set differs from the emitter's mints a
        // series shape the emitter can never match, which is worse than not
        // priming at all: the primed series stays at zero forever while the real
        // one appears beside it, so the absence-is-a-deployment-proof reading
        // above silently stops holding.
        LatticeMetrics.WalReplayStarvationDriveAbandonments.Add(0, treeTag, tenantTag);

        // The four above are lifecycle events and are named individually because
        // there is no enum to derive them from. The terminal outcomes are primed
        // by walking the enum instead of by listing them (issue #2938), which
        // makes the priming exhaustive by construction: a member added later is
        // primed without anyone remembering to, and if it has no arm the mapping
        // throws here on the first pass rather than reporting an uncounted
        // outcome as a measured zero. Listing them was how three of the four
        // came to be missing.
        foreach (var outcome in AllReactivationOutcomes)
        {
            RecordBlockedLeafReactivation(ReactivationOutcomeTag(outcome), treeTag, tenantTag, 0);
        }

        // Zero-prime the blocking-pin classifier at tree level (issue #3042).
        //
        // This instrument is tagged by partition, and a partition is only known
        // once a blocking consumer has been parsed - so the per-(tree,
        // partition) priming done at classification time cannot answer the
        // prior question a reader asks first: "is the classifier running here
        // at all?" On a tree that has never blocked there would be no series of
        // any shape, which is equally consistent with a silo that predates this
        // build. That is the exact ambiguity #3042 exists to remove, so leaving
        // it would reproduce the defect one level up from where it was fixed.
        //
        // The reserved PartitionNone value carries that reachability claim and
        // nothing else. It is minted here, at the top of CollectTreeAsync above
        // every early return, so a minted zero says a GC pass ran and evaluated
        // this tree; a real classification always carries its numeric
        // partition, so the two can never be confused in a query.
        PrimeBlockingPinStates(LatticeMetrics.PartitionNone, treeTag, tenantTag);

        // Issue #2692 Half B. The drive verdicts are primed on the same footing
        // and for the same reason: 'drove_lifted' is the series a reader will
        // query to decide whether the sweep repairs anything, so its absence
        // must mean "the scheduler is not running here" and never "the build
        // predates the drive".
        //
        // Walked rather than listed, for the reason issue #2938 established one
        // commit earlier on the block above. This change originally named its
        // five arms individually, which is the same shape that left three of the
        // four terminal outcomes unprimed; adopting the walk here means a verdict
        // added to LeafStarvationDriveOutcome later is primed without anyone
        // remembering to, and one with no arm throws out of DriveOutcomeTag on
        // the first pass rather than reporting an uncounted verdict as a measured
        // zero. Priming the whole enum also keeps the set summable - 'attempted'
        // stays the cost series, and the verdicts partition what came of it.
        foreach (var outcome in AllStarvationDriveOutcomes)
        {
            RecordBlockedLeafReactivation(DriveOutcomeTag(outcome), treeTag, tenantTag, 0);
        }
    }

    /// <summary>
    /// The single site that writes <see cref="LatticeMetrics.WalGcPasses"/>.
    /// </summary>
    /// <remarks>
    /// Both the real pass emissions and the zero primes route through here, so
    /// a primed series and the emission it anticipates carry an identical tag
    /// set by construction rather than by inspection. Repeating the tag list at
    /// each call site makes a divergence expressible, and a prime whose tags
    /// differ from its emission is worse than no prime at all: it mints a
    /// second series that is permanently zero while the one a reader queries
    /// stays absent, so the absence the prime exists to remove survives behind
    /// a decoy that looks like the fix landed.
    /// </remarks>
    private static void RecordPass(
        long delta,
        in KeyValuePair<string, object?> outcome,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
        => LatticeMetrics.WalGcPasses.Add(delta, treeTag, outcome, tenantTag);

    /// <summary>
    /// Maps the cursor-floor state of a pass that trimmed nothing onto the
    /// outcome arm that names <i>why</i> it trimmed nothing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The classification this replaced was
    /// <c>blocked ? blocked : idle</c>, so <c>idle</c> was a catch-all that
    /// absorbed <see cref="WalGcCursorFloorState.NoCursorReported"/> - a pass
    /// that could not evaluate the cursor branch at all - alongside the
    /// genuinely quiet case. That made <c>blocked = 0</c> read as evidence of
    /// health when it was only evidence that one named predicate had not fired
    /// (issue #2850). Only <see cref="LatticeMetrics.OutcomeReclaimed"/> is an
    /// affirmative reading; every arm below means nothing was trimmed.
    /// </para>
    /// <para>
    /// The switch is total over the enum and its fallback is its own arm rather
    /// than <see cref="LatticeMetrics.OutcomeIdle"/>. That is the whole point: a
    /// floor state added later must land somewhere a reader can see it, not be
    /// absorbed by the arm that means "healthy and quiet". Reaching
    /// <see cref="LatticeMetrics.OutcomeUnclassified"/> is impossible against
    /// today's enum, so a permanent zero there is the expected reading.
    /// </para>
    /// </remarks>
    private static KeyValuePair<string, object?> ClassifyUnreclaimed(WalGcCursorFloorState floorState)
        => floorState switch
        {
            WalGcCursorFloorState.BlockedByUnusablePin => LatticeMetrics.OutcomeBlocked,
            WalGcCursorFloorState.NoCursorReported => LatticeMetrics.OutcomeNoConsumer,
            WalGcCursorFloorState.Available => LatticeMetrics.OutcomeIdle,
            _ => LatticeMetrics.OutcomeUnclassified,
        };

    /// <summary>
    /// Adds <paramref name="addTicks"/> to <paramref name="nowTicks"/>,
    /// saturating at <see cref="long.MaxValue"/> so the next-due time can never
    /// overflow to a negative (past) tick count. <see cref="LatticeOptions.WalGcInterval"/>
    /// carries no upper bound and is not validated, so an operator-supplied
    /// interval near <see cref="TimeSpan.MaxValue"/> would otherwise wrap the
    /// sum negative: the tree would then compare due on every subsequent pass,
    /// <c>earliestDueTicks</c> would go negative, and the pass would return
    /// <see cref="TimeSpan.Zero"/> - driving the scheduler into a zero-wait busy
    /// loop, the exact opposite of the rare collection an extreme interval
    /// expresses. Saturating instead schedules the tree far in the future and
    /// lets the scheduler quiesce.
    /// </summary>
    private static long SaturatingDueTicks(long nowTicks, long addTicks)
        => addTicks > long.MaxValue - nowTicks ? long.MaxValue : nowTicks + addTicks;

    /// <summary>
    /// Doubles <paramref name="current"/> toward <paramref name="max"/>, never
    /// below <paramref name="min"/>. The ceiling test also guards the overflow:
    /// the doubling only runs when the result is provably below the ceiling.
    /// </summary>
    private static TimeSpan Relax(TimeSpan current, TimeSpan min, TimeSpan max)
    {
        var ticks = current.Ticks;
        if (ticks < min.Ticks)
        {
            ticks = min.Ticks;
        }

        return ticks >= max.Ticks / 2 ? max : TimeSpan.FromTicks(ticks * 2);
    }

    /// <summary>
    /// Returns the current no-collectable-tree wait and relaxes it for next
    /// time, so a silo with an empty or faulting registry retries promptly once
    /// and then backs off on the same geometric schedule a quiet tree does,
    /// instead of polling at the floor indefinitely.
    /// </summary>
    private TimeSpan Quiet(TimeSpan minInterval, TimeSpan interval)
    {
        var wait = _quietWait < minInterval ? minInterval : _quietWait;
        _quietWait = Relax(wait, minInterval, interval);
        return wait;
    }

    /// <summary>
    /// Drops cadence state for trees the registry no longer reports. Only walks
    /// the map when it holds more entries than this pass tracked, and removes in
    /// place - <see cref="Dictionary{TKey, TValue}"/> permits removal during
    /// enumeration - so the common no-churn case is a single integer comparison
    /// and the churn case allocates nothing.
    /// </summary>
    private void PruneRetiredTrees(int generation, int tracked)
    {
        if (_cadence.Count <= tracked)
        {
            return;
        }

        foreach (var entry in _cadence)
        {
            if (entry.Value.Generation != generation)
            {
                _cadence.Remove(entry.Key);
                _primedTrees.Remove(entry.Key);
                _blockedConsumers.Remove(entry.Key);
                snapshotPins?.Forget(entry.Key);
            }
        }
    }

    /// <summary>
    /// Observes a tree reported as blocked by one or more named consumers and,
    /// subject to a per-consumer minimum block age, retry cooldown and hard
    /// attempt budget, touches the owning leaves so they activate.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why reactivation heals, and why the ordering matters.</b> The touch
    /// itself fixes nothing. Activation replays the WAL forward, which advances
    /// the leaf's checkpoint, which makes the already-shipped zero-coverage
    /// repair applicable, which captures a snapshot and stamps coverage - and
    /// only then does the pin resolve to a real offset instead of the blocking
    /// sentinel. Read the other way round it looks like a no-op, which is why
    /// the sequence is spelled out here.
    /// </para>
    /// <para>
    /// <b>Blast radius.</b> At most
    /// <see cref="MaxReactivationTouchesPerPass"/> leaves per tree per pass, and
    /// trees are swept sequentially, so that is also the whole silo's concurrent
    /// touch ceiling. Live leaves are excluded by construction, because a live
    /// consumer never reaches the exit that reports it. The remedy is
    /// self-extinguishing - a healed tree stops reporting blocked and this path
    /// stops running - so steady-state cost on a healthy tree is zero.
    /// </para>
    /// <para>
    /// <b>Why the bound is no longer one.</b> It used to be one, and that was
    /// structural rather than chosen: the floor named a single blocking
    /// consumer, so a pass could not discover a second blocker to act on even in
    /// principle. The consequence was not a slower heal but no heal at all on
    /// the population that needs one. Every limit here is written and reasoned
    /// about per blocking leaf, yet a report naming one consumer collapsed them
    /// into a per-tree rate limit of roughly one leaf per
    /// <see cref="ReactivationRetryCooldown"/>; against a tree carrying
    /// thousands of blocked leaves that is not slow convergence, it is none, and
    /// it was measured as 2 attempts and 0 heals across 46 blocked passes while
    /// the WAL grew unbounded (issue #2768). The per-leaf limits are unchanged.
    /// Only the number of leaves one pass may apply them to has moved.
    /// </para>
    /// <para>
    /// <b>Touches are concurrent, and that is load-bearing.</b> A touch is a
    /// grain call into an activation that is, by hypothesis, unable to complete,
    /// so it routinely runs to the cluster response timeout. Issued serially,
    /// one such leaf would consume the whole pass and starve the other blockers
    /// of the tree - the failing leaf would deny the sweep to the leaves that
    /// might still heal. Issued concurrently, a pass costs the same wall-clock
    /// as it did when it made a single touch.
    /// </para>
    /// <para>
    /// <b>It is not assumed to work.</b> See <see cref="MaxReactivationAttempts"/>:
    /// a leaf whose capture cannot complete is touched a bounded number of times
    /// and then reported as abandoned, rather than retried forever. It is also
    /// not the whole remedy for a blocked tree, and must not be relied on as
    /// one: a touch can only ask a leaf to activate, so a leaf whose activation
    /// is itself being cancelled before it can bank a snapshot is beyond what
    /// any amount of touching can reach.
    /// </para>
    /// </remarks>
    private async Task ObserveAndHealBlockedTreeAsync(
        string treeId,
        IReadOnlyList<string> blockingConsumerIds,
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag,
        CancellationToken stoppingToken)
    {
        var now = _time.GetUtcNow();

        if (!_blockedConsumers.TryGetValue(treeId, out var observation))
        {
            observation = new BlockedConsumerObservation(
                EpisodeStarted: now,
                LastAnyAttempt: null,
                AnyAbandoned: false,
                Escalated: false,
                Budgets: new Dictionary<string, ConsumerReactivationBudget>(StringComparer.Ordinal));
            _blockedConsumers[treeId] = observation;
        }

        var budgets = observation.Budgets;

        // Admit this pass's reported blockers. A consumer seen for the first
        // time starts its own rate limiter here rather than inheriting one: a
        // newly revealed leaf must serve its own minimum block age, or a
        // draining tree would fire its remaining reactivations back to back.
        //
        // A consumer already known keeps every field it has. Carrying them is
        // the fix for issue #2772: this state used to be rebuilt whenever the
        // reported blocker changed, so the attempt count and the abandoned flag
        // were both destroyed by the sweep's own remedy - an activated leaf
        // reports a cursor and the floor then skips it - which made the give-up
        // branch unreachable rather than merely slow. Keyed per consumer, a
        // blocker that rotates out of the report and returns resumes its count.
        for (var i = 0; i < blockingConsumerIds.Count; i++)
        {
            var consumerId = blockingConsumerIds[i];
            if (budgets.TryGetValue(consumerId, out var known))
            {
                budgets[consumerId] = known with { LastObserved = now };
                continue;
            }

            budgets[consumerId] = new ConsumerReactivationBudget(
                FirstObserved: now,
                LastObserved: now);

            // Count every admission, but warn only on the first of the episode
            // (issue #2815). The warning used to fire here unconditionally, so
            // it was throttled per blocker identity - which is no throttle at
            // all on the population it matters for. A tree whose blockers churn
            // faster than ReactivationMinBlockAge admits a new consumer on every
            // pass, and a blocked tree is deliberately held at the cadence
            // floor, so the warning ran at the floor rate indefinitely: about
            // two a minute at stock defaults.
            //
            // That population is the same one the escalation below exists for -
            // no blocker holds still long enough to be touched, so no
            // attempt-derived budget can report it. It was therefore both
            // invisible to the give-up budget and the loudest thing in the log,
            // on exactly the rigs where the log stream is the only diagnosis
            // available.
            observation = observation with
            {
                DistinctBlockers = observation.DistinctBlockers + 1,
                FirstBlocker = observation.FirstBlocker ?? consumerId,
                LatestBlocker = consumerId,
            };

            if (!observation.WarnedBlocked)
            {
                observation = observation with { WarnedBlocked = true };
                WarnFloorBlocked(treeId, consumerId);
            }
        }

        // The admission bookkeeping above is on the observation, not on the
        // budgets map, so it has to be written back. Budgets is a reference and
        // mutates in place; the observation is a record struct and does not.
        _blockedConsumers[treeId] = observation;

        // Classify each blocker's durable-pin state (issue #3042). Placed after
        // admission so every blocker has a budget to latch the result on, and
        // before the reactivation machinery below so the classification is
        // recorded even for a tree whose blockers all rotate faster than the
        // minimum block age and are therefore never touched - that population is
        // invisible to every attempt-derived signal, and it is the one a reader
        // most needs classified.
        await ClassifyBlockingPinsAsync(treeId, blockingConsumerIds, budgets, treeTag, tenantTag, stoppingToken)
            .ConfigureAwait(false);

        PruneBlockedConsumerBudgets(budgets, now);

        // The escalation for a block the sweep cannot get purchase on at all.
        // Every clause below this one is gated on an attempt having been made,
        // and an attempt is gated on a reported blocker holding still for the
        // minimum block age - so a tree whose blockers all rotate faster than
        // that is never touched, and no attempt-derived budget can ever report
        // it. Unlike the per-leaf give-up this does not stop the sweep: there is
        // no evidence here that any particular touch is futile, only that none
        // is happening, and the sweep remains the sole remedy. Suppressed once
        // some consumer on this tree has been abandoned, so the two alarms
        // cannot both fire for one condition.
        if (!observation.Escalated
            && !observation.AnyAbandoned
            && now - (observation.LastAnyAttempt ?? observation.EpisodeStarted) >= UnreachableBlockEscalation)
        {
            observation = observation with { Escalated = true };
            _blockedConsumers[treeId] = observation;

            logger.LogWarning(
                "WAL GC has not been able to attempt a reactivation on tree {Tree} for {Elapsed}, while its cursor floor stayed blocked and its WAL stayed retained; the consumers reported as blocking keep changing before any one of them has been blocking long enough to touch, so the per-leaf attempt budget cannot report this tree. Currently reported blocker is {Consumer}, and the floor has named {DistinctBlockers} blocking consumers since the episode began. Investigate why this tree has several leaves whose snapshot capture does not complete.",
                treeId,
                now - (observation.LastAnyAttempt ?? observation.EpisodeStarted),
                blockingConsumerIds[0],
                observation.DistinctBlockers);
        }

        List<string>? touching = null;

        for (var i = 0; i < blockingConsumerIds.Count; i++)
        {
            if (touching is { Count: >= MaxReactivationTouchesPerPass })
            {
                break;
            }

            var consumerId = blockingConsumerIds[i];
            var budget = budgets[consumerId];

            if (budget.Attempts >= MaxReactivationAttempts)
            {
                if (!budget.Abandoned)
                {
                    // The budget is spent and the leaf is still blocking, so the
                    // block is not one activation away from clearing. Say so
                    // once, loudly, and stop paying for touches that do not
                    // work.
                    //
                    // This pauses the sweep for this consumer; it no longer
                    // stops it for the life of the process (issue #2783). The
                    // stamp below is what lets it resume: see TryRearm.
                    budget = budget with
                    {
                        Abandoned = true,
                        AbandonedAt = now,
                        HealEpochAtAbandonment = _reactivationHealEpoch,
                    };
                    budgets[consumerId] = budget;
                    observation = observation with { AnyAbandoned = true };
                    _blockedConsumers[treeId] = observation;
                    RecordBlockedLeafReactivation(
                        LatticeMetrics.BlockedLeafReactivationAbandoned, treeTag, tenantTag);

                    logger.LogWarning(
                        "WAL GC gave up reactivating consumer {Consumer} on tree {Tree} after {Attempts} attempts; it is still blocking the cursor floor, so the block is not clearable by activation alone and the WAL stays retained. The sweep will try again after {Backoff}. Investigate why the leaf's snapshot capture does not complete.",
                        consumerId,
                        treeId,
                        budget.Attempts,
                        RearmBackoff(budget.Cycles));

                    continue;
                }

                if (TryRearm(budget, now) is not { } rearmed)
                {
                    // Still inside the backoff for this consumer.
                    continue;
                }

                budget = rearmed;
                budgets[consumerId] = budget;
                RecordBlockedLeafReactivation(
                    LatticeMetrics.BlockedLeafReactivationRearmed, treeTag, tenantTag);

                logger.LogInformation(
                    "WAL GC restored the reactivation budget for consumer {Consumer} on tree {Tree} after a backoff (cycle {Cycle}); its cursor floor is still blocked, and the conditions that make a touch futile are transient, so the sweep tries again rather than staying stopped.",
                    consumerId,
                    treeId,
                    budget.Cycles);

                // Fall through: the restored budget is spendable on this very
                // pass, subject to the same block-age and cooldown gates as any
                // other attempt. The first touch of a new cycle is never made to
                // serve a further cooldown on top of the backoff it has already
                // waited out, but nothing here clears the cooldown stamp to
                // achieve that - the re-arm only rebuilds the budget. It falls
                // out of the constants instead: ReactivationRearmMinBackoff (the
                // shortest backoff any re-arm can serve) is >=
                // ReactivationRetryCooldown, and AbandonedAt is stamped no
                // earlier than the last attempt, so
                // now - LastAttempt >= now - AbandonedAt >= backoff >= cooldown
                // holds by transitivity and the cooldown gate below always
                // passes. Lowering ReactivationRearmMinBackoff under
                // ReactivationRetryCooldown would break that chain and silently
                // delay the first touch of each new cycle, so keep the two in
                // that order.
            }

            if (now - budget.FirstObserved < ReactivationMinBlockAge)
            {
                continue;
            }

            if (budget.LastAttempt is { } lastAttempt
                && now - lastAttempt < ReactivationRetryCooldown)
            {
                continue;
            }

            // Stamp the attempt BEFORE making it. A call that throws, times out
            // or is cancelled must still consume the budget and the cooldown, or
            // a leaf that fails fast would be retried every pass - turning a
            // rate-limited heal into the stampede this path is bounded to avoid.
            budgets[consumerId] = budget with
            {
                Attempts = budget.Attempts + 1,
                LastAttempt = now,
            };

            RecordBlockedLeafReactivation(
                LatticeMetrics.BlockedLeafReactivationAttempted, treeTag, tenantTag);

            (touching ??= new List<string>(MaxReactivationTouchesPerPass)).Add(consumerId);
        }

        if (touching is null)
        {
            return;
        }

        observation = observation with { LastAnyAttempt = now };
        _blockedConsumers[treeId] = observation;

        var touches = new Task<ReactivationOutcome>[touching.Count];
        for (var i = 0; i < touching.Count; i++)
        {
            touches[i] = TryReactivateBlockedLeafAsync(
                treeId, touching[i], treeTag, tenantTag, stoppingToken);
        }

        var outcomes = await Task.WhenAll(touches).ConfigureAwait(false);

        // Refund a faulted or undelivered touch, within a cap. Charging the
        // budget before the call is right for the cooldown - it is what stops a
        // fast-failing leaf being retried every pass - but it is wrong for the
        // give-up decision. Neither outcome establishes that activation would
        // fail to heal the leaf, which is the only thing abandonment is entitled
        // to conclude: a fault proves the silo was too busy to find out, and a
        // timeout proves only that the sweep stopped waiting for an answer. Both
        // are produced by exactly the transient pressure the re-arm exists to
        // outlast. Without the refund a burst of them spends the whole budget
        // without ever testing the leaf even once, so abandonment would be a
        // verdict on the burst rather than on the leaf.
        //
        // The cap is what keeps that safe. Unlimited refunds would let a leaf
        // that faults on every touch be retried once per cooldown for the life
        // of the process, never reaching abandonment and so never entering the
        // escalating backoff - trading a permanent stall for a permanent retry,
        // which is the failure mode this whole change exists to remove.
        for (var i = 0; i < outcomes.Length; i++)
        {
            // Recording is unconditional and no longer shares a branch with the
            // refund decision below (issue #2938). While the two were one piece
            // of control flow, only the outcome that happened to need a refund
            // was counted, and the other three reported a structural zero that
            // read as a measured one.
            RecordBlockedLeafReactivation(
                ReactivationOutcomeTag(outcomes[i]), treeTag, tenantTag);

            if (!IsRefundableReactivationOutcome(outcomes[i]))
            {
                continue;
            }

            var current = budgets[touching[i]];
            if (current.Refunds < MaxReactivationRefunds)
            {
                budgets[touching[i]] = current with
                {
                    Attempts = current.Attempts - 1,
                    Refunds = current.Refunds + 1,
                };
            }
        }
    }

    /// <summary>
    /// Drops budgets for consumers the floor has stopped reporting, so an
    /// episode whose blockers rotate for a long time cannot grow the map
    /// without bound.
    /// </summary>
    /// <remarks>
    /// Only ever drops an entry with no live attempt state - never touched,
    /// never abandoned, never re-armed - so it cannot destroy the evidence that
    /// a heal is not working, which is the defect issue #2772 fixed. A consumer
    /// the sweep has acted on is retained for the life of the episode, and the
    /// whole map is dropped when the episode ends.
    /// </remarks>
    private static void PruneBlockedConsumerBudgets(
        Dictionary<string, ConsumerReactivationBudget> budgets,
        DateTimeOffset now)
    {
        if (budgets.Count <= LatticeWalGc.MaxReportedBlockingConsumers)
        {
            return;
        }

        foreach (var entry in budgets)
        {
            var budget = entry.Value;
            if (budget.Attempts == 0
                && budget.Cycles == 0
                && !budget.Abandoned
                && budget.LastAttempt is null
                && now - budget.LastObserved >= BlockedConsumerRetention)
            {
                budgets.Remove(entry.Key);
            }
        }
    }

    /// <summary>
    /// Warns, once per blocked episode, that a tree's cursor floor is blocked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Throttled per episode and not per blocker identity (issue #2815). The
    /// per-identity form was no throttle at all for the population that needs
    /// one: a blocked tree is deliberately held at the cadence floor, so a tree
    /// whose reported blocker changes every pass emitted this at the floor rate
    /// indefinitely - roughly two a minute at stock defaults, for as long as the
    /// silo ran.
    /// </para>
    /// <para>
    /// <b>What the throttle gives up, and where it is paid back.</b> The
    /// warning's diagnostic value is that it names <i>which</i> consumer, so one
    /// line per episode loses the identity sequence. The sequence is not
    /// dropped, it is summarised: the episode carries a distinct-blocker count
    /// and the first and most recent identities, which
    /// <see cref="ReportBlockerChurn"/> reports when the episode ends and the
    /// unreachable-block escalation reports when it fires. A count with both
    /// ends of the sequence answers the three questions the sequence was being
    /// read for - is it churning, how wide, and which leaf do I open - whereas a
    /// bounded sample of the first few identities answers only the first and
    /// re-creates this same unbounded log in miniature as the bound is raised.
    /// </para>
    /// </remarks>
    private void WarnFloorBlocked(string treeId, string blockingConsumerId) =>
        logger.LogWarning(
            "WAL GC for tree {Tree} cannot reclaim: durable materialiser pin {Consumer} carries no usable offset, so the cursor floor is blocked and the WAL is retained without bound. The consumer id embeds the owning leaf's grain id. Other leaves on this tree may also be blocked; a pass reports a bounded number of them, so an unreported leaf is not evidence of an unblocked one. This fires once for the whole blocked episode rather than once per reported blocker, so it naming one consumer is not evidence that only one was blocking; the distinct-blocker count and both ends of the sequence are reported when the episode escalates or ends.",
            treeId,
            blockingConsumerId);

    /// <summary>
    /// Reports the blocker churn an ending episode saw, which is where the
    /// identity sequence that <see cref="WarnFloorBlocked"/> no longer streams
    /// is paid back (issue #2815).
    /// </summary>
    /// <remarks>
    /// Silent on a single-blocker episode, because the warning already named
    /// that consumer and there is nothing the summary would add. Logged at
    /// information rather than warning: an episode that ends is a tree that
    /// recovered, and the condition it describes has already been alarmed on.
    /// </remarks>
    private void ReportBlockerChurn(string treeId, in BlockedConsumerObservation observation)
    {
        if (observation.DistinctBlockers <= 1)
        {
            return;
        }

        logger.LogInformation(
            "WAL GC tree {Tree} is no longer blocked after {Elapsed}. Its cursor floor named {DistinctBlockers} blocking consumers during the episode, first {FirstConsumer} and most recently {Consumer}; the per-blocker warning is emitted once per episode, so this is where that rotation is reported rather than in the log stream throughout it. The count is of admissions, so a consumer that rotated out for over an hour and returned is counted twice - read it as how widely the blockers churned, never as a leaf inventory.",
            treeId,
            _time.GetUtcNow() - observation.EpisodeStarted,
            observation.DistinctBlockers,
            observation.FirstBlocker,
            observation.LatestBlocker);
    }

    /// <summary>
    /// Ends a tree's blocked episode, crediting a heal for every consumer the
    /// sweep touched during it.
    /// </summary>
    private void ClearBlockedObservation(
        string treeId,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (_blockedConsumers.Remove(treeId, out var observation))
        {
            ReportBlockerChurn(treeId, observation);
            CreditHealedConsumers(observation, treeTag, tenantTag);
        }
    }

    /// <summary>
    /// Records that the consumers this sweep reactivated have stopped blocking.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is evidence that the sweep is achieving something, not proof that it
    /// caused the heal - an ordinary read or write could have touched the leaf
    /// first. Attributing precisely is not possible from here, and the useful
    /// question this answers is the coarse one: are reactivated leaves clearing
    /// at all, or is the sweep running without effect? A consumer that was never
    /// swept is not credited, so the ratio against <c>attempted</c> stays
    /// meaningful.
    /// </para>
    /// <para>
    /// <b>Why this waits for the episode to end.</b> It used to credit the
    /// outgoing consumer each time the reported blocker changed, which read as
    /// "that one stopped blocking". It does not mean that: the floor skips
    /// consumers present in the live registry, so a leaf the sweep has just
    /// touched drops off the head of the queue precisely because it activated,
    /// and it returns when it deactivates still blocked. Crediting there counted
    /// one leaf many times and counted leaves that never healed at all. Deferred
    /// to the end of the episode the claim is sound, because the floor is no
    /// longer blocked by anything, so every consumer swept during it has in fact
    /// stopped blocking. The ratio this feeds is documented as the measure of
    /// whether the sweep works, so an inflated numerator is worse than a missing
    /// one.
    /// </para>
    /// </remarks>
    private void CreditHealedConsumers(
        BlockedConsumerObservation observation,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        foreach (var budget in observation.Budgets.Values)
        {
            if (budget.Attempts > 0 && !budget.Abandoned)
            {
                // Advance the heal epoch before recording. A credited heal is
                // direct evidence that a blocked leaf managed to activate,
                // replay and capture a snapshot in this process right now - so
                // whatever headroom a stranded leaf needs demonstrably exists,
                // and any consumer abandoned before this moment is entitled to
                // retry on the shortened backoff rather than serve out an
                // escalation earned under conditions that have since lifted.
                //
                // A consumer re-armed after abandonment is creditable here
                // again, because the re-arm clears Abandoned: its budget is
                // live, and if it stops blocking it healed in exactly the sense
                // this counter means.
                _reactivationHealEpoch++;
                RecordBlockedLeafReactivation(
                    LatticeMetrics.BlockedLeafReactivationHealed, treeTag, tenantTag);
            }
        }
    }

    /// <summary>
    /// Restores an abandoned consumer's attempt budget once its backoff has
    /// elapsed, or returns <see langword="null"/> while it has not.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The budget is <b>restored, not widened</b>: a new cycle buys the same
    /// <see cref="MaxReactivationAttempts"/> touches at the same cooldown as
    /// the first, so the sweep's cost per cycle is unchanged and only the gap
    /// between cycles grows.
    /// </para>
    /// <para>
    /// Two signals gate the wait, and the choice between them is the substance
    /// of issue #2783. The floor is elapsed time with an escalating backoff,
    /// which is a bare timer and the weakest acceptable signal - but strictly
    /// better than never retrying. Above it sits evidence: if any blocked leaf
    /// in this process has healed since this consumer was abandoned, the
    /// backoff collapses to <see cref="ReactivationRearmMinBackoff"/>. That is
    /// preferred to a timer because it is a statement about the blocking
    /// condition rather than about the clock - a completed capture means the
    /// memory headroom and replay capacity a stranded leaf needs were available
    /// moments ago.
    /// </para>
    /// <para>
    /// <see cref="ConsumerReactivationBudget.Refunds"/> resets with the cycle,
    /// so each cycle gets its own refund allowance. A restored budget is
    /// spendable on the caller's very next gate rather than after a further
    /// cooldown, which holds because
    /// <see cref="ReactivationRearmMinBackoff"/> is not shorter than
    /// <see cref="ReactivationRetryCooldown"/> - not because anything clears the
    /// cooldown stamp, which lives on the observation and is untouched here.
    /// </para>
    /// </remarks>
    private ConsumerReactivationBudget? TryRearm(ConsumerReactivationBudget budget, DateTimeOffset now)
    {
        if (budget.AbandonedAt is not { } abandonedAt)
        {
            return null;
        }

        var backoff = RearmBackoff(budget.Cycles);
        if (_reactivationHealEpoch != budget.HealEpochAtAbandonment)
        {
            backoff = ReactivationRearmMinBackoff;
        }

        if (now - abandonedAt < backoff)
        {
            return null;
        }

        return budget with
        {
            Attempts = 0,
            Abandoned = false,
            AbandonedAt = null,
            Refunds = 0,
            Cycles = budget.Cycles + 1,
        };
    }

    /// <summary>
    /// The backoff a consumer serves before its <paramref name="cycles"/>'th
    /// re-arm: <see cref="ReactivationRearmBaseBackoff"/> doubling per cycle
    /// and saturating at <see cref="ReactivationRearmMaxBackoff"/>.
    /// </summary>
    private static TimeSpan RearmBackoff(int cycles)
    {
        var ticks = ReactivationRearmBaseBackoff.Ticks;
        var ceiling = ReactivationRearmMaxBackoff.Ticks;

        for (var i = 0; i < cycles && ticks < ceiling; i++)
        {
            ticks *= 2;
        }

        return ticks >= ceiling ? ReactivationRearmMaxBackoff : TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    /// The single site that writes
    /// <see cref="LatticeMetrics.WalGcBlockedLeafReactivations"/>.
    /// </summary>
    /// <remarks>
    /// Both the real emissions and the zero primes route through here, so a
    /// primed series and the emission it anticipates carry an identical tag set
    /// by construction rather than by inspection. A prime whose tags differ
    /// from its emission is worse than no prime at all: it mints a second
    /// series that is permanently zero while the one a reader queries stays
    /// absent, so the absence the prime exists to remove survives behind a
    /// series that looks like it answers the question.
    /// </remarks>
    private static void RecordBlockedLeafReactivation(
        KeyValuePair<string, object?> outcome,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag,
        long delta = 1) =>
        LatticeMetrics.WalGcBlockedLeafReactivations.Add(delta, treeTag, outcome, tenantTag);

    /// <summary>
    /// Every member of <see cref="WalGcBlockingPinState"/>, walked when priming
    /// <see cref="LatticeMetrics.WalGcBlockingPinStates"/> so the priming is
    /// exhaustive by construction rather than by anyone remembering to extend a
    /// list. A member added later is primed without a code change here, and if
    /// it has no arm the mapping throws on the first pass instead of reporting
    /// an uncounted state as a measured zero (the failure mode issue #2938
    /// found three times over on the sibling instrument).
    /// </summary>
    private static readonly WalGcBlockingPinState[] AllBlockingPinStates =
        Enum.GetValues<WalGcBlockingPinState>();

    /// <summary>
    /// Maps a classified blocking-pin state onto the
    /// <see cref="LatticeMetrics.WalGcBlockingPinStates"/> arm that names it.
    /// </summary>
    /// <remarks>
    /// Throws on an unmapped member rather than falling back to a catch-all.
    /// A catch-all would let a newly added state be counted under an existing
    /// arm, which on this instrument is worse than not counting it at all: every
    /// arm here is read as evidence about whether a block is repairable, and a
    /// misfiled state is a wrong answer rather than a missing one.
    /// </remarks>
    internal static KeyValuePair<string, object?> BlockingPinStateTag(WalGcBlockingPinState state) =>
        state switch
        {
            WalGcBlockingPinState.CheckpointedUncovered => LatticeMetrics.BlockingPinCheckpointedUncovered,
            WalGcBlockingPinState.NeverCheckpointed => LatticeMetrics.BlockingPinNeverCheckpointed,
            WalGcBlockingPinState.NoDurableState => LatticeMetrics.BlockingPinNoDurableState,
            WalGcBlockingPinState.Unreadable => LatticeMetrics.BlockingPinUnreadable,
            _ => throw new ArgumentOutOfRangeException(
                nameof(state),
                state,
                "No " + LatticeMetrics.WalGcBlockingPinStatesName + " arm is armed for this blocking-pin state. "
                    + "Add one rather than letting the state be counted under another arm: every arm on this "
                    + "instrument is read as evidence about whether a block is repairable, so a misfiled state "
                    + "is a wrong answer rather than a missing one."),
        };

    /// <summary>
    /// Records one classified blocking consumer against
    /// <see cref="LatticeMetrics.WalGcBlockingPinStates"/>.
    /// </summary>
    private static void RecordBlockingPinState(
        WalGcBlockingPinState state,
        string partition,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag,
        long delta = 1) =>
        LatticeMetrics.WalGcBlockingPinStates.Add(
            delta,
            treeTag,
            new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition),
            BlockingPinStateTag(state),
            tenantTag);

    /// <summary>
    /// Zero-primes every <see cref="LatticeMetrics.WalGcBlockingPinStates"/>
    /// arm for one <c>(tree, partition)</c>.
    /// </summary>
    private static void PrimeBlockingPinStates(
        string partition,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        for (var i = 0; i < AllBlockingPinStates.Length; i++)
        {
            RecordBlockingPinState(AllBlockingPinStates[i], partition, treeTag, tenantTag, 0);
        }
    }

    /// <summary>
    /// Maps a leaf's starvation-drive verdict onto the
    /// <see cref="LatticeMetrics.WalGcBlockedLeafReactivations"/> outcome arm
    /// that names it (issue #2692 Half B).
    /// </summary>
    /// <remarks>
    /// <para>
    /// The unmapped verdict throws rather than falling back to a catch-all,
    /// matching <see cref="ReactivationOutcomeTag"/> and for the reason issue
    /// #2938 established: a verdict nobody has classified must not be folded
    /// into a neighbouring bucket, because that produces a plausible wrong
    /// number in exactly the place a reader trusts one.
    /// </para>
    /// <para>
    /// This replaced a <c>_ =&gt; drove_not_driven</c> fallback, whose stated
    /// justification was that an unrecognised verdict must not read as a repair.
    /// That is true and insufficient. A catch-all also makes the omission
    /// undetectable: a member added without a case here would be absorbed
    /// silently, the mapping would stay total, and no reflection gate could
    /// distinguish a classified verdict from an unclassified one. Throwing
    /// converts a silent miscount into a first-pass failure, which is the only
    /// form in which the omission is observable at all.
    /// </para>
    /// <para>
    /// Internal rather than private so
    /// <c>LatticeWalGcSchedulerCadenceTests.StarvationDriveOutcomeArming</c> can
    /// drive it for every declared member.
    /// </para>
    /// </remarks>
    internal static KeyValuePair<string, object?> DriveOutcomeTag(LeafStarvationDriveOutcome outcome)
        => outcome switch
        {
            LeafStarvationDriveOutcome.NotDriven => LatticeMetrics.BlockedLeafReactivationDroveNotDriven,
            LeafStarvationDriveOutcome.Lifted => LatticeMetrics.BlockedLeafReactivationDroveLifted,
            LeafStarvationDriveOutcome.NoAdvance => LatticeMetrics.BlockedLeafReactivationDroveNoAdvance,
            LeafStarvationDriveOutcome.MemoryRefused => LatticeMetrics.BlockedLeafReactivationDroveMemoryRefused,
            LeafStarvationDriveOutcome.AlreadyDriving => LatticeMetrics.BlockedLeafReactivationDroveAlreadyDriving,
            LeafStarvationDriveOutcome.TimedOut => LatticeMetrics.BlockedLeafReactivationDroveTimedOut,
            _ => throw new ArgumentOutOfRangeException(
                nameof(outcome),
                outcome,
                "Every starvation-drive verdict must have a metric arm; an unmapped one would report as a structural zero indistinguishable from a measured one (issue #2692)."),
        };

    /// <summary>
    /// Every declared starvation-drive verdict, cached once.
    /// </summary>
    /// <remarks>
    /// Cached because the priming path walks it per tree per pass and
    /// <see cref="Enum.GetValues{TEnum}"/> allocates a fresh array on each call.
    /// Derived from the enum rather than written out, so it cannot fall behind
    /// the type it describes.
    /// </remarks>
    private static readonly LeafStarvationDriveOutcome[] AllStarvationDriveOutcomes =
        Enum.GetValues<LeafStarvationDriveOutcome>();

    /// <summary>
    /// Resolves a blocking consumer id back to its owning leaf and drives its
    /// WAL replay forward, so the leaf advances its persisted checkpoint and
    /// stamps snapshot coverage even when it is already active.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Best-effort by design. A failure here retains WAL, which is the safe
    /// direction - the block simply persists, exactly as it did before this
    /// path existed - so a fault is logged and the pass continues rather than
    /// failing the collection of every other tree.
    /// </para>
    /// <para>
    /// <b>Why this is no longer a read-only touch (issue #2692 Half B).</b> It
    /// used to call <c>GetTreeIdAsync</c>, on the reasoning that "the work is
    /// done by activation, not by the call". That reasoning is sound for a
    /// <i>dormant</i> leaf and is precisely the defect for a live one: the
    /// per-partition checkpoint advance is reached from one call site in the
    /// whole solution, inside <c>OnActivateAsync</c>, so for an already-active
    /// leaf it is not merely unlikely to run, it is unreachable. The touch
    /// returned promptly, replayed nothing, and was recorded as
    /// <see cref="ReactivationOutcome.Completed"/> - a value nothing inspected.
    /// The sweep was therefore not failing loudly, it was succeeding vacuously,
    /// and the leaf was re-selected on every cooldown forever.
    /// </para>
    /// </remarks>
    private async Task<ReactivationOutcome> TryReactivateBlockedLeafAsync(
        string treeId,
        string blockingConsumerId,
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag,
        CancellationToken stoppingToken)
    {
        var factory = grainFactory;
        if (factory is null || !TryResolveLeafGrainId(treeId, blockingConsumerId, out var leafGrainId))
        {
            // Not refundable. An id that does not resolve is a permanent
            // property of the id, so it would resolve to nothing again on every
            // retry - refunding it would produce an unbounded attempted counter
            // with no touches behind it.
            return ReactivationOutcome.Unresolvable;
        }

        try
        {
            // Drive the leaf's replay rather than merely touching it. A
            // read-only call is enough ONLY for a dormant leaf; a resident one
            // answers it immediately and replays nothing, which is the whole of
            // issue #2692 Half B. The drive is [AlwaysInterleave] and takes a
            // permit from the same per-silo replay gate an activation replay
            // takes one from, so it neither blocks the leaf's foreground traffic
            // nor escapes the concurrency bound.
            var leaf = factory.GetGrain<IBPlusLeafGrain>(leafGrainId);
            var drive = await leaf.DriveStarvedCheckpointAsync().ConfigureAwait(false);

            // Record what the drive actually achieved, per leaf. 'attempted' is
            // the cost series and says only that a call was issued; these five
            // arms partition what came of it. Without them a sweep that repairs
            // nothing is indistinguishable from one that repairs every leaf it
            // reaches while the tree stays blocked for an unrelated reason.
            RecordBlockedLeafReactivation(DriveOutcomeTag(drive), treeTag, tenantTag);

            logger.LogInformation(
                "WAL GC drove leaf {Leaf} on tree {Tree} to clear a blocking durable materialiser pin ({Consumer}); the drive replays the WAL forward and stamps snapshot coverage, and reported {Drive}. Only 'Lifted' means the pin now resolves to a real offset.",
                leafGrainId,
                treeId,
                blockingConsumerId,
                drive);

            return ReactivationOutcome.Completed;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (TimeoutException ex)
        {
            // The probe did not return in time. Reported separately from a
            // fault because it means the opposite thing about the leaf: the
            // activation request was delivered and is very probably still in
            // flight - a caller-side timeout does not cancel the callee - so
            // this is the signature of the stuck activation the sweep exists to
            // clear, not a sign the touch was misdirected.
            //
            // Stated plainly because a reader will otherwise conclude the sweep
            // failed here: this arm advancing while 'healed' stays flat is
            // evidence that reactivation-by-grain-call cannot reach the leaves
            // that need it, which is a finding about the REMEDY and not about
            // the tree. That distinction is unavailable while a timeout is
            // folded into 'attempted'.
            logger.LogWarning(
                ex,
                "WAL GC could not confirm reactivation of leaf {Leaf} on tree {Tree} to clear blocking pin {Consumer} within the cluster response timeout; the activation request was delivered and may still be running, so this is not evidence the touch was wasted. The tree stays blocked and the attempt is retried after the cooldown.",
                leafGrainId,
                treeId,
                blockingConsumerId);

            return ReactivationOutcome.Undelivered;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "WAL GC could not reactivate leaf {Leaf} on tree {Tree} to clear blocking pin {Consumer}; the tree stays blocked and the attempt is retried after the cooldown.",
                leafGrainId,
                treeId,
                blockingConsumerId);

            return ReactivationOutcome.Faulted;
        }
    }

    /// <summary>
    /// Parses a materialiser consumer id back into the grain id of the leaf
    /// that published it.
    /// </summary>
    /// <remarks>
    /// Fail-closed: anything that does not match the exact expected shape
    /// resolves nothing and no leaf is touched. Reactivating the wrong grain
    /// would be worse than leaving the tree blocked, so an unrecognised id is
    /// treated as unresolvable rather than guessed at.
    /// </remarks>
    private bool TryResolveLeafGrainId(string treeId, string consumerId, out GrainId leafGrainId) =>
        TryResolveLeafGrainId(treeId, consumerId, out leafGrainId, out _);

    /// <summary>
    /// Parses a materialiser consumer id back into the grain id of the leaf
    /// that published it <i>and</i> the WAL partition the pin belongs to.
    /// </summary>
    /// <remarks>
    /// The partition is the half <see cref="TryResolveLeafGrainId(string, string, out GrainId)"/>
    /// discards, and it is exactly what the blocking-pin classifier needs: the
    /// leaf's durable checkpoint is per-partition, so a classification that did
    /// not know which partition blocked would have to guess one. A consumer id
    /// carrying no suffix is partition <c>0</c>, matching the legacy
    /// single-partition shape the unsuffixed form exists for.
    /// </remarks>
    private bool TryResolveLeafGrainId(string treeId, string consumerId, out GrainId leafGrainId, out int partition)
    {
        leafGrainId = default;
        partition = 0;

        var expectedStart = $"{BPlusTree.Grains.ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_";
        if (!consumerId.StartsWith(expectedStart, StringComparison.Ordinal))
        {
            return false;
        }

        var remainder = consumerId[expectedStart.Length..];
        if (remainder.Length == 0)
        {
            return false;
        }

        // A multi-partition leaf appends "_{partition}". Strip it only when the
        // tree is actually partitioned, so a grain id that legitimately ends in
        // "_<digits>" on a single-partition tree is not silently truncated.
        if (optionsMonitor.Get(treeId).WalPartitions > 1)
        {
            var lastSeparator = remainder.LastIndexOf('_');
            if (lastSeparator > 0
                && remainder.AsSpan(lastSeparator + 1).Length > 0
                && ulong.TryParse(remainder.AsSpan(lastSeparator + 1), out var parsedPartition))
            {
                remainder = remainder[..lastSeparator];
                partition = parsedPartition > int.MaxValue ? int.MaxValue : (int)parsedPartition;
            }
        }

        return GrainId.TryParse(remainder, out leafGrainId);
    }

    /// <summary>
    /// Classifies which durable-pin state each blocking consumer is in, without
    /// activating the leaf (issue #3042).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why this cannot ask the leaf.</b> The population being measured is
    /// precisely the population that will not activate - a leaf that could be
    /// activated would report a cursor, be <i>present</i> in the registry, and
    /// therefore be skipped by the floor before its pin was ever evaluated, so
    /// it could not have been the blocker. An instrument that needed the leaf
    /// live would measure only leaves that are not the problem, which is the
    /// same error as the coverage repair it exists to adjudicate. The read
    /// therefore goes straight to the storage provider, following the
    /// precedent set by <c>LeafCursorReporter.DirectStoreSlotAsync</c> - a path
    /// that exists specifically as the fallback for when the grain cannot be
    /// called.
    /// </para>
    /// <para>
    /// <b>Cost.</b> A leaf's durable state carries its projection, so this is
    /// not a cheap read. It is charged once per consumer per blocked episode,
    /// not once per pass: the result is latched on the consumer's budget, which
    /// lives on the episode observation and is discarded with it when the tree
    /// unblocks. A tree blocked for an hour at the cadence floor therefore pays
    /// for one read per blocker, not one per pass. The latch is deliberately
    /// not refreshed while an episode runs - a classification that changed would
    /// mean the leaf wrote durable state, which requires an activation, which
    /// would clear the block and end the episode.
    /// </para>
    /// <para>
    /// <b>Diagnostic only.</b> Nothing here feeds the trim predicate. A pass
    /// reclaims exactly the entries it would have reclaimed had this method not
    /// run, and a throw is swallowed into the <c>unreadable</c> arm rather than
    /// failing the pass.
    /// </para>
    /// </remarks>
    private async Task ClassifyBlockingPinsAsync(
        string treeId,
        IReadOnlyList<string> blockingConsumerIds,
        Dictionary<string, ConsumerReactivationBudget> budgets,
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag,
        CancellationToken stoppingToken)
    {
        for (var i = 0; i < blockingConsumerIds.Count; i++)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            var consumerId = blockingConsumerIds[i];
            if (!budgets.TryGetValue(consumerId, out var budget) || budget.PinStateClassified)
            {
                continue;
            }

            // Latch before the read, not after. A read that throws must still
            // count exactly once - retrying it every pass would turn a single
            // unreadable leaf into an unbounded stream of storage calls on a
            // tree that is, by construction, blocked indefinitely.
            budgets[consumerId] = budget with { PinStateClassified = true };

            var resolved = TryResolveLeafGrainId(treeId, consumerId, out var leafGrainId, out var partition);
            var partitionTag = resolved
                ? partition.ToString(CultureInfo.InvariantCulture)
                : LatticeMetrics.PartitionUnknown;

            var state = resolved
                ? await ReadBlockingPinStateAsync(leafGrainId, partition, stoppingToken).ConfigureAwait(false)
                : WalGcBlockingPinState.Unreadable;

            // Prime this partition's other arms before recording the one that
            // resolved, so a reader sees a measured zero on the states that did
            // not apply rather than an absence they have to interpret.
            PrimeBlockingPinStates(partitionTag, treeTag, tenantTag);
            RecordBlockingPinState(state, partitionTag, treeTag, tenantTag);

            logger.LogInformation(
                "WAL GC classified blocking pin {Consumer} on tree {Tree} partition {Partition} as {PinState}. This is diagnostic only and does not change what the pass may trim.",
                consumerId,
                treeId,
                partitionTag,
                state);
        }
    }

    /// <summary>
    /// Reads one leaf's persisted projection checkpoint for a partition
    /// directly from the storage provider and maps it onto a
    /// <see cref="WalGcBlockingPinState"/>. Never activates the leaf.
    /// </summary>
    private async Task<WalGcBlockingPinState> ReadBlockingPinStateAsync(
        GrainId leafGrainId,
        int partition,
        CancellationToken stoppingToken)
    {
        if (leafStateStorage is null)
        {
            // No storage provider on this silo. That is a property of the
            // measurement, not of the leaf, so it is reported as unreadable
            // rather than as an absence of durable state.
            return WalGcBlockingPinState.Unreadable;
        }

        try
        {
            var grainState = new GrainState<LeafNodeState>(new LeafNodeState());
            await leafStateStorage.ReadStateAsync(LeafStateName, leafGrainId, grainState)
                .ConfigureAwait(false);

            if (!grainState.RecordExists || grainState.State is null)
            {
                return WalGcBlockingPinState.NoDurableState;
            }

            return ClassifyCheckpoint(grainState.State, partition);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            logger.LogDebug(
                ex,
                "WAL GC could not read durable state for leaf {Leaf} to classify its blocking pin; counting it as unreadable. The pass is unaffected - this read is diagnostic only.",
                leafGrainId);

            return WalGcBlockingPinState.Unreadable;
        }
    }

    /// <summary>
    /// Maps a leaf's persisted projection checkpoint for one partition onto the
    /// two states that describe a real leaf.
    /// </summary>
    /// <remarks>
    /// The per-partition array is the authority when present. When it is absent
    /// the state predates multi-partition replay, and the scalar slot holds
    /// partition <c>0</c>'s checkpoint only - so partition <c>0</c> reads the
    /// scalar and any higher partition has genuinely never been checkpointed
    /// under that layout. A partition beyond the persisted array's length is the
    /// same case: the leaf has never written a checkpoint there.
    /// </remarks>
    internal static WalGcBlockingPinState ClassifyCheckpoint(LeafNodeState state, int partition)
    {
        var byPartition = state.ProjectionCheckpointOffsetsByPartition;

        var checkpoint = byPartition is not null
            ? (partition >= 0 && partition < byPartition.Length ? byPartition[partition] : -1L)
            : (partition == 0 ? state.ProjectionCheckpointOffset : -1L);

        // >= 0 means the leaf durably applied up to that offset, so there is a
        // WAL offset it could honestly claim and the unusable pin is the
        // coverage half of min(checkpoint, covered) being absent - repairable.
        // < 0 is the sentinel for "nothing applied", and the blocking pin is
        // then correct by design rather than a defect.
        return checkpoint >= 0
            ? WalGcBlockingPinState.CheckpointedUncovered
            : WalGcBlockingPinState.NeverCheckpointed;
    }

    /// <summary>
    /// Durable state name of <c>BPlusLeafGrain</c>'s persisted
    /// <see cref="LeafNodeState"/>, as declared by its
    /// <c>[PersistentState("leaf", ...)]</c> injection. The classifier reads
    /// the same slot the grain would, which is what makes a direct read
    /// equivalent to asking the leaf.
    /// </summary>
    private const string LeafStateName = "leaf";

    /// <summary>
    /// One tree's adaptive cadence state.
    /// </summary>
    /// <param name="IntervalTicks">The interval most recently selected for the tree, in ticks.</param>
    /// <param name="NextDueTicks">UTC tick count at which the tree becomes collectable again.</param>
    /// <param name="Generation">Pass counter that last observed the tree in the registry.</param>
    private readonly record struct TreeCadence(long IntervalTicks, long NextDueTicks, int Generation);
}
