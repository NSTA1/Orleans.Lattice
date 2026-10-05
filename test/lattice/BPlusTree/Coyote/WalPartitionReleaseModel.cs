using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// A Coyote model of one leaf whose writes span two WAL partitions, built to
/// exercise the arm of <see cref="LeafDurablePinCore.Resolve"/> that no other
/// model reaches: the empty release a leaf with a live clock publishes for a
/// partition it has applied nothing in (issue #4433, review finding F08). The
/// release carries the leaf's clock as its frontier and no offset, so it neither
/// holds its partition (it is not a Zero pin) nor joins the partition's offset
/// floor, and the GC's cursor arm trims below the frontier.
/// <para>
/// The leaf writes to either partition (each write applied in the foreground and
/// stamped above the leaf's clock), reads each partition through its own read
/// position, persists each checkpoint (persisting the clock with it), captures a
/// snapshot covering both partitions at their read positions, publishes a pin per
/// partition through <see cref="LeafDurablePinCore.Resolve"/>, stops, and
/// reactivates from its snapshot or cold. The GC trims each partition through
/// <see cref="Orleans.Lattice.WalGcTrimCore.IsEntryEligible"/>, with the cursor
/// arm and the offset admission computed as <c>LatticeWalGc</c> computes them:
/// a partition holding a Zero pin with no offset is held; the offset floor of a
/// partition is the lowest offset its pins report, falling back to the lowest
/// offset across the tree when none of its pins reports one; the cursor is the
/// lowest frontier; and the uncovered cursor is the lowest frontier among pins
/// with no offset. That glue is the model's, not a production core.
/// </para>
/// <para>
/// <c>releaseOnlyAfterReplay</c> selects the publisher: when <see langword="false"/>
/// a pin may be published for a partition before the activation has replayed it;
/// when <see langword="true"/> the empty release is published only for a partition
/// this activation has replayed to its head or whose WAL is proven empty, and the
/// block pin is published otherwise. Production waits: until the replay barrier
/// latches, <c>BPlusLeafGrain.WithUnreplayedPartitionsLive</c> counts every partition
/// as data-bearing when it resolves a pin, so none releases empty (issue #4669,
/// fixed by #4677); the first-swept partition's checkpoint flush tail had published
/// such a release before.
/// </para>
/// <para>
/// With <c>overrideStampedWrites</c> the leaf also appends records stamped below
/// its clock (issue #4641). The landed design (#4679), which this model's defaults
/// are: before such an append the leaf durably raises an override hold for the
/// partition's consumer, at most once per partition per activation and a no-op once
/// the consumer's stored offset is real; the pin store drops the hold only in the
/// write that lands that consumer's first real offset; and a GC pass reads each
/// partition's head bound, then the holds, then the offset census, each a separate
/// step, reading a held uncovered consumer as a Zero block pin.
/// </para>
/// <para>
/// With <c>ttl</c> a retention ceiling also trims, admitting entries by age. A
/// pin's frontier only rises, so an empty release published before the leaf
/// writes to that partition leaves an uncovered pin no Zero hold can see. Issue
/// #4622 caps the partition's TTL ceiling at the lowest frontier of its uncovered
/// pins (<c>ttlCappedAtUncoveredFrontier</c>): the release was published while
/// the leaf held no row there, so every write it owns there afterwards is stamped
/// above that frontier. <c>afterWalReset</c> starts from a WAL reset that kept the
/// snapshot, the shape of the proven-empty release (issue #3103).
/// </para>
/// </summary>
public sealed class WalPartitionReleaseModel : ICoyoteModel
{
    private const int Partitions = 2;
    private const int WritesPerPartition = 2;
    private const int Steps = 24;

    private readonly bool _releaseOnlyAfterReplay;
    private readonly int _stopBudget;
    private readonly bool _ttl;
    private readonly bool _ttlCappedAtUncoveredFrontier;
    private readonly bool _afterWalReset;
    private readonly bool _overrideStampedWrites;
    private readonly bool _overrideHold;
    private readonly OverrideHoldClear _overrideHoldClear;
    private readonly OverrideHoldTrigger _overrideHoldTrigger;
    private readonly GcReadOrder _gcReadOrder;
    private readonly bool _saturatedMerges;

    /// <summary>Creates the model.</summary>
    /// <param name="releaseOnlyAfterReplay">Whether the empty release waits for the partition's replay.</param>
    /// <param name="stopBudget">How many times the leaf may stop.</param>
    /// <param name="ttl">Whether a retention TTL trims too, admitting every entry by age.</param>
    /// <param name="ttlCappedAtUncoveredFrontier">
    /// Whether the partition's TTL ceiling is capped at the lowest frontier of the
    /// pins on it the offset floor does not cover (issue #4622). When
    /// <see langword="false"/> the TTL arm yields only to a Zero pin with no offset.
    /// </param>
    /// <param name="afterWalReset">
    /// Whether the run starts just after a WAL reset that preserved the snapshot
    /// (issue #3103): partition 0 holds rows rehydrated from the snapshot, has
    /// never checkpointed, and its WAL is empty.
    /// </param>
    /// <param name="overrideStampedWrites">
    /// Whether the leaf also accepts writes stamped with a source HLC at or below
    /// its clock (<c>LatticeHlcOverrideContext</c>: replication apply, a range
    /// delete's pinned issue stamp, resize, snapshot, reshard and split copies),
    /// which merge the clock rather than advance it past the stamp (issue #4641).
    /// </param>
    /// <param name="overrideHold">
    /// Whether the leaf raises the override hold at all (issue #4641). Every GC arm
    /// treats a held, uncovered consumer like a block.
    /// </param>
    /// <param name="overrideHoldClear">When a raised hold is cleared.</param>
    /// <param name="overrideHoldTrigger">Which override writes raise the hold.</param>
    /// <param name="gcReadOrder">
    /// Whether a GC pass reads the pin store atomically, or in separate steps in
    /// the given order with leaf steps interleaving between them.
    /// </param>
    /// <param name="saturatedMerges">
    /// Whether an override merge may saturate at the HLC counter ceiling, leaving
    /// the clock equal to the carried stamp instead of strictly past it.
    /// </param>
    public WalPartitionReleaseModel(
        bool releaseOnlyAfterReplay,
        int stopBudget = 1,
        bool ttl = false,
        bool ttlCappedAtUncoveredFrontier = true,
        bool afterWalReset = false,
        bool overrideStampedWrites = false,
        bool overrideHold = true,
        OverrideHoldClear overrideHoldClear = OverrideHoldClear.StoreOnFirstRealOffset,
        OverrideHoldTrigger overrideHoldTrigger = OverrideHoldTrigger.StampBelowClockOrNotTicked,
        GcReadOrder gcReadOrder = GcReadOrder.HeadHoldsCensus,
        bool saturatedMerges = false)
    {
        _overrideHoldTrigger = overrideHoldTrigger;
        _gcReadOrder = gcReadOrder;
        _saturatedMerges = saturatedMerges;
        _overrideHoldClear = overrideHoldClear;
        _overrideStampedWrites = overrideStampedWrites;
        _overrideHold = overrideHold;
        _ttl = ttl;
        _ttlCappedAtUncoveredFrontier = ttlCappedAtUncoveredFrontier;
        _afterWalReset = afterWalReset;
        ArgumentOutOfRangeException.ThrowIfNegative(stopBudget);
        _releaseOnlyAfterReplay = releaseOnlyAfterReplay;
        _stopBudget = stopBudget;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var s = new State(_stopBudget);
        if (_afterWalReset)
        {
            // The rows the snapshot preserved belong to no surviving WAL offset;
            // they are durable in the snapshot and live in the projection.
            s.HasSnapshot = true;
            s.ResetRows[0] = true;
            s.Hlc = s.Clock = s.PersistedClock = 5;
        }

        for (var step = 0; step < Steps; step++)
        {
            var enabled = Enabled(s);
            if (enabled.Count == 0)
            {
                break;
            }

            Apply(s, enabled[runtime.RandomInteger(enabled.Count)]);
            CheckSafety(s);
        }
    }

    private enum Kind { Write, OverrideWrite, SaturatedOverrideWrite, Read, Persist, Capture, Publish, Trim, TtlTrim, GcStep, Stop, Activate }

    private readonly record struct Act(Kind Kind, int Partition);

    private sealed class State
    {
        public State(int stops)
        {
            Stops = stops;
            for (var p = 0; p < Partitions; p++)
            {
                Durable[p] = new bool[WritesPerPartition];
                Acked[p] = new bool[WritesPerPartition];
                Stamp[p] = new long[WritesPerPartition];
                Cache[p] = new bool[WritesPerPartition];
                SnapRows[p] = new bool[WritesPerPartition];
                Rp[p] = StCp[p] = SnapCov[p] = Cov[p] = PinOff[p] = -1;
            }
        }

        public long Hlc;
        public int Stops;
        public readonly long[] Next = new long[Partitions];
        public readonly long[] Tail = new long[Partitions];
        public readonly bool[][] Durable = new bool[Partitions][];
        public readonly bool[][] Acked = new bool[Partitions][];
        public readonly long[][] Stamp = new long[Partitions][];

        public bool Up = true;
        public bool Stale;
        public long Clock;
        public long PersistedClock;
        public readonly bool[][] Cache = new bool[Partitions][];
        public readonly long[] Rp = new long[Partitions];
        public readonly long[] StCp = new long[Partitions];
        public readonly long[] Cov = new long[Partitions];
        public readonly bool[] Replayed = new bool[Partitions];
        public bool HasSnapshot;
        public readonly bool[] ResetRows = new bool[Partitions];
        public readonly long[] SnapCov = new long[Partitions];
        public readonly bool[][] SnapRows = new bool[Partitions][];

        // The durable pin store, merged by monotone max: one pin per partition.
        public readonly long[] PinOff = new long[Partitions];
        public readonly long[] PinFrontier = new long[Partitions];

        // The durable override hold per partition (issue #4641). A flag, not
        // max-merged with the pin: the leaf clears it.
        public readonly bool[] OverrideHold = new bool[Partitions];

        // Whether this activation has already raised (or found covered) each
        // partition's hold: the leaf raises at most once per partition per
        // activation.
        public readonly bool[] RaisedThisActivation = new bool[Partitions];

        // A GC pass in flight per partition when the pin store is read in steps:
        // how many reads it has made, whether it trims under the TTL ceiling, the
        // head bound it read, and the hold it read.
        public readonly int[] GcPhase = new int[Partitions];
        public readonly bool[] GcTtl = new bool[Partitions];
        public readonly long[] GcHead = new long[Partitions];
        public readonly bool[] GcHeld = new bool[Partitions];
    }

    private List<Act> Enabled(State s)
    {
        var acts = new List<Act>();
        for (var p = 0; p < Partitions; p++)
        {
            if (s.Up && !s.Stale)
            {
                if (s.Next[p] < WritesPerPartition)
                {
                    acts.Add(new Act(Kind.Write, p));
                    if (_overrideStampedWrites && s.Clock > 0)
                    {
                        acts.Add(new Act(Kind.OverrideWrite, p));
                        if (_saturatedMerges)
                        {
                            acts.Add(new Act(Kind.SaturatedOverrideWrite, p));
                        }
                    }
                }

                if (ReadFrom(s, p) < s.Next[p])
                {
                    acts.Add(new Act(Kind.Read, p));
                }

                if (s.Rp[p] > s.StCp[p])
                {
                    acts.Add(new Act(Kind.Persist, p));
                }

                acts.Add(new Act(Kind.Publish, p));
            }

            if (_gcReadOrder == GcReadOrder.Atomic || s.GcPhase[p] == 0)
            {
                acts.Add(new Act(Kind.Trim, p));
                if (_ttl)
                {
                    acts.Add(new Act(Kind.TtlTrim, p));
                }
            }
            else
            {
                acts.Add(new Act(Kind.GcStep, p));
            }
        }

        if (s.Up && !s.Stale)
        {
            acts.Add(new Act(Kind.Capture, 0));
            if (s.Stops > 0)
            {
                acts.Add(new Act(Kind.Stop, 0));
            }
        }
        else if (!s.Up && !s.Stale)
        {
            acts.Add(new Act(Kind.Activate, 0));
        }

        return acts;
    }

    /// <summary>When a raised override hold is cleared.</summary>
    public enum OverrideHoldClear
    {
        /// <summary>
        /// The landed design (#4679): the leaf never clears a hold; the pin store
        /// drops it in the same write that lands the consumer's first real offset
        /// (<c>WalMaterialiserPinState.OverrideHolds</c>).
        /// </summary>
        StoreOnFirstRealOffset,

        /// <summary>A perturbation: the store drops the hold on any publish, the empty release included.</summary>
        StoreOnAnyPublish,

        /// <summary>A perturbation: the leaf clears the hold when a checkpoint persists, whether or not a snapshot covers it.</summary>
        OnPersistedCheckpoint,
    }

    /// <summary>Which records raise the override hold.</summary>
    public enum OverrideHoldTrigger
    {
        /// <summary>
        /// The landed design (<c>BPlusLeafGrain.NeedsOverrideHold</c>): a record
        /// stamped strictly below the leaf's clock, or any record the leaf did not
        /// tick itself.
        /// </summary>
        StampBelowClockOrNotTicked,

        /// <summary>
        /// A perturbation: only a record stamped strictly below the clock. Misses a
        /// merge saturated at the HLC counter ceiling, which leaves the stamp at the clock.
        /// </summary>
        StampBelowClockOnly,
    }

    /// <summary>How a GC pass reads the pin store relative to the leaf's steps.</summary>
    public enum GcReadOrder
    {
        /// <summary>Head, holds and pins in one atomic step.</summary>
        Atomic,

        /// <summary>The issue #4641 design: the head bound, then the holds, then the census and pins, each its own step.</summary>
        HeadHoldsCensus,

        /// <summary>The holds before the head bound: a hold raised between them is missed by a pass whose bound covers its write.</summary>
        HoldsHeadCensus,
    }

    private static long ReadFrom(State s, int p) => s.Rp[p] + 1 < s.Tail[p] ? s.Tail[p] : s.Rp[p] + 1;

    private void Apply(State s, Act act)
    {
        var p = act.Partition;
        switch (act.Kind)
        {
            case Kind.Write:
            {
                // The leaf accepts the write, stamps it above its clock, applies it
                // in the foreground and acknowledges it once the WAL holds it.
                var o = s.Next[p]++;
                s.Hlc = Math.Max(s.Hlc, s.Clock) + 1;
                s.Stamp[p][o] = s.Hlc;
                s.Durable[p][o] = true;
                s.Acked[p][o] = true;
                s.Cache[p][o] = true;
                s.Clock = s.Hlc;
                break;
            }

            case Kind.OverrideWrite:
            case Kind.SaturatedOverrideWrite:
            {
                // AdvanceClockOrOverride under an override: the entry carries the
                // source stamp and the clock is merged with it. HybridLogicalClock.Merge
                // is an HLC receive, strictly past both inputs, except at the counter
                // ceiling, where it saturates and leaves the clock equal to a stamp
                // that shares its wall clock.
                var o = s.Next[p]++;
                var saturated = act.Kind == Kind.SaturatedOverrideWrite;
                s.Stamp[p][o] = saturated ? s.Clock : 1;
                if (!saturated)
                {
                    s.Hlc = Math.Max(s.Hlc, s.Clock) + 1;
                    s.Clock = s.Hlc;
                }

                RaiseOverrideHold(s, p, s.Stamp[p][o]);
                s.Durable[p][o] = true;
                s.Acked[p][o] = true;
                s.Cache[p][o] = true;
                break;
            }

            case Kind.Read:
            {
                var o = ReadFrom(s, p);
                if (s.Durable[p][o])
                {
                    s.Cache[p][o] = true;
                    s.Clock = Math.Max(s.Clock, s.Stamp[p][o]);
                }

                s.Rp[p] = o;
                if (o == s.Next[p] - 1)
                {
                    s.Replayed[p] = true;
                }

                break;
            }

            case Kind.Persist:
                s.StCp[p] = s.Rp[p];
                if (_overrideHoldClear == OverrideHoldClear.OnPersistedCheckpoint)
                {
                    s.OverrideHold[p] = false;
                }

                s.PersistedClock = Math.Max(s.PersistedClock, s.Clock);
                break;

            case Kind.Capture:
                // A capture claims each partition's read position; one that covers
                // nothing anywhere is declined (#2725).
                if (Enumerable.Range(0, Partitions).Any(q => s.Rp[q] >= 0))
                {
                    s.HasSnapshot = true;
                    for (var q = 0; q < Partitions; q++)
                    {
                        s.SnapCov[q] = s.Rp[q];
                        s.Cov[q] = s.Rp[q];
                        Array.Copy(s.Cache[q], s.SnapRows[q], WritesPerPartition);
                    }
                }

                break;

            case Kind.Publish:
                Publish(s, p);
                break;

            case Kind.Trim:
                Trim(s, p, ttl: false);
                break;

            case Kind.TtlTrim:
                Trim(s, p, ttl: true);
                break;

            case Kind.GcStep:
                GcStep(s, p);
                break;

            case Kind.Stop:
                s.Stops--;
                s.Up = false;
                s.Clock = s.PersistedClock;
                for (var q = 0; q < Partitions; q++)
                {
                    Array.Clear(s.Cache[q]);
                    s.Rp[q] = -1;
                    s.Cov[q] = -1;
                    s.Replayed[q] = false;
                    s.RaisedThisActivation[q] = false;
                }

                break;

            case Kind.Activate:
                Activate(s);
                break;
        }
    }

    private void Publish(State s, int p)
    {
        var current = Math.Max(s.Rp[p], s.StCp[p]);
        var hasLiveData = Array.IndexOf(s.Cache[p], true) >= 0 || s.ResetRows[p];
        var walProvenEmpty = s.Next[p] == 0;
        var decision = LeafDurablePinCore.Resolve(
            currentCheckpoint: current,
            persistedCheckpoint: s.StCp[p],
            coveredOffset: s.Cov[p],
            hasLiveData: hasLiveData,
            walProvenEmpty: walProvenEmpty,
            releaseNeverWrittenScannedThrough: false);

        if (_releaseOnlyAfterReplay
            && decision.Kind == LeafDurablePinKind.ReleaseEmpty
            && !s.Replayed[p]
            && !walProvenEmpty)
        {
            decision = new LeafDurablePinDecision(LeafDurablePinKind.Block, -1L);
        }

        if (decision.Offset > s.PinOff[p])
        {
            s.PinOff[p] = decision.Offset;
        }

        if (_overrideHoldClear == OverrideHoldClear.StoreOnFirstRealOffset)
        {
            // WalMaterialiserPinState: the slot write that lands the consumer's
            // first real offset drops its hold.
            if (decision.Offset >= 0)
            {
                s.OverrideHold[p] = false;
            }
        }
        else if (_overrideHoldClear == OverrideHoldClear.StoreOnAnyPublish)
        {
            s.OverrideHold[p] = false;
        }

        if (!decision.HasZeroFrontier)
        {
            s.PinFrontier[p] = Math.Max(s.PinFrontier[p], s.Clock);
        }
    }

    private void RaiseOverrideHold(State s, int p, long stamp)
    {
        if (!_overrideHold)
        {
            return;
        }

        // BPlusLeafGrain.RaiseOverrideHoldsForAsync: before the append, an awaited
        // write-through raise, at most once per partition per activation, and a
        // no-op for a consumer whose stored offset is already real. Every write
        // this model takes under an override is one the leaf did not tick.
        var triggered = _overrideHoldTrigger == OverrideHoldTrigger.StampBelowClockOrNotTicked || stamp < s.Clock;
        if (triggered && !s.RaisedThisActivation[p])
        {
            s.RaisedThisActivation[p] = true;
            if (s.PinOff[p] < 0)
            {
                s.OverrideHold[p] = true;
            }
        }
    }

    private void Trim(State s, int p, bool ttl)
    {
        if (_gcReadOrder == GcReadOrder.Atomic)
        {
            TrimWith(s, p, ttl, held: s.OverrideHold[p], head: s.Next[p]);
            return;
        }

        // LatticeWalGc.RunOnceAsync reads the pin store in separate steps;
        // this starts a pass with its first read.
        s.GcTtl[p] = ttl;
        GcStep(s, p);
    }

    private void GcStep(State s, int p)
    {
        var phase = ++s.GcPhase[p];
        var headFirst = _gcReadOrder == GcReadOrder.HeadHoldsCensus;
        if (phase == (headFirst ? 1 : 2))
        {
            s.GcHead[p] = s.Next[p];
        }

        if (phase == (headFirst ? 2 : 1))
        {
            s.GcHeld[p] = s.OverrideHold[p];
        }

        if (phase == 3)
        {
            s.GcPhase[p] = 0;
            TrimWith(s, p, s.GcTtl[p], held: s.GcHeld[p], head: s.GcHead[p]);
        }
    }

    private void TrimWith(State s, int p, bool ttl, bool held, long head)
    {
        // LatticeWalGc: a Zero pin with no offset holds its partition against
        // every arm, the retention ceiling included (#4622).
        if (s.PinFrontier[p] == 0 && s.PinOff[p] < 0)
        {
            return;
        }

        // A raised override hold stops every arm on its partition (issue #4641).
        if (held)
        {
            return;
        }

        long? treeFloor = null;
        long? partitionFloor = null;
        long cursor = long.MaxValue;
        long uncovered = long.MaxValue;
        for (var q = 0; q < Partitions; q++)
        {
            cursor = Math.Min(cursor, s.PinFrontier[q]);
            if (s.PinOff[q] >= 0)
            {
                treeFloor = treeFloor is { } t ? Math.Min(t, s.PinOff[q]) : s.PinOff[q];
                if (q == p)
                {
                    partitionFloor = s.PinOff[q];
                }
            }
            else
            {
                uncovered = Math.Min(uncovered, s.PinFrontier[q]);
            }
        }

        var floor = partitionFloor ?? treeFloor;
        var admission = floor is { } f
            ? new Orleans.Lattice.WalGcOffsetAdmission(
                f, uncovered == long.MaxValue ? null : new HybridLogicalClock { WallClockTicks = uncovered })
            : (Orleans.Lattice.WalGcOffsetAdmission?)null;
        var minCursor = new HybridLogicalClock { WallClockTicks = cursor };

        var tail = s.Tail[p];
        while (tail < head
            && (floor is not { } stop || tail <= stop)
            && Orleans.Lattice.WalGcTrimCore.IsEntryEligible(
                new HybridLogicalClock { WallClockTicks = s.Durable[p][tail] ? s.Stamp[p][tail] : 0 },
                entryVectorClock: null,
                tail,
                minCursor: minCursor,
                ttlCeiling: ttl ? new HybridLogicalClock { WallClockTicks = TtlCeiling(s, p) } : null,
                causalStable: null,
                blockedFloor: null,
                offsetAdmission: admission))
        {
            s.Durable[p][tail] = false;
            tail++;
        }

        s.Tail[p] = tail;
    }

    // The retention ceiling admits every entry by age, capped, under the
    // per-partition cap, at the frontier of an uncovered pin on p.
    private long TtlCeiling(State s, int p)
        => _ttlCappedAtUncoveredFrontier && s.PinOff[p] < 0 ? s.PinFrontier[p] : long.MaxValue;


    private static void Activate(State s)
    {
        for (var q = 0; q < Partitions; q++)
        {
            if (s.HasSnapshot)
            {
                Array.Copy(s.SnapRows[q], s.Cache[q], WritesPerPartition);
                s.Rp[q] = s.StCp[q] = s.Cov[q] = s.SnapCov[q];
            }

            var checkpoint = s.HasSnapshot ? s.SnapCov[q] : s.StCp[q];
            if (WalFallOffCore.IsPrefixLost(checkpoint, s.Tail[q]))
            {
                s.Stale = true;
                return;
            }

            s.Replayed[q] = s.Next[q] == 0;
        }

        s.Up = true;
    }

    private static void CheckSafety(State s)
    {
        for (var p = 0; p < Partitions; p++)
        {
            for (var o = 0; o < WritesPerPartition; o++)
            {
                if (!s.Acked[p][o])
                {
                    continue;
                }

                var recoverable = (s.HasSnapshot && s.SnapRows[p][o])
                    || (s.Durable[p][o] && o >= s.Tail[p]);
                Specification.Assert(
                    recoverable,
                    $"[AckedWriteDurable] acked write {o} of partition {p} is in neither the snapshot nor the readable WAL.");
            }
        }
    }
}
