using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// The end-to-end Coyote model of the WAL durability lifecycle - the
/// implementation-level companion of <c>spec/wal/WalDurability.tla</c>. One WAL
/// partition is shared by two leaves; writes are appended, flushed out of order
/// and acknowledged; each leaf reads the shared stream through its read
/// position, folds its own entries into its projection, persists its checkpoint
/// (which can fail), captures snapshots (which can fail), publishes a durable
/// pin, and the GC trims the prefix. Leaves stop at any step and reactivate from
/// their snapshot or cold.
/// <para>
/// Every decision the model takes that production also takes is made by the
/// production core: <see cref="WalOffsetAllocationCore.Assign"/> for offsets,
/// <see cref="WalShippingWatermark"/> for the reader's horizon,
/// <see cref="LeafDurablePinCore.Resolve"/> for the published pin,
/// <see cref="Orleans.Lattice.WalGcTrimCore.IsEntryEligible"/> with a
/// <see cref="Orleans.Lattice.WalGcOffsetAdmission"/> for the trim, and
/// <see cref="WalFallOffCore.IsPrefixLost"/> for the activation's fall-off test.
/// The glue between them is the model's: the GC's min-over-pins floor and its
/// block-pin stop are computed here, not by a production core (production makes
/// them inline in <c>LatticeWalGc</c>), so the trim guard below perturbs model
/// glue and the <c>LatticeWalGc</c> unit tests are what detect those decisions in
/// production. A failed snapshot load fails the activation (issue #4450), a
/// capture claims the read position as its coverage (issue #4451), and no
/// in-activation replay re-arm is modelled (issue #4467); all three match
/// production since those fixes landed.
/// </para>
/// <para>
/// OWNERSHIP. By default the two leaves own alternate entries, so neither stays
/// never-written. With <c>neverWrittenLeaf</c> leaf 0 owns every entry and leaf 1
/// owns none: leaf 1 then reaches the core's never-written release (issues #3453,
/// #4456 and #4523), and <c>[RecoveryNeverFallsOffLog]</c> is what it threatens.
/// <c>[ReleaseBackedBySnapshot]</c>, the TLA+ module's root-cause invariant, is
/// asserted in both ownerships and reports the same defect at the publication,
/// before any trim or restart.
/// </para>
/// <para>
/// OUT OF SCOPE, deliberately: shard crashes and moves (the TLA+ modules, and
/// <c>WalOffsetContiguityModel</c> / <c>WalMoveQuiesceModel</c> /
/// <c>WalMoveRedriveModel</c>, cover them).
/// </para>
/// </summary>
public sealed class WalDurabilityLifecycleModel : ICoyoteModel
{
    private const int Leaves = 2;
    private const int Writes = 3;
    private const int Steps = 26;

    /// <summary>Alternating ownership: each leaf's entries are sparse in the shared stream.</summary>
    private static readonly int[] AlternatingOwner = [0, 1, 0];

    /// <summary>Leaf 0 owns every entry; leaf 1 owns none and stays never-written.</summary>
    private static readonly int[] OneOwner = [0, 0, 0];

    private readonly WalDurabilityLifecycleGuard _guard;
    private readonly int _faultBudget;
    private readonly int[] _owner;
    private readonly bool _neverWrittenVariant;
    private readonly bool _checkReleaseBacking;

    /// <summary>Creates the model with the given guard and fault budget.</summary>
    /// <param name="guard">The fix to remove, or <see cref="WalDurabilityLifecycleGuard.None"/>.</param>
    /// <param name="faultBudget">
    /// Leaf stops, failed persists, failed captures and failed loads together;
    /// the bound that makes the closing bounded-progress check meaningful.
    /// </param>
    /// <param name="neverWrittenLeaf">
    /// When <see langword="true"/>, leaf 1 owns no entry and stays never-written,
    /// so the core's never-written release is exercised.
    /// </param>
    /// <param name="checkReleaseBacking">
    /// When <see langword="false"/>, <c>[ReleaseBackedBySnapshot]</c> is not
    /// asserted. It is the root-cause form of <c>[RecoveryNeverFallsOffLog]</c>'s
    /// never-written violation and reports it first, at the publication itself;
    /// turning it off is how a test proves the outcome assertion catches the same
    /// defect on its own, after the trim and the restart.
    /// </param>
    public WalDurabilityLifecycleModel(
        WalDurabilityLifecycleGuard guard,
        int faultBudget = 2,
        bool neverWrittenLeaf = false,
        bool checkReleaseBacking = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(faultBudget);
        _guard = guard;
        _faultBudget = faultBudget;
        _owner = neverWrittenLeaf ? OneOwner : AlternatingOwner;
        _neverWrittenVariant = neverWrittenLeaf;
        _checkReleaseBacking = checkReleaseBacking;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // All per-iteration state is local: the engine reuses this instance.
        var s = new State(_faultBudget);

        // The never-written variant is about the never-written leaf's lifecycle,
        // not the append path or the data owner: every write is appended, flushed
        // and acknowledged before any leaf acts, leaf 0 (which owns them all) is
        // brought forward only by Settle, and a stopped leaf restarts only once the
        // random phase is over and the GC has had a pass (see Settle). The
        // exploration is spent on leaf 1's reads, persists, captures, pins and
        // stop. Its only fault is a leaf stop: on a leaf that owns nothing, and
        // with leaf 0 out of the random phase, a failed persist (rolled back), a
        // failed capture (no coverage recorded) and a failed load (the activation
        // fails closed) change no state but the fault budget, so offering them
        // would only spend the budget and dilute the search. The default variant
        // keeps every step and every fault in the random phase.
        if (_neverWrittenVariant)
        {
            while (s.Next < Writes)
            {
                var next = s.Next;
                var offset = WalOffsetAllocationCore.Assign(ref next);
                s.Next = next;
                s.Durable[offset] = true;
                s.Acked[offset] = true;
            }
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

        Settle(s);
        CheckSafety(s);

        // Bounded progress: faults are exhausted or stopped, every pending step
        // has been driven, so every acknowledged write is materialised by its
        // owner and the stream is fully reclaimed.
        for (var o = 0; o < Writes; o++)
        {
            if (s.Acked[o])
            {
                Specification.Assert(
                    s.Up[_owner[o]] && s.Cache[_owner[o]][o],
                    $"[EveryAckedWriteMaterialised] acked write {o} is not materialised by leaf {_owner[o]} at quiescence.");
            }
        }

        Specification.Assert(
            s.Tail == s.Next,
            $"[ReclamationEventuallyAdvances] the GC stopped at tail {s.Tail} below the head {s.Next} at quiescence.");
    }

    private enum Kind { Append, FlushAck, FlushAckApply, Read, Persist, PersistFail, Capture, CaptureFail, Publish, Trim, Stop, Activate, LoadFail }

    private readonly record struct Act(Kind Kind, int Arg);

    private sealed class State
    {
        public State(int faults)
        {
            Faults = faults;
            for (var l = 0; l < Leaves; l++)
            {
                Up[l] = true;
                Cache[l] = new bool[Writes];
                SnapRows[l] = new bool[Writes];
                Rp[l] = StCp[l] = DurCp[l] = Anchor[l] = -1;
                Cov[l] = -1;
                SnapCov[l] = -1;
                PinOff[l] = -1;
                PinZero[l] = true; // born holding the seeded Zero block pin
            }
        }

        public long Next;
        public long Tail;
        public readonly bool[] Inflight = new bool[Writes];
        public readonly bool[] Durable = new bool[Writes];
        public readonly bool[] Acked = new bool[Writes];
        public int Faults;

        public readonly bool[] Up = new bool[Leaves];
        public readonly bool[][] Cache = new bool[Leaves][];
        public readonly long[] Rp = new long[Leaves];
        public readonly long[] StCp = new long[Leaves];
        public readonly long[] DurCp = new long[Leaves];
        public readonly long[] Anchor = new long[Leaves];
        public readonly bool[] ClockPersisted = new bool[Leaves];
        public readonly long[] Cov = new long[Leaves];
        public readonly bool[] HasSnapshot = new bool[Leaves];
        public readonly long[] SnapCov = new long[Leaves];
        public readonly bool[][] SnapRows = new bool[Leaves][];
        public readonly bool[] Stale = new bool[Leaves];
        public readonly long[] PinOff = new long[Leaves];
        public readonly bool[] PinZero = new bool[Leaves];
    }

    private List<Act> Enabled(State s)
    {
        var acts = new List<Act>();
        if (s.Next < Writes)
        {
            acts.Add(new Act(Kind.Append, 0));
        }

        for (var o = 0; o < Writes; o++)
        {
            if (s.Inflight[o])
            {
                acts.Add(new Act(Kind.FlushAck, o));
                acts.Add(new Act(Kind.FlushAckApply, o));
            }
        }

        for (var l = 0; l < Leaves; l++)
        {
            // In the never-written variant leaf 0 is the caught-up data owner,
            // driven only by Settle; the exploration is leaf 1's and the GC's.
            if (s.Stale[l] || (_neverWrittenVariant && l == 0))
            {
                continue;
            }

            if (s.Up[l])
            {
                if (CanRead(s, l))
                {
                    acts.Add(new Act(Kind.Read, l));
                }

                if (s.Rp[l] > s.StCp[l])
                {
                    acts.Add(new Act(Kind.Persist, l));
                    if (s.Faults > 0 && !_neverWrittenVariant)
                    {
                        acts.Add(new Act(Kind.PersistFail, l));
                    }
                }

                if (Cur(s, l) >= 0 && s.Rp[l] >= 0 && (!s.HasSnapshot[l] || s.Rp[l] >= s.SnapCov[l]))
                {
                    acts.Add(new Act(Kind.Capture, l));
                    if (s.Faults > 0 && !_neverWrittenVariant)
                    {
                        acts.Add(new Act(Kind.CaptureFail, l));
                    }
                }

                acts.Add(new Act(Kind.Publish, l));
                if (s.Faults > 0)
                {
                    acts.Add(new Act(Kind.Stop, l));
                }
            }
            else
            {
                if (!_neverWrittenVariant)
                {
                    acts.Add(new Act(Kind.Activate, l));
                }

                if (s.HasSnapshot[l] && s.Faults > 0 && !_neverWrittenVariant)
                {
                    acts.Add(new Act(Kind.LoadFail, l));
                }
            }
        }

        // In the never-written variant leaf 0 is driven only by Settle, so its
        // seeded Zero block pin stands through the whole random phase and every
        // trim there is a no-op (the GC's block-pin clause): offering it would
        // only dilute the search for leaf 1's lifecycle.
        if (!_neverWrittenVariant)
        {
            acts.Add(new Act(Kind.Trim, 0));
        }

        return acts;
    }

    private void Apply(State s, Act act)
    {
        var l = act.Arg;
        switch (act.Kind)
        {
            case Kind.Append:
            {
                var next = s.Next;
                var offset = WalOffsetAllocationCore.Assign(ref next);
                s.Next = next;
                s.Inflight[offset] = true;
                break;
            }

            case Kind.FlushAck:
            case Kind.FlushAckApply:
            {
                var o = act.Arg;
                s.Inflight[o] = false;
                s.Durable[o] = true;
                s.Acked[o] = true;
                var owner = _owner[o];
                if (act.Kind == Kind.FlushAckApply && s.Up[owner] && !s.Stale[owner])
                {
                    s.Cache[owner][o] = true; // the foreground apply; no read-position advance
                }

                break;
            }

            case Kind.Read:
            {
                var o = ReadFrom(s, l);
                if (s.Durable[o] && _owner[o] == l)
                {
                    s.Cache[l][o] = true;
                }

                s.Rp[l] = o; // a READ position: advances over other leaves' entries too
                break;
            }

            case Kind.Persist:
                s.DurCp[l] = s.Rp[l];
                s.StCp[l] = s.Rp[l];
                s.ClockPersisted[l] |= Any(s.Cache[l]);
                break;

            case Kind.PersistFail:
                s.Faults--;
                if (_guard == WalDurabilityLifecycleGuard.NoRollbackOnFailedPersist)
                {
                    s.StCp[l] = s.Rp[l]; // the guard: the commit is not rolled back
                }

                break;

            case Kind.Capture:
                // Intended design (issue #4451): the claim is the read position,
                // what the projection actually holds.
                s.HasSnapshot[l] = true;
                s.SnapCov[l] = s.Rp[l];
                Array.Copy(s.Cache[l], s.SnapRows[l], Writes);
                s.Cov[l] = s.Rp[l];
                break;

            case Kind.CaptureFail:
                s.Faults--; // coverage is recorded only from a kept capture (#3440)
                break;

            case Kind.Publish:
                Publish(s, l);
                break;

            case Kind.Trim:
                Trim(s);
                break;

            case Kind.Stop:
                s.Faults--;
                s.Up[l] = false;
                Array.Clear(s.Cache[l]);
                s.Rp[l] = -1;
                s.StCp[l] = s.DurCp[l];
                s.Cov[l] = -1;
                break;

            case Kind.Activate:
                Activate(s, l);
                break;

            case Kind.LoadFail:
                // Intended design (issue #4450): a snapshot that exists but fails
                // to load fails the activation closed, whatever the tail reads.
                s.Faults--;
                break;
        }
    }

    private void Publish(State s, int l)
    {
        var clockLive = s.ClockPersisted[l] || Any(s.Cache[l]);
        var hasLiveData = Any(s.Cache[l]);

        // Production publishes nothing from the cursor mirror for a Zero-clock
        // leaf; only the flush paths opt into the never-written release.
        var releaseNeverWritten = !clockLive;
        if (!clockLive && !(s.StCp[l] >= 0 && !hasLiveData))
        {
            return;
        }

        var persisted = _guard == WalDurabilityLifecycleGuard.PinFromPendingCheckpoint
            ? Cur(s, l) // the guard: the pending checkpoint reaches the pin
            : s.StCp[l];

        var decision = LeafDurablePinCore.Resolve(
            currentCheckpoint: Cur(s, l),
            persistedCheckpoint: persisted,
            coveredOffset: _guard == WalDurabilityLifecycleGuard.NeverWrittenReleaseIgnoresCoverage && releaseNeverWritten
                ? Math.Max(s.Cov[l], s.StCp[l]) // the guard: the release ignores the coverage it holds
                : s.Cov[l],
            hasLiveData: hasLiveData,
            // A WAL is never proven empty here: every scenario appends.
            walProvenEmpty: false,
            releaseNeverWrittenScannedThrough: releaseNeverWritten);

        if (decision.Offset > s.PinOff[l])
        {
            Specification.Assert(
                decision.Offset <= s.StCp[l],
                $"[PublishedPinWithinPersistedBelief] leaf {l} published {decision.Offset} above its persisted checkpoint {s.StCp[l]}.");
            s.PinOff[l] = decision.Offset;
        }

        if (!decision.HasZeroFrontier)
        {
            s.PinZero[l] = false;
        }
    }

    private void Trim(State s)
    {
        // A standing block pin stops every trim (the GC's block-pin clause).
        long floor = -1;
        var anyFloor = false;
        for (var l = 0; l < Leaves; l++)
        {
            if (s.PinOff[l] < 0 && s.PinZero[l])
            {
                return;
            }

            if (s.PinOff[l] >= 0)
            {
                floor = !anyFloor
                    ? s.PinOff[l]
                    : _guard == WalDurabilityLifecycleGuard.TrimFloorFromHighestPin
                        ? Math.Max(floor, s.PinOff[l]) // the guard
                        : Math.Min(floor, s.PinOff[l]);
                anyFloor = true;
            }
        }

        if (!anyFloor)
        {
            return;
        }

        // No consumer outside the offset floor here, so the floor is the whole
        // proof (UncoveredCursor null); the HLC cursor axis is not reported.
        var admission = new Orleans.Lattice.WalGcOffsetAdmission(floor, UncoveredCursor: null);
        var tail = s.Tail;
        while (tail < s.Next && !s.Inflight[tail]
            && Orleans.Lattice.WalGcTrimCore.IsEntryEligible(
                new HybridLogicalClock { WallClockTicks = tail + 1 },
                entryVectorClock: null,
                tail,
                minCursor: null,
                ttlCeiling: null,
                causalStable: null,
                blockedFloor: null,
                offsetAdmission: admission))
        {
            s.Durable[tail] = false;
            tail++;
        }

        s.Tail = tail;
    }

    private static void Activate(State s, int l)
    {
        if (s.HasSnapshot[l])
        {
            Array.Copy(s.SnapRows[l], s.Cache[l], Writes);
            s.Rp[l] = s.StCp[l] = s.Anchor[l] = s.Cov[l] = s.SnapCov[l];
            if (WalFallOffCore.IsPrefixLost(s.SnapCov[l], s.Tail))
            {
                s.Stale[l] = true;
                return;
            }
        }
        else
        {
            Array.Clear(s.Cache[l]);
            s.Rp[l] = -1;
            s.StCp[l] = s.Anchor[l] = s.DurCp[l];
            if (WalFallOffCore.IsPrefixLost(s.DurCp[l], s.Tail))
            {
                s.Stale[l] = true;
                return;
            }
        }

        s.Up[l] = true;
    }

    /// <summary>
    /// Drives every pending protocol step to completion, deterministically: the
    /// faults have been spent or stopped, so a correct lifecycle must now
    /// converge.
    /// </summary>
    private void Settle(State s)
    {
        for (var round = 0; round < 12; round++)
        {
            while (s.Next < Writes)
            {
                var next = s.Next;
                s.Inflight[WalOffsetAllocationCore.Assign(ref next)] = true;
                s.Next = next;
            }

            for (var o = 0; o < Writes; o++)
            {
                if (s.Inflight[o])
                {
                    s.Inflight[o] = false;
                    s.Durable[o] = true;
                    s.Acked[o] = true;
                }
            }

            for (var l = 0; l < Leaves; l++)
            {
                if (s.Stale[l] || !s.Up[l])
                {
                    continue;
                }

                while (CanRead(s, l))
                {
                    var o = ReadFrom(s, l);
                    if (s.Durable[o] && _owner[o] == l)
                    {
                        s.Cache[l][o] = true;
                    }

                    s.Rp[l] = o;
                }

                if (s.Rp[l] > s.StCp[l])
                {
                    s.DurCp[l] = s.StCp[l] = s.Rp[l];
                    s.ClockPersisted[l] |= Any(s.Cache[l]);
                }

                if (Cur(s, l) >= 0 && s.Rp[l] >= 0 && (!s.HasSnapshot[l] || s.Rp[l] >= s.SnapCov[l]))
                {
                    s.HasSnapshot[l] = true;
                    s.SnapCov[l] = s.Rp[l];
                    Array.Copy(s.Cache[l], s.SnapRows[l], Writes);
                    s.Cov[l] = s.Rp[l];
                }

                Publish(s, l);
            }

            // The GC runs before a stopped leaf comes back: activation and the
            // trim are concurrent in production, and this order is the one in
            // which a trim through a released pin meets a lower snapshot.
            Trim(s);

            // A stopped leaf comes back only after the GC has had a full pass.
            for (var l = 0; l < Leaves && round > 0; l++)
            {
                if (!s.Stale[l] && !s.Up[l])
                {
                    Activate(s, l);
                }
            }
        }
    }

    private void CheckSafety(State s)
    {
        for (var l = 0; l < Leaves; l++)
        {
            Specification.Assert(
                !s.Stale[l],
                $"[RecoveryNeverFallsOffLog] leaf {l} latched stale: tail {s.Tail}, snapshot {s.SnapCov[l]}, durable checkpoint {s.DurCp[l]}.");

            // Every trim entitlement the pin store holds is backed by durable
            // snapshot coverage (issue #4523): pins and coverage only grow, so a
            // pin above the snapshot is a prefix the next activation may need
            // and the GC may take.
            if (_checkReleaseBacking && s.PinOff[l] >= 0)
            {
                Specification.Assert(
                    s.HasSnapshot[l] && s.SnapCov[l] >= s.PinOff[l],
                    $"[ReleaseBackedBySnapshot] leaf {l} holds pin {s.PinOff[l]} above its snapshot coverage {(s.HasSnapshot[l] ? s.SnapCov[l] : -1)}.");
            }

            if (!s.Up[l])
            {
                continue;
            }

            Specification.Assert(
                s.StCp[l] == s.DurCp[l] || s.StCp[l] == s.Anchor[l],
                $"[PersistedBeliefHonest] leaf {l} believes it persisted {s.StCp[l]} but storage holds {s.DurCp[l]} (anchor {s.Anchor[l]}).");

            for (var o = 0; o < Writes; o++)
            {
                if (s.Inflight[o])
                {
                    Specification.Assert(
                        s.Rp[l] < o,
                        $"[ShippingNeverSkips] leaf {l} read to {s.Rp[l]} past in-flight offset {o}.");
                }

                if (s.Acked[o] && _owner[o] == l && o <= s.Rp[l])
                {
                    Specification.Assert(
                        s.Cache[l][o],
                        $"[ReadPositionHonest] leaf {l} read past its acked write {o} without holding it.");
                }
            }
        }

        for (var o = 0; o < Writes; o++)
        {
            if (!s.Acked[o])
            {
                continue;
            }

            var owner = _owner[o];
            if (o < s.Tail)
            {
                Specification.Assert(
                    s.HasSnapshot[owner] && s.SnapRows[owner][o] && o <= s.SnapCov[owner],
                    $"[TrimCoveredBySnapshot] acked write {o} was trimmed but leaf {owner}'s snapshot does not hold it.");
            }

            var recoverable = s.HasSnapshot[owner]
                ? s.SnapRows[owner][o] || (o > s.SnapCov[owner] && s.Durable[o] && o >= s.Tail)
                : s.Durable[o] && o >= s.Tail;
            Specification.Assert(
                recoverable,
                $"[AckedWriteDurable] acked write {o} is not recoverable by leaf {owner} from durable state.");
        }
    }

    private static long Cur(State s, int l) => Math.Max(s.Rp[l], s.StCp[l]);

    private static long ReadFrom(State s, int l) => s.Rp[l] + 1 < s.Tail ? s.Tail : s.Rp[l] + 1;

    private bool CanRead(State s, int l)
    {
        var offset = ReadFrom(s, l);
        if (_guard == WalDurabilityLifecycleGuard.ReaderIgnoresWatermark)
        {
            return offset < s.Next; // the guard: the allocator's head, not the durable-contiguous tail
        }

        var hasInFlight = false;
        long first = 0;
        for (var o = 0; o < Writes; o++)
        {
            if (s.Inflight[o])
            {
                hasInFlight = true;
                first = o;
                break;
            }
        }

        // WalShippingWatermark: a reader may be shown only offsets strictly below
        // the durable-contiguous tail. The bound by the allocator's head keeps a
        // watermark that over-exposes from indexing past the assigned offsets: an
        // over-exposure of an in-flight offset is then reported by
        // [ShippingNeverSkips], never by an IndexOutOfRangeException.
        return offset < s.Next
            && WalShippingWatermark.IsOffsetExposable(
                offset, WalShippingWatermark.DurableContiguousTail(hasInFlight, first, s.Next));
    }

    private static bool Any(bool[] values) => Array.IndexOf(values, true) >= 0;
}
