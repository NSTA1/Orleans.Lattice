using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Whether a <see cref="MovedAwaySealInheritanceModel"/> run seeds a freshly minted
/// split sibling with the donor's moved-away seal, mirroring the two shapes
/// <c>BPlusLeafGrain.CompleteSplitAsync</c> can take.
/// </summary>
public enum SealInheritanceMode
{
    /// <summary>
    /// The shipping pre-fix shape: <c>SiblingInitialization</c> seeds the tree id,
    /// shard index, key range and sibling pointers, and nothing else. A sibling minted
    /// from a sealed donor is therefore born unsealed while holding the donor's rows.
    /// </summary>
    NotInherited,

    /// <summary>
    /// The fixed shape: the donor's seal rides the same initialization round-trip and
    /// the sibling unions it into its own through the real
    /// <see cref="MovedAwaySealInheritance.TryInherit"/> before any migrated row
    /// becomes visible.
    /// </summary>
    Inherited,

    /// <summary>
    /// <see cref="Inherited"/>, but the safety property is replaced by a reachability
    /// witness asserting the dangerous ordering is never reached. Exploration is
    /// expected to violate it, which is what proves the fixed arm's silence is earned
    /// rather than an artefact of a scheduler that never places the seal before the
    /// split. It asserts nothing about safety.
    /// </summary>
    InheritedOrderingProbe,
}

/// <summary>
/// Where the donor's sealed row sits relative to the pivot it is divided at.
/// </summary>
public enum SealedRowShape
{
    /// <summary>
    /// The harmful shape: the sealed row sits at or above the pivot, so the division
    /// hands it to the sibling. A sibling born without the seal serves it.
    /// </summary>
    SealedKeyAbovePivot,

    /// <summary>
    /// The no-regression shape: the sealed row sits below the pivot, so it stays on
    /// the donor, which keeps its own seal either way. Inheritance must be a pure
    /// addition here - it must not disturb a division that was already correct.
    /// </summary>
    SealedKeyBelowPivot,

    /// <summary>
    /// The degenerate shape: nothing is ever sealed. Inheritance must be a complete
    /// no-op, and in particular must not fabricate a seal on the sibling, which would
    /// hide live rows rather than orphans.
    /// </summary>
    NoSeal,
}

/// <summary>
/// A Coyote concurrency model of <b>moved-away seal inheritance</b>: the rule that a
/// leaf minted by dividing a sealed leaf must be born carrying that leaf's seal.
/// <para>
/// The defect this model encodes (issue 3121) is <b>structural rather than a window
/// that closes</b>, and the model is shaped to say exactly that. The moved-away seal
/// is keyed by the key's HASH rather than by the leaf's declared range, so a sealed
/// slot is a residue class scattered across the whole keyspace; any non-trivial
/// division of a sealed donor therefore hands the sibling part of a sealed residue
/// class along with the rows, and there is no pivot that avoids it. What the
/// <em>concurrency</em> dimension decides is only whether the shard-side walk that
/// takes the seal runs before or after the division: seal-then-split leaves the
/// sibling permanently unsealed, while split-then-seal happens to catch both halves
/// because the walk finds both. The deterministic content of the rule is covered by
/// <c>MovedAwaySealInheritanceTests</c>; this fixture's job is to prove that
/// <em>no</em> ordering escapes it.
/// </para>
/// <para>
/// <b>Why the unsealed half is permanently wrong rather than briefly stale.</b> The
/// authoritative value for a migrated slot lives on the destination shard, and writes
/// for that slot route there. So the sibling's copy is not merely old - nothing will
/// ever correct it, because no write can reach it. The seal exists precisely to stop
/// a leaf serving such a copy, and a sibling that never receives it serves the orphan
/// through every read path for the rest of its life.
/// </para>
/// <para>
/// <b>Atomicity granularity.</b> The seal, the division, and the destination's
/// authoritative write are three independently schedulable steps that fire exactly
/// once each in a runtime-chosen order, so all six permutations are reachable. This
/// is the property the model lives or dies by: only the orderings placing the seal
/// before the split mint an unsealed sibling, so a model that fused those steps would
/// report a proof while being blind to the bug.
/// </para>
/// <para>
/// <b>Both halves execute production code.</b> The slot a key falls in is
/// <see cref="ShardMap.GetVirtualSlot"/>, the seal test is the sorted-membership
/// probe <c>BPlusLeafGrain.IsKeyMovedAway</c> performs on the result, routing is the
/// real <see cref="SplitBoundary.Owns"/> rule that <c>DeclaresKey</c> uses, and the
/// inheritance decision is the real <see cref="MovedAwaySealInheritance.TryInherit"/>
/// the grain calls on its birth seam. The model supplies the schedule, not the rules.
/// </para>
/// </summary>
public sealed class MovedAwaySealInheritanceModel : ICoyoteModel
{
    /// <summary>The virtual shard count every seal in the model is recorded under.</summary>
    private const int Vsc = 16;

    /// <summary>The donor's inclusive low bound.</summary>
    private const string Low = "k10";

    /// <summary>The key the division is taken at.</summary>
    private const string Pivot = "k20";

    /// <summary>A sealed key at or above the pivot, so the division hands it to the sibling.</summary>
    private const string SealedAbove = "k30";

    /// <summary>A sealed key below the pivot, so it stays with the donor across the division.</summary>
    private const string SealedBelow = "k15";

    /// <summary>The value the source side holds for a migrated key, now an orphan.</summary>
    private const int Orphan = 1;

    /// <summary>The value the destination shard converges on, which the source can never see.</summary>
    private const int Authoritative = 2;

    private const int StepSeal = 0;
    private const int StepSplit = 1;
    private const int StepDestinationWrite = 2;
    private const int StepCount = 3;

    private const int Donor = 0;
    private const int Sibling = 1;
    private const int LeafCount = 2;

    private readonly SealInheritanceMode _mode;
    private readonly SealedRowShape _shape;

    /// <summary>The key whose slot is sealed, or <see langword="null"/> for the degenerate shape.</summary>
    private readonly string? _sealedKey;

    /// <summary>
    /// A key above the pivot that is deliberately NOT in the sealed slot, so the
    /// division always migrates a live row alongside any orphan. It is what catches an
    /// over-broad fix that sealed the sibling wholesale instead of inheriting exactly
    /// the donor's slots.
    /// </summary>
    private readonly string _liveAbove;

    /// <summary>
    /// Creates a model run for one policy and one row shape.
    /// </summary>
    /// <param name="mode">Whether the sibling inherits the donor's seal at birth.</param>
    /// <param name="shape">Where the sealed row sits relative to the pivot.</param>
    public MovedAwaySealInheritanceModel(SealInheritanceMode mode, SealedRowShape shape)
    {
        _mode = mode;
        _shape = shape;
        _sealedKey = shape switch
        {
            SealedRowShape.SealedKeyAbovePivot => SealedAbove,
            SealedRowShape.SealedKeyBelowPivot => SealedBelow,
            _ => null,
        };

        _liveAbove = SelectLiveKeyAbovePivot(_sealedKey);
    }

    /// <inheritdoc/>
    public void Run(ICoyoteRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);

        var leaves = CreateChain();
        var done = new bool[StepCount];
        var sealTaken = false;
        var sealPrecededSplit = false;

        for (var fired = 0; fired < StepCount; fired++)
        {
            var step = ChooseNextStep(runtime, done);
            done[step] = true;

            switch (step)
            {
                case StepSeal:
                    TakeSeal(leaves);
                    sealTaken = true;
                    break;

                case StepSplit:
                    sealPrecededSplit = sealTaken;
                    Split(leaves);
                    break;

                case StepDestinationWrite:
                default:
                    // The destination shard converges on the authoritative value. It
                    // is deliberately not written into this chain: that is the whole
                    // point, since the source side can never observe or be corrected
                    // by it.
                    break;
            }
        }

        if (_mode == SealInheritanceMode.InheritedOrderingProbe)
        {
            AssertSealBeforeSplitUnreachable(sealPrecededSplit);
            return;
        }

        AssertNoUnsealedOrphanIsServed(leaves);
        AssertLiveRowsStillServed(leaves);
    }

    /// <summary>
    /// The initial chain: a single donor declaring <c>[Low, null)</c> holding a live
    /// row above the pivot and, for the sealed shapes, the orphan row whose slot has
    /// migrated. The sibling does not exist until the division mints it.
    /// </summary>
    private Leaf?[] CreateChain()
    {
        var leaves = new Leaf?[LeafCount];
        var donor = new Leaf(Low, null);

        donor.Put(_liveAbove, Orphan);
        if (_sealedKey is not null)
        {
            donor.Put(_sealedKey, Orphan);
        }

        leaves[Donor] = donor;
        return leaves;
    }

    /// <summary>
    /// The shard-side walk that records a migrated slot. It seals every leaf that
    /// exists when it runs, which is exactly why the ordering matters: run after the
    /// division it catches both halves, but run before it there is only one leaf to
    /// find and the sibling is minted afterwards with no seal of its own.
    /// </summary>
    private void TakeSeal(Leaf?[] leaves)
    {
        if (_sealedKey is null)
        {
            return;
        }

        var slot = ShardMap.GetVirtualSlot(_sealedKey, Vsc);
        for (var i = 0; i < leaves.Length; i++)
        {
            leaves[i]?.Seal(slot, Vsc);
        }
    }

    /// <summary>
    /// The division. The sibling is minted over <c>[Pivot, donorHigh)</c>, seeded, and
    /// only then handed the rows at or above the pivot - the ordering the production
    /// split already uses for shadow markers, and the reason the seal is armed before
    /// any migrated row becomes visible.
    /// </summary>
    private void Split(Leaf?[] leaves)
    {
        var donor = leaves[Donor]!;
        var sibling = new Leaf(Pivot, donor.High);

        if (_mode != SealInheritanceMode.NotInherited)
        {
            // The real production core, on the real birth seam.
            if (MovedAwaySealInheritance.TryInherit(
                    sibling.Slots,
                    sibling.Vsc,
                    donor.Slots,
                    donor.Vsc,
                    out var inheritedSlots,
                    out var inheritedVsc))
            {
                sibling.Adopt(inheritedSlots, inheritedVsc);
            }
        }

        leaves[Sibling] = sibling;
        donor.High = Pivot;
        donor.MoveAtOrAbove(Pivot, sibling);
    }

    /// <summary>
    /// Picks the next step to fire from those not yet fired, driving the choice
    /// through the runtime so exploration enumerates every ordering. The last
    /// remaining candidate is taken when no coin comes up, which keeps every run
    /// total - all steps always fire, only their order varies.
    /// </summary>
    private static int ChooseNextStep(ICoyoteRuntime runtime, bool[] done)
    {
        var last = -1;
        for (var step = 0; step < done.Length; step++)
        {
            if (done[step])
            {
                continue;
            }

            last = step;
            if (runtime.RandomBoolean())
            {
                return step;
            }
        }

        return last;
    }

    /// <summary>
    /// The safety property. No leaf may serve a row whose slot has migrated to another
    /// shard, because the authoritative value lives where writes for that slot route
    /// and the copy held here can never be corrected. The seal is the only thing that
    /// stops it being served, so a leaf that holds the row, declares the key, and does
    /// not carry the seal is serving a permanent orphan.
    /// </summary>
    private void AssertNoUnsealedOrphanIsServed(Leaf?[] leaves)
    {
        if (_sealedKey is null)
        {
            return;
        }

        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = leaves[i];
            if (leaf is null || !leaf.Holds(_sealedKey) || !leaf.Declares(_sealedKey))
            {
                continue;
            }

            if (leaf.IsSealedFor(_sealedKey))
            {
                continue;
            }

            Specification.Assert(
                false,
                $"leaf {i} declares [{leaf.Low}, {leaf.High ?? "null"}) and serves migrated key " +
                $"{_sealedKey} as {leaf.Get(_sealedKey)} without the moved-away seal, against an " +
                $"authoritative {Authoritative} on the destination shard that no write can ever " +
                "deliver here (issue 3121)");
        }
    }

    /// <summary>
    /// The converse property, which is what stops a trivially "safe" fix passing. A
    /// row whose slot never migrated must still be served after the division, so a
    /// change that sealed the sibling indiscriminately - hiding live rows rather than
    /// orphans - fails here rather than being reported as a proof.
    /// </summary>
    private void AssertLiveRowsStillServed(Leaf?[] leaves)
    {
        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = leaves[i];
            if (leaf is null || !leaf.Holds(_liveAbove))
            {
                continue;
            }

            Specification.Assert(
                !leaf.IsSealedFor(_liveAbove),
                $"leaf {i} refuses to serve live key {_liveAbove}, whose slot never migrated; " +
                "inheritance must copy exactly the donor's sealed slots, not seal wholesale");
        }
    }

    /// <summary>
    /// The anti-vacuity witness, asserted only under the probe mode. Exploration is
    /// expected to violate it, proving the scheduler really does reach the ordering in
    /// which the seal is taken before the division - the only ordering that mints an
    /// unsealed sibling, and therefore the only one under which the fixed arm's
    /// silence means anything.
    /// </summary>
    private static void AssertSealBeforeSplitUnreachable(bool sealPrecededSplit) =>
        Specification.Assert(
            !sealPrecededSplit,
            "reachability witness: exploration placed the moved-away seal before the division, " +
            "which is the ordering that mints an unsealed sibling");

    /// <summary>
    /// Chooses a key above the pivot that does not share the sealed key's slot, so the
    /// division always carries a live row across alongside any orphan. It is derived
    /// rather than hard-coded because slot membership is a property of
    /// <see cref="ShardMap.GetVirtualSlot"/>, and a hard-coded key would silently
    /// collide - and quietly weaken the model - if that hash ever changed.
    /// </summary>
    private static string SelectLiveKeyAbovePivot(string? sealedKey)
    {
        var sealedSlot = sealedKey is null ? -1 : ShardMap.GetVirtualSlot(sealedKey, Vsc);

        for (var i = 0; i < 1000; i++)
        {
            var candidate = $"k9{i:D4}";
            if (string.CompareOrdinal(candidate, Pivot) > 0
                && ShardMap.GetVirtualSlot(candidate, Vsc) != sealedSlot)
            {
                return candidate;
            }
        }

        throw new InvalidOperationException(
            "no key above the pivot falls outside the sealed slot; the model cannot be built");
    }

    /// <summary>
    /// A minimal leaf: a declared range, a small ordered row set, and a moved-away
    /// seal. A reference type deliberately - a value type would copy on assignment and
    /// silently discard the row and seal mutations the steps perform.
    /// </summary>
    private sealed class Leaf(string? low, string? high)
    {
        private const int Capacity = 4;

        private readonly string[] _keys = new string[Capacity];
        private readonly int[] _values = new int[Capacity];
        private int _count;

        /// <summary>The inclusive low bound of the range this leaf declares.</summary>
        public string? Low { get; } = low;

        /// <summary>The exclusive high bound of the range this leaf declares.</summary>
        public string? High { get; set; } = high;

        /// <summary>The sealed slots, or <see langword="null"/> when this leaf holds no seal.</summary>
        public int[]? Slots { get; private set; }

        /// <summary>The virtual shard count <see cref="Slots"/> was recorded under.</summary>
        public int? Vsc { get; private set; }

        /// <summary>Records a migrated slot, exactly as <c>MarkSlotsMovedAwayAsync</c> would.</summary>
        public void Seal(int slot, int virtualShardCount)
        {
            if (Slots is { } present && Array.IndexOf(present, slot) >= 0)
            {
                return;
            }

            var grown = new int[(Slots?.Length ?? 0) + 1];
            Slots?.CopyTo(grown, 0);
            grown[^1] = slot;
            Array.Sort(grown);
            Slots = grown;
            Vsc = virtualShardCount;
        }

        /// <summary>Takes the seal the inheritance core computed for this leaf.</summary>
        public void Adopt(int[]? slots, int? virtualShardCount)
        {
            Slots = slots;
            Vsc = virtualShardCount;
        }

        /// <summary>
        /// The production seal test: hash the key to its slot under the recorded count
        /// and probe the sorted set, exactly as <c>BPlusLeafGrain.IsKeyMovedAway</c> does.
        /// </summary>
        public bool IsSealedFor(string key)
        {
            if (Slots is not { Length: > 0 } slots || Vsc is not { } vsc)
            {
                return false;
            }

            return Array.BinarySearch(slots, ShardMap.GetVirtualSlot(key, vsc)) >= 0;
        }

        /// <summary>Whether routing would select this leaf for the key.</summary>
        public bool Declares(string key) => SplitBoundary.Owns(key, Low, High);

        /// <summary>Whether this leaf physically holds a row for the key.</summary>
        public bool Holds(string key) => IndexOf(key) >= 0;

        /// <summary>Reads the row this leaf holds for the key.</summary>
        public int Get(string key)
        {
            var at = IndexOf(key);
            return at < 0 ? 0 : _values[at];
        }

        /// <summary>Writes a row, keeping the row set ordered by key.</summary>
        public void Put(string key, int value)
        {
            var at = IndexOf(key);
            if (at >= 0)
            {
                _values[at] = value;
                return;
            }

            var insert = _count;
            for (var i = 0; i < _count; i++)
            {
                if (string.CompareOrdinal(_keys[i], key) > 0)
                {
                    insert = i;
                    break;
                }
            }

            for (var i = _count; i > insert; i--)
            {
                _keys[i] = _keys[i - 1];
                _values[i] = _values[i - 1];
            }

            _keys[insert] = key;
            _values[insert] = value;
            _count++;
        }

        /// <summary>
        /// The unfiltered row transfer the production split performs: every row at or
        /// above the pivot moves, sealed or not. Sealed rows are deliberately carried
        /// rather than dropped, which is why the sibling needs the seal.
        /// </summary>
        public void MoveAtOrAbove(string pivot, Leaf destination)
        {
            ArgumentNullException.ThrowIfNull(destination);

            var kept = 0;
            for (var i = 0; i < _count; i++)
            {
                if (string.CompareOrdinal(_keys[i], pivot) >= 0)
                {
                    destination.Put(_keys[i], _values[i]);
                    continue;
                }

                _keys[kept] = _keys[i];
                _values[kept] = _values[i];
                kept++;
            }

            _count = kept;
        }

        private int IndexOf(string key)
        {
            for (var i = 0; i < _count; i++)
            {
                if (string.Equals(_keys[i], key, StringComparison.Ordinal))
                {
                    return i;
                }
            }

            return -1;
        }
    }
}
