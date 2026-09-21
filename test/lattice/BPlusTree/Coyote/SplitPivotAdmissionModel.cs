using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Whether a <see cref="SplitPivotAdmissionModel"/> run validates the pivot it
/// selects against the leaf's declared span, mirroring the two shapes
/// <c>BPlusLeafGrain.SplitAsync</c> can take.
/// </summary>
public enum SplitPivotMode
{
    /// <summary>
    /// The shipping pre-fix shape: the pivot is whatever bisecting the row set
    /// returns, used unexamined. A leaf holding a row outside its own declared span
    /// can therefore be divided at that row, minting a half that declares an empty
    /// range.
    /// </summary>
    Unvalidated,

    /// <summary>
    /// The fixed shape: the pivot is tested with
    /// <see cref="SplitPivotAdmission.IsAdmissible"/>, repaired to
    /// <see cref="SplitPivotAdmission.SelectMedianAdmissible"/> when inadmissible,
    /// and the split declined outright when no row falls strictly inside the span.
    /// </summary>
    Validated,

    /// <summary>
    /// <see cref="Validated"/>, but the safety property is replaced by a
    /// reachability witness asserting the dangerous ordering is never reached.
    /// Exploration is expected to violate it, which is what proves the fixed arm's
    /// silence is earned rather than an artefact of a scheduler that never places
    /// the out-of-span graft before the split. It asserts nothing about safety.
    /// </summary>
    ValidatedOrderingProbe,
}

/// <summary>
/// The row shape the donor leaf carries into its split.
/// </summary>
public enum DonorRowShape
{
    /// <summary>
    /// The harmful shape: the donor holds one row inside its declared span and one
    /// grafted row at its exclusive high bound. Bisecting two rows takes the upper
    /// of the pair, so the pivot is the out-of-span key and the sibling is seeded
    /// with <c>[High, High)</c> - the exact degenerate range observed in the #3117
    /// trace.
    /// </summary>
    OneInSpanOneGrafted,

    /// <summary>
    /// The decline shape: every row the donor holds is out of span, so no pivot can
    /// divide it without emptying a half. The fixed policy must refuse to split at
    /// all rather than pick the least-bad key.
    /// </summary>
    AllGrafted,

    /// <summary>
    /// The no-regression shape: every row is comfortably inside the span, so the
    /// pivot is admissible on selection and the validated policy must divide exactly
    /// as the unvalidated one did. This is what would catch an over-eager guard that
    /// declines or repairs a healthy split.
    /// </summary>
    AllInSpan,
}

/// <summary>
/// A Coyote concurrency model of <b>split pivot admissibility</b>: the rule that a
/// leaf may only be divided at a key strictly inside its own
/// <c>[LowKeyInclusive, HighKeyExclusive)</c> range.
/// <para>
/// The defect this model encodes is structural rather than a window that closes. A
/// division hands <c>[Low, pivot)</c> to the donor and <c>[pivot, High)</c> to the
/// sibling, but the pivot is drawn from the <em>row set</em>, and a leaf's rows are
/// not a subset of its declared span - span admission's forward is deliberately
/// fail-open, and a cross-shard migration grafts rows before the range fixup lands.
/// Bisecting such a leaf returns a key outside the span, and one half is born
/// declaring an empty range.
/// </para>
/// <para>
/// <b>Why that half is worse than useless rather than merely empty.</b> It still
/// holds rows and it still answers reads, because the read path is custody-agnostic
/// by design. But no routing descent can ever select it, since every descent tests
/// the key against exactly that range. So it can never be written, never be chosen
/// as a forward target, and never be drained. A reader served by it observes a
/// value that no writer can correct - permanently. That asymmetry between the
/// <em>read</em> rule (a leaf serves what it holds) and the <em>write</em> rule (a
/// leaf receives what it declares) is the whole defect, and both rules here are the
/// production predicates rather than restatements of them.
/// </para>
/// <para>
/// <b>Atomicity granularity.</b> The out-of-span graft, the authoritative write,
/// and the split are three independently schedulable steps that fire exactly once
/// each in a runtime-chosen order, so all six permutations are reachable. This is
/// the property the model lives or dies by: only the orderings placing the graft
/// before the split give the bisect an out-of-span key to return, so a model that
/// fused those steps would report a proof while being blind to the bug.
/// </para>
/// <para>
/// The pivot decision executes the real <see cref="SplitPivotAdmission"/> core -
/// the same calls <c>BPlusLeafGrain.IsAdmissibleSplitPivot</c> and
/// <c>TryFindAdmissibleSplitPivot</c> make - so the half of the model that the fix
/// lives in cannot drift from production. Write routing executes the real
/// <see cref="SplitBoundary.Owns"/> rule, the same call <c>DeclaresKey</c>,
/// <c>ShouldApplyDuringReplay</c> and <c>TryResolveSpanForwardTarget</c> make.
/// </para>
/// </summary>
public sealed class SplitPivotAdmissionModel : ICoyoteModel
{
    /// <summary>The donor's inclusive low bound.</summary>
    private const string Low = "k10";

    /// <summary>A key strictly inside the donor's declared span.</summary>
    private const string InSpan = "k20";

    /// <summary>
    /// The donor's exclusive high bound, and the grafted key. They are deliberately
    /// the same value: a key equal to the exclusive high bound is out of span by one,
    /// which is the narrowest possible violation and the one the #3117 trace showed
    /// (<c>donorRange=[reshard-tx-07,reshard-tx-11) splitKey=reshard-tx-11</c>).
    /// </summary>
    private const string High = "k30";

    /// <summary>A second grafted key, used by <see cref="DonorRowShape.AllGrafted"/>.</summary>
    private const string FarGrafted = "k40";

    /// <summary>A second in-span key, used by <see cref="DonorRowShape.AllInSpan"/>.</summary>
    private const string InSpanLower = "k15";

    /// <summary>No row is held for the key.</summary>
    private const int None = 0;

    /// <summary>The value a grafted row carries, and the value a marooned leaf freezes at.</summary>
    private const int Pre = 1;

    /// <summary>The authoritative value an in-flight write commits.</summary>
    private const int Post = 2;

    private const int StepGraft = 0;
    private const int StepWrite = 1;
    private const int StepSplit = 2;
    private const int StepCount = 3;

    private const int Donor = 0;
    private const int Sibling = 1;
    private const int Successor = 2;
    private const int LeafCount = 3;
    private const int NoLeaf = -1;

    private readonly SplitPivotMode _mode;
    private readonly DonorRowShape _shape;

    /// <summary>
    /// Creates the model for a donor carrying the given <paramref name="shape"/> of
    /// rows into its split, under the given pivot <paramref name="mode"/>.
    /// </summary>
    public SplitPivotAdmissionModel(SplitPivotMode mode, DonorRowShape shape)
    {
        _mode = mode;
        _shape = shape;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);

        var leaves = CreateChain();
        var done = new bool[StepCount];
        var grafted = false;
        var graftPrecededSplit = false;

        for (var fired = 0; fired < StepCount; fired++)
        {
            var step = ChooseNextStep(runtime, done);
            done[step] = true;

            switch (step)
            {
                case StepGraft:
                    Graft(leaves);
                    grafted = true;
                    break;

                case StepWrite:
                    CommitAuthoritativeWrite(leaves);
                    break;

                default:
                    graftPrecededSplit = grafted;
                    Split(leaves);
                    break;
            }
        }

        if (_mode == SplitPivotMode.ValidatedOrderingProbe)
        {
            AssertGraftBeforeSplitUnreachable(graftPrecededSplit);
            return;
        }

        AssertNoMaroonedHolder(leaves);
    }

    /// <summary>
    /// The initial chain: a donor declaring <c>[Low, High)</c> whose in-span rows are
    /// already resident, and a successor declaring <c>[High, null)</c> that is the
    /// real custodian of every grafted key. The sibling does not exist until the
    /// split mints it.
    /// </summary>
    private Leaf?[] CreateChain()
    {
        var leaves = new Leaf?[LeafCount];

        var donor = new Leaf(Low, High, NoLeaf, Successor);
        leaves[Donor] = donor;
        leaves[Successor] = new Leaf(High, null, Donor, NoLeaf);

        switch (_shape)
        {
            case DonorRowShape.AllGrafted:
                break;

            case DonorRowShape.AllInSpan:
                donor.Put(InSpanLower, Pre);
                donor.Put(InSpan, Pre);
                break;

            default:
                donor.Put(InSpan, Pre);
                break;
        }

        return leaves;
    }

    /// <summary>
    /// The fail-open graft: an out-of-span row comes to rest on the donor because no
    /// forward target resolved for it, which
    /// <c>BPlusLeafGrain.SpanAdmission.cs</c> documents as deliberate rather than
    /// exceptional. This is the step that puts a key the donor does not declare into
    /// the row set the bisect will later draw its pivot from.
    /// </summary>
    private void Graft(Leaf?[] leaves)
    {
        var donor = leaves[Donor]!;
        donor.Put(High, Pre);

        if (_shape == DonorRowShape.AllGrafted)
        {
            donor.Put(FarGrafted, Pre);
        }
    }

    /// <summary>
    /// An authoritative write of the grafted key, routed the way production routes
    /// one: to the leaf that <em>declares</em> it. Span admission forwards a write
    /// until it reaches the declaring leaf, so the declaring leaf is where a write
    /// converges regardless of which leaf the descent first touched. A leaf declaring
    /// an empty range declares nothing and therefore never receives it.
    /// </summary>
    private static void CommitAuthoritativeWrite(Leaf?[] leaves)
    {
        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = leaves[i];
            if (leaf is not null && SplitBoundary.Owns(High, leaf.Low, leaf.High))
            {
                leaf.Put(High, Post);
                return;
            }
        }
    }

    /// <summary>
    /// The donor's division, as <c>BPlusLeafGrain.SplitAsync</c> performs it: bisect
    /// the row set, then - under <see cref="SplitPivotMode.Validated"/> - validate
    /// the result against the declared span, repair it, or decline. On a division the
    /// sibling is seeded with <c>[pivot, donorPreSplitHigh)</c> and takes every row at
    /// or above the pivot, and the donor's own high bound narrows to the pivot.
    /// </summary>
    private void Split(Leaf?[] leaves)
    {
        var donor = leaves[Donor]!;
        var pivot = Bisect(donor);

        if (_mode != SplitPivotMode.Unvalidated
            && !SplitPivotAdmission.IsAdmissible(pivot, donor.Low, donor.High))
        {
            pivot = SplitPivotAdmission.SelectMedianAdmissible(
                donor.OrderedKeys, donor.Low, donor.High);

            // No row falls strictly inside the declared range, so there is nothing
            // this leaf is entitled to divide. Declining is self-correcting: the
            // out-of-span rows drain to their real custodians, after which the leaf
            // is either under threshold or divisible.
            if (pivot is null)
            {
                return;
            }
        }

        // An empty donor bisects to nothing under either policy.
        if (pivot is null)
        {
            return;
        }

        var donorPreSplitHigh = donor.High;
        var sibling = new Leaf(pivot, donorPreSplitHigh, Donor, Successor);
        leaves[Sibling] = sibling;
        donor.High = pivot;
        donor.Next = Sibling;
        leaves[Successor]!.Prev = Sibling;

        donor.MoveAtOrAbove(pivot, sibling);
    }

    /// <summary>
    /// <c>LeafEntryCache.TryGetBisectingKeyWithoutHydrating</c>'s median selection,
    /// abstracted to its load-bearing property: the pivot is drawn from the row set
    /// and the declared span is never consulted. An empty row set bisects to nothing.
    /// </summary>
    private static string? Bisect(Leaf donor)
    {
        var count = donor.Count;
        if (count == 0)
        {
            return null;
        }

        return donor.KeyAt(count / 2);
    }

    /// <summary>
    /// Picks the next step to fire from those not yet fired, driving the choice
    /// through the runtime so exploration enumerates every ordering. The last
    /// remaining candidate is taken when no coin comes up, which keeps every run
    /// total - all steps always fire, only their order varies.
    /// </summary>
    private static int ChooseNextStep(ICoyoteRuntime runtime, bool[] done)
    {
        var last = NoLeaf;
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
    /// The safety property. No present leaf may declare an empty range while holding
    /// rows, because such a leaf serves reads that no write can ever reach: writes
    /// converge on the leaf that declares the key, reads are served by the leaf that
    /// holds it, and a leaf declaring nothing is permanently excluded from the first
    /// while remaining fully able to answer the second.
    /// <para>
    /// The assertion is deliberately stated over the leaf's declared range rather
    /// than over an observed read, because the defect is durable rather than a window
    /// that closes - there is no later instant at which the marooned copy heals, so
    /// the converged state is the honest place to assert. The companion value check
    /// then pins the observable consequence: the marooned leaf is frozen at the
    /// pre-write value while the real custodian holds the authoritative one.
    /// </para>
    /// </summary>
    private static void AssertNoMaroonedHolder(Leaf?[] leaves)
    {
        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = leaves[i];
            if (leaf is null || leaf.Count == 0 || DeclaresSomething(leaf))
            {
                continue;
            }

            // The message is built only on the failing path so exploration does not
            // pay for string formatting on every one of a thousand passing runs.
            Specification.Assert(
                false,
                $"leaf {i} declares the empty range [{leaf.Low},{leaf.High}) " +
                $"while holding {leaf.Count} row(s), including key {High} at value " +
                $"{leaf.Get(High)} against an authoritative {Post}. No routing descent " +
                "can select it, so it can never be written, forwarded to, or drained - but it " +
                "still answers reads, so the key is frozen at the stale value permanently.");
            return;
        }
    }

    /// <summary>
    /// Whether a leaf declares any key at all. A range is empty exactly when both
    /// bounds are present and the low one is not strictly below the high one; an
    /// unbounded side always leaves something declared.
    /// </summary>
    private static bool DeclaresSomething(Leaf leaf) =>
        leaf.Low is null || leaf.High is null || string.CompareOrdinal(leaf.Low, leaf.High) < 0;

    /// <summary>
    /// The anti-vacuity witness for <see cref="SplitPivotMode.ValidatedOrderingProbe"/>.
    /// Only an ordering that grafts the out-of-span row before the split gives the
    /// bisect an inadmissible key to return, so the fixed arm proves nothing unless
    /// exploration actually reaches that ordering. Coyote finding this violation is
    /// the proof that it does.
    /// </summary>
    private static void AssertGraftBeforeSplitUnreachable(bool graftPrecededSplit) =>
        Specification.Assert(
            !graftPrecededSplit,
            "reachability witness: exploration placed the out-of-span graft before the " +
            "split, which is the only ordering in which the bisect can return an " +
            "inadmissible pivot. This violation is expected and is what makes the " +
            "validated safety arm non-vacuous.");

    /// <summary>
    /// A leaf: its declared half-open range, its chain pointers, and its rows. Rows
    /// are held in a small ordered pair of parallel arrays rather than a dictionary
    /// so enumeration order is the ordinal key order the bisect and the median
    /// selection both assume.
    /// <para>
    /// A reference type deliberately. The split hands one leaf's rows to another, so
    /// a value type would copy the receiver and silently discard its row-count
    /// mutation while still writing through the shared backing arrays - a defect in
    /// the harness that would present as a defect in the model.
    /// </para>
    /// </summary>
    private sealed class Leaf
    {
        private const int Capacity = 4;

        private readonly string[] _keys = new string[Capacity];
        private readonly int[] _values = new int[Capacity];
        private int _count;

        public Leaf(string? low, string? high, int prev, int next)
        {
            Low = low;
            High = high;
            Prev = prev;
            Next = next;
        }

        public string? Low { get; set; }

        public string? High { get; set; }

        public int Prev { get; set; }

        public int Next { get; set; }

        public int Count => _count;

        /// <summary>The keys in ascending ordinal order, for the median selection.</summary>
        public IEnumerable<string> OrderedKeys
        {
            get
            {
                for (var i = 0; i < _count; i++)
                {
                    yield return _keys[i];
                }
            }
        }

        public string KeyAt(int ordinal) => _keys[ordinal];

        public int Get(string key)
        {
            for (var i = 0; i < _count; i++)
            {
                if (string.Equals(_keys[i], key, StringComparison.Ordinal))
                {
                    return _values[i];
                }
            }

            return None;
        }

        /// <summary>
        /// Inserts or overwrites a row, keeping the arrays in ascending ordinal key
        /// order so <see cref="OrderedKeys"/> and <see cref="KeyAt"/> agree with the
        /// production cache's sorted view.
        /// </summary>
        public void Put(string key, int value)
        {
            var slot = 0;
            while (slot < _count && string.CompareOrdinal(_keys[slot], key) < 0)
            {
                slot++;
            }

            if (slot < _count && string.Equals(_keys[slot], key, StringComparison.Ordinal))
            {
                _values[slot] = value;
                return;
            }

            for (var i = _count; i > slot; i--)
            {
                _keys[i] = _keys[i - 1];
                _values[i] = _values[i - 1];
            }

            _keys[slot] = key;
            _values[slot] = value;
            _count++;
        }

        /// <summary>
        /// Moves every row at or above <paramref name="pivot"/> to
        /// <paramref name="destination"/>, as <c>CompleteSplitAsync</c> does. The
        /// split is by key order, not by declared span, which is exactly why a
        /// grafted row travels with the sibling rather than being left behind.
        /// </summary>
        public void MoveAtOrAbove(string pivot, Leaf destination)
        {
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
    }
}
