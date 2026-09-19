using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for <b>split pivot admissibility</b> -
/// the rule that a leaf may only be divided at a key strictly inside its own
/// <c>[LowKeyInclusive, HighKeyExclusive)</c> range. They are tagged
/// <c>[Category("Coyote")]</c> so the fast dev loop and the per-package
/// deterministic CI step skip them; a dedicated CI step runs this category. See the
/// "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// <see cref="SpanAdmissionMigrationCoyoteTests"/> covers a <b>placement</b> defect
/// in the same subsystem: a row admitted by a leaf that does not declare the key.
/// This fixture covers the <b>converse</b>: a leaf that declares no key at all, yet
/// holds rows and answers reads. The first leaves a row in the wrong place while
/// every leaf stays routable; the second leaves a whole leaf unroutable, which is
/// why this model asserts over the declared ranges rather than over row placement.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class SplitPivotAdmissionCoyoteTests
{
    /// <summary>
    /// The guard, and the encoded perturbation. Drawing the pivot from the row set
    /// without checking it against the declared span lets a donor holding a
    /// fail-open grafted row be divided at that row, seeding the sibling with
    /// <c>[High, High)</c>. Coyote must find the ordering.
    /// <para>
    /// Expected failure value, not merely status: the sibling declares the empty
    /// range <c>[k30,k30)</c> while holding the row for <c>k30</c> at the stale
    /// <c>1</c>, against an authoritative <c>2</c> that converged on the successor -
    /// the leaf that actually declares the key. That is the #3117 signature exactly:
    /// reads served from a leaf no write can reach.
    /// </para>
    /// <para>
    /// Violating orderings are exactly those placing the graft before the split; in
    /// the others the donor's row set holds only in-span keys, the bisect returns an
    /// admissible pivot on its own, and the unvalidated policy is indistinguishable
    /// from the validated one. That is why the defect presented as an intermittent
    /// chaos failure rather than a deterministic one.
    /// </para>
    /// </summary>
    [Test]
    public void An_unvalidated_pivot_maroons_a_leaf_that_declares_nothing()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SplitPivotAdmissionModel(
                SplitPivotMode.Unvalidated,
                DonorRowShape.OneInSpanOneGrafted));
    }

    /// <summary>
    /// The fix direction, and the oracle a candidate fix must satisfy. Validating
    /// the pivot against the declared span and repairing it to the in-span median
    /// leaves no ordering in which any leaf comes to declare an empty range, so every
    /// leaf stays routable and no key can be frozen out of reach of its writers.
    /// Its silence is earned rather than assumed - the only difference between this
    /// arm and the violating arm above is the one predicate.
    /// </summary>
    [Test]
    public void A_validated_pivot_never_maroons_a_leaf()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SplitPivotAdmissionModel(
                SplitPivotMode.Validated,
                DonorRowShape.OneInSpanOneGrafted));
    }

    /// <summary>
    /// The anti-vacuity witness. Under the validated policy the model asserts the
    /// dangerous ordering is never reached, so Coyote finding a violation is the
    /// proof that exploration does place the out-of-span graft before the split.
    /// Without this, a step scheduler that silently stopped exploring that ordering
    /// would make the arm above pass while proving nothing.
    /// </summary>
    [Test]
    public void A_validated_pivot_still_reaches_the_graft_before_split_ordering()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SplitPivotAdmissionModel(
                SplitPivotMode.ValidatedOrderingProbe,
                DonorRowShape.OneInSpanOneGrafted));
    }

    /// <summary>
    /// The decline arm. When every row the donor holds is out of span there is no
    /// admissible pivot at all, and the only safe answer is to refuse the split
    /// rather than pick the least-bad key. This proves the repair path degrades to a
    /// decline instead of falling back on an inadmissible pivot, which would
    /// reintroduce the defect through the fix itself.
    /// <para>
    /// Declining is not a wedge: the leaf owns nothing it is entitled to divide, so
    /// its rows drain to their real custodians and it is then either under threshold
    /// or divisible normally. The model asserts the safety property, not that a split
    /// occurred, which is the distinction that makes a decline acceptable here.
    /// </para>
    /// </summary>
    [Test]
    public void A_donor_holding_only_out_of_span_rows_declines_rather_than_maroons()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SplitPivotAdmissionModel(SplitPivotMode.Validated, DonorRowShape.AllGrafted));
    }

    /// <summary>
    /// The no-regression arm, which tests that the guard does not fire where it must
    /// not. A donor whose rows are all comfortably inside its span bisects to an
    /// admissible pivot unaided, so validation must be a pure pass-through and the
    /// split must proceed exactly as before. This is what would catch an over-eager
    /// predicate that declined or repaired a healthy division - a change that would
    /// pass every arm above while silently starving the tree of splits.
    /// </summary>
    [Test]
    public void A_healthy_donor_splits_identically_under_both_policies(
        [Values(SplitPivotMode.Unvalidated, SplitPivotMode.Validated)] SplitPivotMode mode)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SplitPivotAdmissionModel(mode, DonorRowShape.AllInSpan));
    }
}
