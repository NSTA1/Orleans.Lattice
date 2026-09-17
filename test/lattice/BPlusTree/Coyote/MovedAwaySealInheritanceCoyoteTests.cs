using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for <b>moved-away seal inheritance</b> -
/// the rule that a leaf minted by dividing a sealed leaf must be born carrying that
/// leaf's seal. They are tagged <c>[Category("Coyote")]</c> so the fast dev loop and
/// the per-package deterministic CI step skip them; a dedicated CI step runs this
/// category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// <b>What this fixture is and is not for.</b> The content of the inheritance rule is
/// deterministic and is covered by <c>MovedAwaySealInheritanceTests</c>. Issue 3121 is
/// a structural defect - every non-trivial division of a sealed donor leaks, not just
/// an unlucky one - so a schedule explorer is the wrong instrument for the rule
/// itself. What is genuinely schedule-dependent is whether the shard-side walk that
/// records the seal runs before or after the division: after, it finds and seals both
/// halves by accident; before, there is only one leaf to find and the sibling is
/// minted afterwards with nothing. This fixture exists to prove that no ordering
/// escapes the rule, which is the one claim the deterministic tests cannot make.
/// </para>
/// <para>
/// <see cref="SplitPivotAdmissionCoyoteTests"/> covers the neighbouring defect in the
/// same seam - a division at a pivot outside the donor's declared span - and the two
/// are worth reading together: that one leaves a leaf that declares nothing, this one
/// leaves a leaf that declares too much.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class MovedAwaySealInheritanceCoyoteTests
{
    /// <summary>
    /// The defect, and the encoded perturbation. Seeding a sibling with the tree id,
    /// shard index, key range and sibling pointers but not the donor's moved-away seal
    /// leaves it holding migrated rows with nothing to suppress them. Coyote must find
    /// the ordering.
    /// <para>
    /// Expected failure value, not merely status: the sibling declares
    /// <c>[k20, null)</c> and serves the migrated key at the orphan <c>1</c>, against
    /// an authoritative <c>2</c> on the destination shard that no write can ever
    /// deliver to it - because writes for that slot route to the destination. That is
    /// the issue 3121 signature exactly: a read served from a leaf the correcting
    /// write can never reach.
    /// </para>
    /// <para>
    /// Violating orderings are exactly those placing the seal before the split. In the
    /// others the shard-side walk finds both halves already present and seals both, so
    /// the unseeded policy is indistinguishable from the seeded one - which is why the
    /// defect survived: the ordering that hides it is as common as the one that
    /// exposes it.
    /// </para>
    /// </summary>
    [Test]
    public void A_sibling_denied_the_donor_seal_serves_a_migrated_orphan()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new MovedAwaySealInheritanceModel(
                SealInheritanceMode.NotInherited,
                SealedRowShape.SealedKeyAbovePivot));
    }

    /// <summary>
    /// The fix direction, and the oracle a candidate fix must satisfy. Carrying the
    /// donor's seal on the initialization round-trip and unioning it into the sibling
    /// before any migrated row becomes visible leaves no ordering in which a leaf
    /// serves a migrated orphan. Its silence is earned rather than assumed - the only
    /// difference between this arm and the violating arm above is the one seeding
    /// step.
    /// </summary>
    [Test]
    public void An_inherited_seal_leaves_no_ordering_that_serves_an_orphan()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new MovedAwaySealInheritanceModel(
                SealInheritanceMode.Inherited,
                SealedRowShape.SealedKeyAbovePivot));
    }

    /// <summary>
    /// The anti-vacuity witness. Under the fixed policy the model asserts the
    /// dangerous ordering is never reached, so Coyote finding a violation is the proof
    /// that exploration does place the seal before the division. Without this, a step
    /// scheduler that silently stopped exploring that ordering would make the arm
    /// above pass while proving nothing at all - which matters more here than usual,
    /// because seal-after-split is independently safe and would mask the defect.
    /// </summary>
    [Test]
    public void The_fixed_policy_still_reaches_the_seal_before_split_ordering()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new MovedAwaySealInheritanceModel(
                SealInheritanceMode.InheritedOrderingProbe,
                SealedRowShape.SealedKeyAbovePivot));
    }

    /// <summary>
    /// The degenerate arm. A donor holding no seal has nothing to pass on, so
    /// inheritance must be a complete no-op - and in particular must not stamp a bare
    /// virtual shard count or an empty slot set onto the sibling as though it were a
    /// seal. The model's live-row property is what makes this arm bite: a fix that
    /// sealed the sibling defensively would hide a row nothing had migrated, which
    /// fails here rather than passing quietly.
    /// </summary>
    [Test]
    public void An_unsealed_donor_mints_a_sibling_that_still_serves_its_rows(
        [Values(SealInheritanceMode.NotInherited, SealInheritanceMode.Inherited)]
        SealInheritanceMode mode)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new MovedAwaySealInheritanceModel(mode, SealedRowShape.NoSeal));
    }

    /// <summary>
    /// The no-regression arm, which tests that the change does not disturb a division
    /// that was already correct. When the sealed row sits below the pivot it stays with
    /// the donor, which keeps its own seal either way, so both policies must be silent
    /// and identical. This is what would catch an inheritance step that moved the seal
    /// instead of copying it - stripping the donor and reintroducing the same defect on
    /// the other half.
    /// </summary>
    [Test]
    public void A_sealed_row_below_the_pivot_is_unaffected_by_either_policy(
        [Values(SealInheritanceMode.NotInherited, SealInheritanceMode.Inherited)]
        SealInheritanceMode mode)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new MovedAwaySealInheritanceModel(mode, SealedRowShape.SealedKeyBelowPivot));
    }
}
