using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the <b>declared-span admission
/// exemption</b> that cross-shard migration imports used to enjoy - the
/// <c>!isCrossShardMigration</c> term in <c>BPlusLeafGrain.MergeManyAsync</c>'s
/// admission predicate. They are tagged <c>[Category("Coyote")]</c> so the fast dev
/// loop and the per-package deterministic CI step skip them; a dedicated CI step
/// runs this category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// <see cref="ReshardForwardWindowCoyoteTests"/> covers a <b>read-visibility</b>
/// defect in the same subsystem: a destination holding a drain-migrated value that
/// no shadow marker yet gates, so a reader falls through onto it. This fixture
/// covers a <b>placement</b> defect: a row admitted by a leaf that does not declare
/// the key, so it comes to rest in the wrong place and stays there. The first is a
/// window that closes; the second is durable, which is why this model asserts on the
/// converged state rather than on an interleaved observation.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class SpanAdmissionMigrationCoyoteTests
{
    /// <summary>
    /// The guard, and the encoded perturbation. Exempting a migration import from
    /// declared-span admission lets a donor whose span was narrowed by its own split
    /// accept a late import for a key that split had already moved to the sibling, so
    /// the key comes to rest on two leaves at once and the donor serves a stale
    /// pre-split value it no longer declares. Coyote must find the ordering.
    /// <para>
    /// This is the only fixture that can observe the clause, because the clause's
    /// sole effect is to decide whether an out-of-span migration import is forwarded
    /// or committed locally, and this is the only model that makes the span-narrowing
    /// step, the import's arrival, and the foreground commit independently
    /// schedulable - so it can place the import strictly after the narrow, the single
    /// ordering in which the clause changes the outcome.
    /// </para>
    /// <para>
    /// Expected failure value, not merely status: the donor holds <c>1</c> (the
    /// pre-split value, migrated) while the sibling holds <c>2</c> (the authoritative
    /// post-split value), so the reported violation is <c>2 leaves hold key k50</c>.
    /// Violating orderings are exactly those placing the import after the narrow; in
    /// the others the donor still holds a non-migrated row and the asymmetric
    /// migration guard drops the import, which is why the exemption looked harmless.
    /// </para>
    /// </summary>
    [Test]
    public void Migration_exempt_admission_resurrects_a_stale_row_on_the_split_donor()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SpanAdmissionMigrationModel(
                SpanAdmissionMode.MigrationExempt,
                ImportDestination.SplitNarrowedDonor));
    }

    /// <summary>
    /// The fix direction: applying declared-span admission to migration imports as
    /// well leaves no ordering in which the key comes to rest anywhere but the single
    /// leaf that declares it, holding the authoritative post-split value <c>2</c>.
    /// This is the oracle a candidate fix must satisfy, and its silence is earned
    /// rather than assumed - the only difference between this arm and the violating
    /// arm above is the one predicate, so it cannot be vacuous.
    /// </summary>
    [Test]
    public void Admission_applied_to_migration_imports_never_resurrects_a_stale_row()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SpanAdmissionMigrationModel(
                SpanAdmissionMode.AdmissionApplied,
                ImportDestination.SplitNarrowedDonor));
    }

    /// <summary>
    /// The anti-vacuity witness. Under the fixed admission rule the model asserts the
    /// dangerous ordering is never reached, so Coyote finding a violation is the
    /// proof that exploration does place the late import after the donor's span
    /// narrows. Without this, a step scheduler that silently stopped exploring that
    /// ordering would make the arm above pass while proving nothing.
    /// </summary>
    [Test]
    public void Admission_applied_still_reaches_the_late_import_ordering()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SpanAdmissionMigrationModel(
                SpanAdmissionMode.AdmissionAppliedOrderingProbe,
                ImportDestination.SplitNarrowedDonor));
    }

    /// <summary>
    /// The no-regression arm, which tests the justification for removing the
    /// exemption rather than the removal itself. Topology seeding really does place
    /// rows before the coordinator sets the destination's range, and that shape must
    /// keep working once migration imports are admitted like any other write. It does,
    /// for two independent reasons, one per case: an unbounded destination never
    /// enters the span scan at all, and a destination seeded with a range that does
    /// not cover the key resolves no forward target and falls open to a local commit.
    /// <para>
    /// Expected value: the destination holds <c>1</c> and nothing was forwarded away,
    /// in both orderings of the range-set step and the import. The second half is what
    /// would catch a fix that over-forwards and loses a legitimate seed.
    /// </para>
    /// </summary>
    [Test]
    public void Admission_applied_still_seeds_a_freshly_created_destination(
        [Values(ImportDestination.UnboundedFreshDestination, ImportDestination.ChainlessFreshDestination)]
        ImportDestination destination)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SpanAdmissionMigrationModel(SpanAdmissionMode.AdmissionApplied, destination));
    }
}
