using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the online-reshard migration
/// protocol's interaction with the atomic-commit saga (#1591, reproducing #1584).
/// They are tagged <c>[Category("Coyote")]</c> so the fast dev loop and the
/// per-package deterministic CI step skip them; a dedicated CI step runs this
/// category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production cores
/// (<see cref="Orleans.Lattice.BPlusTree.MigrationTerminalCore"/>,
/// <see cref="Orleans.Lattice.BPlusTree.AtomicVisibilityGate"/>,
/// <see cref="Orleans.Lattice.BPlusTree.TxRegistryDecisionCore"/>) and the two
/// guard tests remove one orphan guard each, proving Coyote re-finds the
/// split-view / stale-value regression - a vacuous model that still passes with a
/// guard removed would be rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class ReshardMigrationCoyoteTests
{
    /// <summary>
    /// The fix: with both the write-side terminal disposition guard
    /// (<see cref="Orleans.Lattice.BPlusTree.MigrationTerminalBucketAction.DiscardOrphan"/>)
    /// and the read-side gate's terminal-landed input active, no interleaving of the
    /// late orphan prepare, its duplicate terminal, the backstop, and the reader
    /// fan-out lets a reader observe a split view or a stale orphan value - for a
    /// two-key fan-out and wider.
    /// </summary>
    [Test]
    public void Guarded_migration_never_shadows_a_later_saga_value([Values(2, 3, 4)] int keyCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new ReshardMigrationModel(keyCount, ReshardGuardMode.Guarded));
    }

    /// <summary>
    /// The guard: removing the read-side orphan guard (resolving an already-terminal
    /// saga's surviving orphan bucket as if its terminal had not landed) lets
    /// <see cref="Orleans.Lattice.BPlusTree.AtomicVisibilityGate"/> surface the stale
    /// prepare-time value on some keys but not others, and Coyote must find the split.
    /// This proves the <c>alreadyTerminal</c> input to the read gate is load-bearing.
    /// </summary>
    [Test]
    public void Removing_the_read_guard_reintroduces_the_orphan_split([Values(2, 3)] int keyCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new ReshardMigrationModel(keyCount, ReshardGuardMode.NoReadGuard));
    }

    /// <summary>
    /// The guard: removing the write-side terminal disposition guard (draining a
    /// late orphan bucket into projected state instead of discarding it) stamps an
    /// old saga round's value over the current one, and Coyote must find the
    /// resulting <c>unknown-round</c> split. This proves
    /// <see cref="Orleans.Lattice.BPlusTree.MigrationTerminalCore.DecideBucketAction"/>'s
    /// orphan case is load-bearing.
    /// </summary>
    [Test]
    public void Removing_the_write_guard_reintroduces_the_unknown_round_regression([Values(2, 3)] int keyCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new ReshardMigrationModel(keyCount, ReshardGuardMode.NoWriteGuard));
    }
}
