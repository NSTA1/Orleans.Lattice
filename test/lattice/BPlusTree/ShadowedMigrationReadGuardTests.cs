using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="ShadowedMigrationReadGuard"/> -
/// the shared read-side orphan guard the production leaf
/// <c>IsShadowedReadSafeAsync</c> path executes. No Coyote model drives it, so
/// these unit tests are what pin its cases.
/// These pin the exact four-outcome per-saga rule so a change is caught here
/// rather than only by a slow reshard chaos run.
/// </summary>
[TestFixture]
public sealed class ShadowedMigrationReadGuardTests
{
    [Test]
    public void InFlight_passes_through([Values] bool terminalApplied)
    {
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(TxStatus.InFlight, terminalApplied),
            Is.EqualTo(ShadowedReadDecision.PassThrough));
    }

    [Test]
    public void Aborted_passes_through([Values] bool terminalApplied)
    {
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Aborted, terminalApplied),
            Is.EqualTo(ShadowedReadDecision.PassThrough));
    }

    [Test]
    public void Committed_with_terminal_landed_serves_projected()
    {
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Committed, terminalApplied: true),
            Is.EqualTo(ShadowedReadDecision.ServeProjected));
    }

    [Test]
    public void Committed_without_terminal_gates_stale_routing()
    {
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Committed, terminalApplied: false),
            Is.EqualTo(ShadowedReadDecision.GateStaleRouting));
    }

    [Test]
    public void Indeterminate_with_terminal_landed_serves_projected()
    {
        // The terminal has landed, so Entries[K] holds the post-saga value
        // whichever way the decision went. Not knowing the decision costs
        // nothing here, so the read stays available.
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Indeterminate, terminalApplied: true),
            Is.EqualTo(ShadowedReadDecision.ServeProjected));
    }

    [Test]
    public void Indeterminate_without_terminal_gates_stale_routing()
    {
        // Passing through would serve the migrated pre-saga value, which is an
        // affirmative claim that the shadowing saga did not commit - exactly
        // what an indeterminate reading does not know. It must gate, for the
        // same reason AtomicVisibilityGate hides an indeterminate saga's keys.
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Indeterminate, terminalApplied: false),
            Is.EqualTo(ShadowedReadDecision.GateStaleRouting));
    }

    [Test]
    public void Indeterminate_resolves_exactly_as_committed_does([Values] bool terminalApplied)
    {
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Indeterminate, terminalApplied),
            Is.EqualTo(ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Committed, terminalApplied)),
            "An indeterminate saga cannot be ruled out as committed, so it must "
            + "take the committed arm rather than the pass-through one.");
    }

    [Test]
    public void Is_saga_safe_is_false_only_for_committed_without_terminal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Committed, terminalApplied: false), Is.False);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Committed, terminalApplied: true), Is.True);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Indeterminate, terminalApplied: false), Is.False);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Indeterminate, terminalApplied: true), Is.True);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.InFlight, terminalApplied: false), Is.True);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Aborted, terminalApplied: false), Is.True);
        });
    }

    [Test]
    public void Every_tx_status_resolves_to_a_defined_decision()
    {
        // The guard must stay total across the enum. A case added to TxStatus
        // without a decision here would silently inherit the pass-through arm,
        // which is the defect this fixture exists to catch.
        foreach (TxStatus status in Enum.GetValues<TxStatus>())
        {
            foreach (var terminalApplied in new[] { false, true })
            {
                foreach (var incorporated in new[] { false, true })
                {
                    Assert.That(
                        Enum.IsDefined(ShadowedMigrationReadGuard.ResolveSaga(status, terminalApplied, incorporated)),
                        Is.True,
                        $"ResolveSaga({status}, {terminalApplied}, {incorporated}) returned an undefined decision.");
                }
            }
        }
    }

    // ---- Issue #4545: the marked-prepare self-check

    private static readonly HybridLogicalClock P = new() { WallClockTicks = 5_000, Counter = 3 };

    [Test]
    public void A_committed_saga_whose_row_incorporates_its_marked_prepare_is_served_without_its_terminal(
        [Values] bool committed)
    {
        var status = committed ? TxStatus.Committed : TxStatus.Indeterminate;
        // The marker's terminal never reaches this leaf - a leaf split carried it
        // here, or a reactivation forgot the terminal - but the row is the saga's
        // own value or a later write, so the read is safe.
        Assert.That(
            ShadowedMigrationReadGuard.ResolveSaga(status, terminalApplied: false, rowIncorporatesMarkedPrepare: true),
            Is.EqualTo(ShadowedReadDecision.ServeIncorporated));
    }

    [Test]
    public void The_self_check_never_overrides_a_pass_through_or_a_landed_terminal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShadowedMigrationReadGuard.ResolveSaga(TxStatus.InFlight, false, true), Is.EqualTo(ShadowedReadDecision.PassThrough));
            Assert.That(ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Aborted, false, true), Is.EqualTo(ShadowedReadDecision.PassThrough));
            Assert.That(ShadowedMigrationReadGuard.ResolveSaga(TxStatus.Committed, true, true), Is.EqualTo(ShadowedReadDecision.ServeProjected));
        });
    }

    [Test]
    public void Without_the_self_check_the_three_argument_rule_is_the_original_rule()
    {
        foreach (var status in Enum.GetValues<TxStatus>())
        {
            foreach (var terminalApplied in new[] { false, true })
            {
                Assert.That(
                    ShadowedMigrationReadGuard.ResolveSaga(status, terminalApplied, rowIncorporatesMarkedPrepare: false),
                    Is.EqualTo(ShadowedMigrationReadGuard.ResolveSaga(status, terminalApplied)),
                    $"{status}, terminalApplied={terminalApplied}");
            }
        }
    }

    [Test]
    public void A_row_incorporates_a_marked_prepare_only_at_or_above_its_stamp()
    {
        var below = new HybridLogicalClock { WallClockTicks = 5_000, Counter = 2 };
        var above = new HybridLogicalClock { WallClockTicks = 5_001, Counter = 0 };
        Assert.Multiple(() =>
        {
            Assert.That(ShadowedMigrationReadGuard.RowIncorporatesMarkedPrepare(P, P), Is.True, "the saga's own value");
            Assert.That(ShadowedMigrationReadGuard.RowIncorporatesMarkedPrepare(above, P), Is.True, "a later write");
            Assert.That(ShadowedMigrationReadGuard.RowIncorporatesMarkedPrepare(below, P), Is.False,
                "a pre-saga value: serving it would lose the saga's write (NoKeyLost)");
            Assert.That(ShadowedMigrationReadGuard.RowIncorporatesMarkedPrepare(above, null), Is.False,
                "a marker without a marked stamp keeps the original gate");
        });
    }

    [Test]
    public void Is_saga_safe_with_the_self_check_is_false_only_for_an_unincorporated_committed_or_indeterminate_saga()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Committed, false, false), Is.False);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Indeterminate, false, false), Is.False);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Committed, false, true), Is.True);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Indeterminate, false, true), Is.True);
        });
    }
}
