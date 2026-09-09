using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="ShadowedMigrationReadGuard"/> -
/// the shared read-side orphan guard the production leaf
/// <c>IsShadowedReadSafeAsync</c> path and the Coyote reshard model both execute.
/// These pin the exact three-outcome per-saga rule so a change is caught here
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
                Assert.That(
                    Enum.IsDefined(ShadowedMigrationReadGuard.ResolveSaga(status, terminalApplied)),
                    Is.True,
                    $"ResolveSaga({status}, {terminalApplied}) returned an undefined decision.");
            }
        }
    }
}
