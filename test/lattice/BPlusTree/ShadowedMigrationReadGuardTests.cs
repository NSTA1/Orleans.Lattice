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
    public void Is_saga_safe_is_false_only_for_committed_without_terminal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Committed, terminalApplied: false), Is.False);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Committed, terminalApplied: true), Is.True);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.InFlight, terminalApplied: false), Is.True);
            Assert.That(ShadowedMigrationReadGuard.IsSagaSafe(TxStatus.Aborted, terminalApplied: false), Is.True);
        });
    }
}
