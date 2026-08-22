using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="MigrationTerminalCore"/> - the
/// shared write-side terminal disposition core the production leaf
/// <c>ApplyTxTerminalAsync</c> path and the Coyote reshard model both execute.
/// These pin the exact bucket-fate truth table (including the orphan guard) so a
/// change to the rule is caught here rather than only by a slow reshard chaos run.
/// </summary>
[TestFixture]
public sealed class MigrationTerminalCoreTests
{
    [Test]
    public void No_bucket_yields_none([Values] bool alreadyTerminal, [Values] bool committed)
    {
        Assert.That(
            MigrationTerminalCore.DecideBucketAction(hadPending: false, alreadyTerminal, committed),
            Is.EqualTo(MigrationTerminalBucketAction.None));
    }

    [Test]
    public void Pending_not_terminal_commit_drains()
    {
        Assert.That(
            MigrationTerminalCore.DecideBucketAction(hadPending: true, alreadyTerminal: false, committed: true),
            Is.EqualTo(MigrationTerminalBucketAction.DrainCommit));
    }

    [Test]
    public void Pending_not_terminal_abort_discards_aborted()
    {
        Assert.That(
            MigrationTerminalCore.DecideBucketAction(hadPending: true, alreadyTerminal: false, committed: false),
            Is.EqualTo(MigrationTerminalBucketAction.DiscardAborted));
    }

    [Test]
    public void Pending_already_terminal_discards_orphan_regardless_of_verdict([Values] bool committed)
    {
        // The orphan guard: once the terminal has landed, a surviving or late
        // bucket must be discarded, never drained - regardless of commit/abort.
        Assert.That(
            MigrationTerminalCore.DecideBucketAction(hadPending: true, alreadyTerminal: true, committed),
            Is.EqualTo(MigrationTerminalBucketAction.DiscardOrphan));
    }

    [Test]
    public void Redelivery_is_noop_only_when_terminal_and_no_work_left()
    {
        Assert.That(
            MigrationTerminalCore.IsNoOpRedelivery(alreadyTerminal: true, hadPending: false, hasMissingBackstopKeys: false),
            Is.True);
    }

    [Test]
    public void Redelivery_is_not_noop_when_not_yet_terminal()
    {
        Assert.That(
            MigrationTerminalCore.IsNoOpRedelivery(alreadyTerminal: false, hadPending: false, hasMissingBackstopKeys: false),
            Is.False);
    }

    [Test]
    public void Redelivery_is_not_noop_when_bucket_present()
    {
        Assert.That(
            MigrationTerminalCore.IsNoOpRedelivery(alreadyTerminal: true, hadPending: true, hasMissingBackstopKeys: false),
            Is.False);
    }

    [Test]
    public void Redelivery_is_not_noop_when_backstop_keys_outstanding()
    {
        Assert.That(
            MigrationTerminalCore.IsNoOpRedelivery(alreadyTerminal: true, hadPending: false, hasMissingBackstopKeys: true),
            Is.False);
    }
}
