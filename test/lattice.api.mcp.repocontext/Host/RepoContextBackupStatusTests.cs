using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the agent-memory backup health signal (issue #2602, criterion 6).
/// <para>
/// This epic has repeatedly been bitten by criteria that pass through
/// <b>absence</b>: a job that reports success over an empty or wrongly-scoped
/// selection is indistinguishable, through a success boolean, from one that
/// captured the memory tree. So the signal is a <b>positive statement about what
/// was captured</b> - which tree, how many entries, when - and every way that
/// statement can be true-but-worthless carries an explicit warning. These tests
/// assert the warnings, because the warnings are the part that has to survive a
/// future tidy-up.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextBackupStatusTests
{
    private const string Tree = RepoContextHostTrees.Memory;

    private static RepoContextBackupStatus Enabled() => new(enabled: true, scopedTreeId: Tree);

    [Test]
    public void A_disabled_status_says_plainly_that_nothing_is_being_captured()
    {
        var status = new RepoContextBackupStatus(enabled: false, scopedTreeId: Tree);

        Assert.That(status.Describe(), Does.Contain("DISABLED"));
        Assert.That(status.Describe(), Does.Contain(Tree));
        Assert.That(status.Describe(), Does.Contain(RepoContextBackup.BlobConnectionStringKey));
    }

    [Test]
    public void An_enabled_status_that_has_captured_nothing_says_so_rather_than_looking_healthy()
    {
        var status = Enabled();

        // The failure mode being excluded: "backup is enabled" reading as "backup
        // has happened". Registration and capture are different facts.
        Assert.That(status.Describe(), Does.Contain("captured NOTHING yet"));
        Assert.That(status.CaptureCount, Is.Zero);
    }

    [Test]
    public void A_successful_full_capture_states_the_tree_and_the_entry_count()
    {
        var status = Enabled();
        status.RecordCapture(
            backupId: "b-1",
            capturedTreeId: Tree,
            entryCount: 412,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

        var described = status.Describe();

        Assert.That(described, Does.Contain(Tree));
        Assert.That(described, Does.Contain("412"));
        Assert.That(described, Does.Contain("b-1"));
        Assert.That(described, Does.Not.Contain("WARNING"));
        Assert.That(status.CapturedTreeId, Is.EqualTo(Tree));
    }

    [Test]
    public void A_capture_of_the_wrong_tree_is_reported_as_a_warning_not_as_success()
    {
        var status = Enabled();
        status.RecordCapture(
            backupId: "b-1",
            capturedTreeId: "repo-context-structure",
            entryCount: 9000,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

        // A large entry count against the wrong tree is the most convincing-looking
        // form of this failure, so it is the one asserted.
        Assert.That(status.Describe(), Does.Contain("WARNING"));
        Assert.That(status.Describe(), Does.Contain("NOT the configured scope"));
    }

    [Test]
    public void A_full_capture_describing_zero_entries_is_reported_as_a_warning()
    {
        var status = Enabled();
        status.RecordCapture(
            backupId: "b-1",
            capturedTreeId: Tree,
            entryCount: 0,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

        // Capturing an empty selection succeeds. It also protects nothing.
        Assert.That(status.Describe(), Does.Contain("WARNING"));
        Assert.That(status.Describe(), Does.Contain("ZERO entries"));
    }

    [Test]
    public void An_incremental_silently_promoted_to_a_full_capture_is_counted_and_reported()
    {
        var status = Enabled();

        // LatticeBackupCaptureService degrades an incremental into a full capture
        // when the base chain is unusable, recording ReasonIncrementalFallback. A
        // deployment where every incremental has quietly fallen back is running,
        // but is not doing what its configuration says.
        status.RecordCapture(
            backupId: "b-2",
            capturedTreeId: Tree,
            entryCount: 412,
            isFull: true,
            requestedIncremental: true,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

        Assert.That(status.IncrementalFallbackCount, Is.EqualTo(1));
        Assert.That(status.Describe(), Does.Contain("promoted to full"));
    }

    [Test]
    public void A_genuine_incremental_is_not_counted_as_a_fallback()
    {
        var status = Enabled();
        status.RecordCapture(
            backupId: "b-2",
            capturedTreeId: Tree,
            entryCount: 3,
            isFull: false,
            requestedIncremental: true,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

        Assert.That(status.IncrementalFallbackCount, Is.Zero);
        Assert.That(status.Describe(), Does.Not.Contain("promoted to full"));
    }

    [Test]
    public void A_non_durable_sink_is_reported_as_a_warning_even_when_captures_succeed()
    {
        var status = Enabled();
        status.RecordNonDurableSink("InClusterLatticeBackupSink");
        status.RecordCapture(
            backupId: "b-1",
            capturedTreeId: Tree,
            entryCount: 412,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

        // The sharpest form of false protection: every capture succeeds, and every
        // capture dies with the store it was protecting.
        Assert.That(status.Describe(), Does.Contain("WARNING"));
        Assert.That(status.Describe(), Does.Contain("NOT durable"));
        Assert.That(status.NonDurableSinkType, Is.EqualTo("InClusterLatticeBackupSink"));
    }

    [Test]
    public void Sink_inventory_distinguishes_an_unread_sink_from_a_readably_empty_one()
    {
        var unread = Enabled();

        // -1 (never read) and 0 (read, and empty) are different facts. After the
        // store is destroyed this is the only number that says whether anything is
        // recoverable, because the backup catalog lived in the destroyed tree.
        Assert.That(unread.SinkBackupCount, Is.EqualTo(-1));
        Assert.That(unread.Describe(), Does.Not.Contain("holds NO backup"));

        var empty = Enabled();
        empty.RecordSinkInventory(0, null, null);
        Assert.That(empty.SinkBackupCount, Is.Zero);
        Assert.That(empty.Describe(), Does.Contain("NO backup"));
    }

    [Test]
    public void Sink_inventory_names_the_newest_restorable_backup()
    {
        var status = Enabled();
        status.RecordSinkInventory(7, "b-newest", DateTimeOffset.UnixEpoch);

        var described = status.Describe();

        Assert.That(described, Does.Contain("7"));
        Assert.That(described, Does.Contain("b-newest"));
        Assert.That(status.SinkNewestBackupId, Is.EqualTo("b-newest"));
        Assert.That(status.SinkNewestAtUtc, Is.EqualTo(DateTimeOffset.UnixEpoch));
    }

    [Test]
    public void A_failure_is_surfaced_alongside_the_successes()
    {
        var status = Enabled();
        status.RecordFailure("the sink refused the write");

        Assert.That(status.LastFailure, Is.EqualTo("the sink refused the write"));
        Assert.That(status.Describe(), Does.Contain("the sink refused the write"));
    }

    [Test]
    public void A_later_success_clears_a_previous_failure()
    {
        var status = Enabled();
        status.RecordFailure("transient");
        status.RecordCapture("b-1", Tree, 1, isFull: true, requestedIncremental: false, DateTimeOffset.UnixEpoch);

        Assert.That(status.LastFailure, Is.Null);
    }

    [Test]
    public void A_negative_sink_count_is_refused()
    {
        Assert.That(
            () => Enabled().RecordSinkInventory(-1, null, null),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
