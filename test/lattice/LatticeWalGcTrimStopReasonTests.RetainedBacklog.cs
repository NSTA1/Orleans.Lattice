using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for <see cref="LatticeWalGcReport.RetainedBacklog"/> (issue #3213), the
/// signal the scheduler reads to tell a tree that had <i>nothing to do</i> from
/// one that <i>could not do anything</i>.
/// <para>
/// The issue proposed deriving that signal from
/// <see cref="LatticeWalGcReport.RetainedBytesAfter"/>, which cannot work:
/// the byte sample is taken only when <c>WalMaxRetainedBytes</c> is configured
/// and that option has no default, so the field is <see langword="null"/> in
/// exactly the deployment the fix exists for. The trim scan's own stop reason
/// carries the same information unconditionally and at no extra cost, because
/// the scan already computes it on every pass: it walks ascending offsets and
/// halts at the first entry it may not trim, so any stop other than an exhausted
/// or an empty log <i>is</i> a statement that WAL was left behind.
/// </para>
/// <para>
/// These tests bind that derivation to the real collector rather than to a
/// stubbed report. A scheduler test can only assert what the scheduler does with
/// the flag; if the flag were never set by the code that actually scans, every
/// cadence test in the suite would pass against a build where the defect was
/// fully intact.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcTrimStopReasonTests
{
    [Test]
    public async Task RunOnceAsync_a_scan_stopped_by_the_offset_floor_reports_a_retained_backlog()
    {
        // The live signature from issue #3213: the whole retained range sits
        // above a stranded durable checkpoint, so the scan stops on its first
        // entry and three entries stay on disk. No ceiling is configured here -
        // the collector is built without one - which is the entire point.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(5, Hlc(10)), Entry(6, Hlc(11)), Entry(7, Hlc(12)) },
            CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 2);

        var (report, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(report.RetainedBytesAfter, Is.Null,
                "and the byte sample the issue proposed reading is null here, which is why it could not be used.");
            Assert.That(report.RetainedBacklog, Is.True,
                "a scan that stopped on an entry it may not trim left WAL behind, and must say so.");
        });
    }

    [Test]
    public async Task RunOnceAsync_a_scan_stopped_by_the_cursor_floor_reports_a_retained_backlog()
    {
        // The other indicting stop. The signal has to be about the *fact* of
        // retention rather than about any one cause, because the scheduler's
        // question is "is there anything left to come back for", which every
        // retention stop answers identically.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(100)), Entry(1, Hlc(101)) },
            CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 10);

        var (report, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(report.RetainedBacklog, Is.True);
        });
    }

    [Test]
    public async Task RunOnceAsync_a_scan_that_consumed_the_whole_log_reports_no_retained_backlog()
    {
        // The half of the split that must keep relaxing. A tree that trimmed
        // everything it had has nothing outstanding for a shorter cadence to
        // recover, so capping its backoff would buy nothing and cost a wake.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(11)), Entry(2, Hlc(12)) },
            CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 10);

        var (report, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3));
            Assert.That(report.RetainedBacklog, Is.False,
                "an exhausted scan consumed the whole log, so nothing is outstanding.");
        });
    }

    [Test]
    public async Task RunOnceAsync_an_empty_shard_reports_no_retained_backlog()
    {
        // The genuinely quiet tree, and the one the acceptance criteria name
        // explicitly: it must still relax all the way to WalGcInterval.
        var provider = new InMemoryWalStorageProvider();

        var sut = await CollectorAsync(provider, checkpointOffset: 10);

        var (report, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(report.RetainedBacklog, Is.False,
                "an empty shard holds no WAL at all, which is not the same as holding WAL it cannot release.");
        });
    }

    [Test]
    public async Task RunOnceAsync_distinguishes_a_stranded_pass_from_a_quiet_one_that_both_reclaimed_nothing()
    {
        // The discrimination property stated directly, in the same shape the
        // trim-stop fixture states its own. Both passes reclaim zero and, with
        // no byte ceiling configured, both produce a byte-for-byte identical
        // report in every other field the scheduler reads - which is precisely
        // why the scheduler backed both of them off identically.
        var stranded = new InMemoryWalStorageProvider();
        await stranded.AppendBatchAsync(
            Tree, 0, new[] { Entry(5, Hlc(10)) }, CancellationToken.None);

        var quiet = new InMemoryWalStorageProvider();

        var (strandedReport, _) = await RunAsync(await CollectorAsync(stranded, checkpointOffset: 2));
        var (quietReport, _) = await RunAsync(await CollectorAsync(quiet, checkpointOffset: 10));

        Assert.Multiple(() =>
        {
            Assert.That(strandedReport.EntriesTrimmed, Is.Zero);
            Assert.That(quietReport.EntriesTrimmed, Is.Zero);
            Assert.That(strandedReport.CursorFloorState, Is.EqualTo(quietReport.CursorFloorState),
                "the cursor-floor state cannot separate them - both report a usable floor.");
            Assert.That(strandedReport.BytePressureOverThreshold, Is.EqualTo(quietReport.BytePressureOverThreshold),
                "and neither is over a ceiling, because no ceiling is configured.");
        });

        Assert.That(strandedReport.RetainedBacklog, Is.Not.EqualTo(quietReport.RetainedBacklog),
            "the two passes must be separable by the report alone; that collapse is the defect #3213 removes.");
    }
}
