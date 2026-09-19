using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the durable heap record added for issue #3255: what a run measured about
/// its own memory requirement, carried across a restart so the next process can
/// compare the grant it has been given against a measurement rather than a model.
/// </summary>
/// <remarks>
/// <para>
/// <b>These tests exist mainly to pin a direction, not a format.</b> This store is
/// modelled on <see cref="RepoContextDrainHistory"/>, which fails soft in both
/// directions because "a corrupt diagnostic must never stop a container starting".
/// This one feeds the single check that <i>does</i> stop a container starting, so the
/// same property has to be re-established deliberately rather than assumed to have
/// come along with the shape: every failure to read must produce <i>no record</i>,
/// because no record admits, while a partially-trusted record could refuse. The
/// fixture is deliberately heavy on malformed input for that reason.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextMemoryHistoryTests
{
    private static readonly DateTimeOffset Observed = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    private string _directory = null!;

    [SetUp]
    public void SetUp()
    {
        _directory = Path.Combine(Path.GetTempPath(), "repocontext-heap-history-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_directory))
        {
            try
            {
                Directory.Delete(_directory, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private string HistoryPath() => RepoContextMemoryHistory.PathIn(_directory);

    private static RepoContextMemoryObservation Sample(
        RepoContextMemoryOutcome outcome = RepoContextMemoryOutcome.Completed,
        long limit = 9_663_676_416,
        long peak = 8_000_000_000,
        long events = 0,
        long? exhaustedAt = null,
        long? overriddenAt = null) =>
        new(Observed, outcome, limit, peak, events, exhaustedAt, overriddenAt);

    [Test]
    public void The_record_lives_under_the_data_directory_the_next_process_will_mount()
    {
        // The whole mechanism rests on both processes resolving the same path: a file
        // written anywhere else in the container dies with the container, and the
        // admission check would then admit for ever without ever saying why.
        Assert.That(
            RepoContextMemoryHistory.PathIn("/data"),
            Is.EqualTo(Path.Combine("/data", RepoContextMemoryHistory.FileName)));
    }

    [Test]
    public void A_written_record_reads_back_field_for_field()
    {
        var written = Sample(
            RepoContextMemoryOutcome.Exhausted,
            limit: 9_663_676_416,
            peak: 9_600_000_000,
            events: 304,
            exhaustedAt: 9_663_676_416,
            overriddenAt: 1234);

        Assert.That(RepoContextMemoryHistory.TryWrite(HistoryPath(), written), Is.True);

        var read = RepoContextMemoryHistory.Read(HistoryPath());

        Assert.That(read, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(read!.Value.Outcome, Is.EqualTo(RepoContextMemoryOutcome.Exhausted));
            Assert.That(read.Value.GrantedLimitBytes, Is.EqualTo(9_663_676_416));
            Assert.That(read.Value.PeakCommittedBytes, Is.EqualTo(9_600_000_000));
            Assert.That(read.Value.ExhaustionEvents, Is.EqualTo(304));
            Assert.That(read.Value.ExhaustedAtLimitBytes, Is.EqualTo(9_663_676_416));
            Assert.That(read.Value.OverriddenAtLimitBytes, Is.EqualTo(1234));
            Assert.That(read.Value.ObservedAtUtc, Is.EqualTo(Observed));
        });
    }

    [Test]
    public void The_optional_ceilings_round_trip_as_absent_rather_than_as_zero()
    {
        // Zero is a legal byte count and "never exhausted" is not one, so the two
        // must stay distinguishable across the file. If absence collapsed to zero,
        // every fresh deployment would read as having exhausted at a ceiling of zero
        // bytes, which no grant is smaller than - and so would admit for the wrong
        // reason, silently, for ever.
        Assert.That(RepoContextMemoryHistory.TryWrite(HistoryPath(), Sample()), Is.True);

        var read = RepoContextMemoryHistory.Read(HistoryPath());

        Assert.Multiple(() =>
        {
            Assert.That(read!.Value.ExhaustedAtLimitBytes, Is.Null);
            Assert.That(read.Value.OverriddenAtLimitBytes, Is.Null);
        });
    }

    [Test]
    public void A_rewrite_replaces_the_record_rather_than_appending_to_it()
    {
        Assert.That(RepoContextMemoryHistory.TryWrite(HistoryPath(), Sample(peak: 1)), Is.True);
        Assert.That(RepoContextMemoryHistory.TryWrite(HistoryPath(), Sample(peak: 2)), Is.True);

        Assert.That(RepoContextMemoryHistory.Read(HistoryPath())!.Value.PeakCommittedBytes, Is.EqualTo(2));
    }

    [Test]
    public void A_missing_file_reads_as_no_record_rather_than_throwing()
    {
        // No record admits. That is the correct behaviour on a first-ever start, and
        // it is the behaviour every other read failure below is required to converge
        // on.
        Assert.That(RepoContextMemoryHistory.Read(HistoryPath()), Is.Null);
    }

    [Test]
    public void A_record_from_an_unrecognised_format_version_reads_as_no_record()
    {
        // A future version may mean anything at all, including something that would
        // make this process refuse for a reason that no longer exists. Declining to
        // interpret it is the only safe reading.
        File.WriteAllLines(
            HistoryPath(),
            [
                "version=99",
                "observedAtUtc=2026-03-04T05:06:07.0000000+00:00",
                "outcome=Exhausted",
                "grantedLimitBytes=1",
                "peakCommittedBytes=1",
                "exhaustionEvents=1",
                "exhaustedAtLimitBytes=1",
            ]);

        Assert.That(RepoContextMemoryHistory.Read(HistoryPath()), Is.Null);
    }

    [Test]
    public void A_truncated_record_reads_as_no_record()
    {
        // A write interrupted by the very out-of-memory condition being recorded is
        // the most likely way this file gets damaged, so it is the case that must not
        // become a refusal.
        File.WriteAllLines(HistoryPath(), ["version=1", "outcome=Exha"]);

        Assert.That(RepoContextMemoryHistory.Read(HistoryPath()), Is.Null);
    }

    [TestCase("grantedLimitBytes=not-a-number")]
    [TestCase("exhaustionEvents=")]
    [TestCase("outcome=Annihilated")]
    [TestCase("observedAtUtc=yesterday")]
    public void An_unparseable_field_reads_as_no_record_rather_than_as_a_partial_one(string corruption)
    {
        // Partial trust is the dangerous option: a record whose outcome parsed but
        // whose ceiling did not could refuse against a garbage number. All or
        // nothing, and nothing admits.
        var key = corruption.Split('=')[0];
        var lines = RepoContextMemoryHistory
            .Render(Sample(RepoContextMemoryOutcome.Exhausted, exhaustedAt: 9_663_676_416))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.TrimEnd('\r'))
            .Select(line => line.StartsWith(key + "=", StringComparison.Ordinal) ? corruption : line)
            .ToArray();

        File.WriteAllLines(HistoryPath(), lines);

        Assert.That(RepoContextMemoryHistory.Read(HistoryPath()), Is.Null);
    }

    [Test]
    public void An_unwritable_path_writes_false_rather_than_throwing()
    {
        // Failing to record a measurement must never become a failure to run: this is
        // called on the startup path and from a sampling timer, and a throw from
        // either would turn a diagnostic into an outage. A file sitting where the
        // directory needs to be is the cheapest way to make the write genuinely
        // impossible - a merely missing directory is created, which is deliberate,
        // because the data mount may be fresh.
        var blocker = Path.Combine(_directory, "blocker");
        File.WriteAllText(blocker, "not a directory");

        Assert.That(
            RepoContextMemoryHistory.TryWrite(Path.Combine(blocker, "heap-history.txt"), Sample()),
            Is.False);
    }

    [Test]
    public void A_missing_directory_is_created_rather_than_failing_the_write()
    {
        // The data mount can be fresh on a first start, and losing the very first
        // measurement would mean the first run that exhausts records nothing.
        var nested = Path.Combine(_directory, "fresh-mount", "heap-history.txt");

        Assert.That(RepoContextMemoryHistory.TryWrite(nested, Sample()), Is.True);
        Assert.That(RepoContextMemoryHistory.Read(nested), Is.Not.Null);
    }

    [Test]
    public void An_unreadable_path_reads_as_no_record_rather_than_throwing()
    {
        var unreadable = Path.Combine(_directory, "missing-subdirectory", "heap-history.txt");

        Assert.That(RepoContextMemoryHistory.Read(unreadable), Is.Null);
    }

    [Test]
    public void Peak_occupancy_is_absent_rather_than_infinite_when_no_ceiling_was_observed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Sample(limit: 0, peak: 5).PeakOccupancyRatio, Is.Null);
            Assert.That(Sample(limit: 100, peak: 50).PeakOccupancyRatio, Is.EqualTo(0.5d));
        });
    }

    [Test]
    public void The_worst_exhaustion_ceiling_is_carried_forward_rather_than_replaced()
    {
        // This is the property that stops the record erasing its own evidence. Raise
        // a grant that exhausted at 9 GiB to one ceilinged at 13.5 GiB and the run
        // succeeds, writes an Admitted record, and - without this carry-forward -
        // deletes the proof that 9 GiB failed. Drop back to the original grant and
        // the container would start happily into a configuration already measured not
        // to work.
        var previous = Sample(RepoContextMemoryOutcome.Exhausted, exhaustedAt: 9_663_676_416);
        var thisRun = Sample(RepoContextMemoryOutcome.Admitted, limit: 14_495_514_624, exhaustedAt: null);

        Assert.That(thisRun.MergeExhaustionHighWater(previous), Is.EqualTo(9_663_676_416));
    }

    [Test]
    public void The_carried_exhaustion_ceiling_is_a_high_water_mark_not_the_newest_value()
    {
        // A later, smaller exhaustion does not retire a larger one: the largest
        // ceiling ever proved insufficient is the one a grant has to beat.
        var previous = Sample(RepoContextMemoryOutcome.Exhausted, exhaustedAt: 9_663_676_416);
        var thisRun = Sample(RepoContextMemoryOutcome.Exhausted, limit: 4_000_000_000, exhaustedAt: 4_000_000_000);

        Assert.That(thisRun.MergeExhaustionHighWater(previous), Is.EqualTo(9_663_676_416));
    }

    [Test]
    public void A_first_exhaustion_is_carried_when_there_is_nothing_to_merge_with()
    {
        var thisRun = Sample(RepoContextMemoryOutcome.Exhausted, exhaustedAt: 9_663_676_416);

        Assert.Multiple(() =>
        {
            Assert.That(thisRun.MergeExhaustionHighWater(null), Is.EqualTo(9_663_676_416));
            Assert.That(Sample().MergeExhaustionHighWater(null), Is.Null);
        });
    }
}
