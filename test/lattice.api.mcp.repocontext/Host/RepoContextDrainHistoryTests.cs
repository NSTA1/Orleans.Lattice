using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the durable drain record added for issue #2598: the file the shutting-down
/// process writes and the starting process reads, which is the only channel that
/// carries a drain measurement across a restart.
/// </summary>
/// <remarks>
/// <para>
/// The load-bearing property here is not the format, it is that <b>every</b> failure
/// of the format is soft. This store is written on the shutdown path and read on the
/// startup path, so a parse that threw would turn a corrupt diagnostic into a
/// container that cannot start, and a write that threw would cost the remainder of a
/// stop sequence to save a record of it. Both would be strictly worse than having no
/// record at all, which is the outcome these tests pin.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextDrainHistoryTests
{
    private static readonly DateTimeOffset Observed = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    private string _directory = null!;

    [SetUp]
    public void SetUp()
    {
        _directory = Path.Combine(Path.GetTempPath(), "repocontext-drain-history-" + Guid.NewGuid().ToString("N"));
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

    private string Path_() => RepoContextDrainHistory.PathIn(_directory);

    [Test]
    public void The_history_file_lives_under_the_data_directory_the_next_process_will_mount()
    {
        // The record only works because both processes see the same path. A file
        // written anywhere else in the container is lost with the container.
        Assert.That(
            RepoContextDrainHistory.PathIn("/data"),
            Is.EqualTo(Path.Combine("/data", RepoContextDrainHistory.FileName)));
    }

    [Test]
    public void A_completed_drain_round_trips_through_the_file()
    {
        var observation = new RepoContextDrainObservation(
            Observed,
            RepoContextDrainOutcome.Completed,
            TimeSpan.FromSeconds(90),
            TimeSpan.FromSeconds(67.2),
            ResidentActivations: 4321);

        Assert.That(RepoContextDrainHistory.TryWrite(Path_(), observation), Is.True);

        var read = RepoContextDrainHistory.Read(Path_());

        Assert.That(read, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(read!.Value.Outcome, Is.EqualTo(RepoContextDrainOutcome.Completed));
            Assert.That(read.Value.Budget, Is.EqualTo(TimeSpan.FromSeconds(90)));
            Assert.That(read.Value.Duration, Is.EqualTo(TimeSpan.FromSeconds(67.2)));
            Assert.That(read.Value.ResidentActivations, Is.EqualTo(4321));
            Assert.That(read.Value.ObservedAtUtc, Is.EqualTo(Observed));
        });
    }

    [Test]
    public void A_start_marker_round_trips_with_no_duration_because_none_was_ever_measured()
    {
        // This is the shape that carries the killed-mid-drain finding. A duration
        // substituted here - a zero, or the budget - would turn "the process did not
        // survive to measure one" into a measurement, which is the whole finding.
        var observation = new RepoContextDrainObservation(
            Observed,
            RepoContextDrainOutcome.Started,
            TimeSpan.FromSeconds(90),
            Duration: null,
            ResidentActivations: 12);

        Assert.That(RepoContextDrainHistory.TryWrite(Path_(), observation), Is.True);
        var read = RepoContextDrainHistory.Read(Path_());

        Assert.That(read, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(read!.Value.Outcome, Is.EqualTo(RepoContextDrainOutcome.Started));
            Assert.That(read.Value.Duration, Is.Null, "no terminal outcome was recorded, so no duration exists");
            Assert.That(read.Value.ResidentActivations, Is.EqualTo(12));
        });
    }

    [Test]
    public void An_unreadable_residency_round_trips_as_unreadable_rather_than_as_zero()
    {
        // A residency of zero and a residency nobody could read are different facts,
        // and the second must not be promoted to the first: a per-activation cost
        // divided by a fabricated zero would be a division by zero dressed as a
        // measurement.
        var observation = new RepoContextDrainObservation(
            Observed,
            RepoContextDrainOutcome.Completed,
            TimeSpan.FromSeconds(90),
            TimeSpan.FromSeconds(10),
            ResidentActivations: null);

        Assert.That(RepoContextDrainHistory.TryWrite(Path_(), observation), Is.True);

        Assert.That(RepoContextDrainHistory.Read(Path_())!.Value.ResidentActivations, Is.Null);
    }

    [Test]
    public void An_absent_file_reads_as_no_history_rather_than_throwing()
    {
        Assert.That(RepoContextDrainHistory.Read(Path_()), Is.Null);
    }

    [Test]
    public void A_corrupt_file_reads_as_no_history_rather_than_stopping_the_container_starting()
    {
        File.WriteAllText(Path_(), "this is not a drain record\n\0\0garbage");

        Assert.That(RepoContextDrainHistory.Read(Path_()), Is.Null);
    }

    [Test]
    public void A_file_from_a_format_this_build_does_not_recognise_reads_as_no_history()
    {
        // Guessing at an unrecognised version is how a field silently changes meaning
        // across an upgrade. Absent is the honest reading.
        var future = RepoContextDrainHistory
            .Render(new RepoContextDrainObservation(
                Observed,
                RepoContextDrainOutcome.Completed,
                TimeSpan.FromSeconds(90),
                TimeSpan.FromSeconds(10),
                7))
            .Replace(
                $"version={RepoContextDrainHistory.FormatVersion}",
                "version=9999",
                StringComparison.Ordinal);

        File.WriteAllText(Path_(), future);

        Assert.That(RepoContextDrainHistory.Read(Path_()), Is.Null);
    }

    [Test]
    public void A_record_missing_its_outcome_reads_as_no_history()
    {
        Assert.That(
            RepoContextDrainHistory.Parse([$"version={RepoContextDrainHistory.FormatVersion}", "budgetSeconds=90"]),
            Is.Null);
    }

    [Test]
    public void A_record_with_a_nonsense_duration_reads_as_no_history_rather_than_a_negative_drain()
    {
        Assert.That(
            RepoContextDrainHistory.Parse(
            [
                $"version={RepoContextDrainHistory.FormatVersion}",
                "observedAtUtc=2026-03-04T05:06:07.0000000+00:00",
                "outcome=Completed",
                "budgetSeconds=90",
                "durationSeconds=-12",
            ]),
            Is.Null);
    }

    [Test]
    public void An_unrecognised_key_is_ignored_so_a_field_added_later_stays_readable_by_an_older_build()
    {
        var parsed = RepoContextDrainHistory.Parse(
        [
            $"version={RepoContextDrainHistory.FormatVersion}",
            "observedAtUtc=2026-03-04T05:06:07.0000000+00:00",
            "outcome=Abandoned",
            "budgetSeconds=90",
            "durationSeconds=102.1",
            "somethingAddedInAFutureBuild=yes",
        ]);

        Assert.That(parsed, Is.Not.Null);
        Assert.That(parsed!.Value.Duration, Is.EqualTo(TimeSpan.FromSeconds(102.1)));
    }

    [Test]
    public void A_write_to_an_unwritable_path_reports_the_loss_rather_than_throwing_during_a_shutdown()
    {
        // The write runs while the host is stopping. Throwing here would cost the
        // remainder of the stop sequence to save a diagnostic about it.
        var unwritable = Path.Combine(Path_(), "nested-under-a-file", RepoContextDrainHistory.FileName);
        File.WriteAllText(Path_(), "occupied");

        Assert.That(
            RepoContextDrainHistory.TryWrite(unwritable, new RepoContextDrainObservation(
                Observed,
                RepoContextDrainOutcome.Completed,
                TimeSpan.FromSeconds(90),
                TimeSpan.FromSeconds(10),
                1)),
            Is.False);
    }

    [Test]
    public void A_later_record_replaces_the_earlier_one_so_the_file_always_holds_the_last_drain()
    {
        RepoContextDrainHistory.TryWrite(Path_(), new RepoContextDrainObservation(
            Observed,
            RepoContextDrainOutcome.Started,
            TimeSpan.FromSeconds(90),
            Duration: null,
            100));

        RepoContextDrainHistory.TryWrite(Path_(), new RepoContextDrainObservation(
            Observed,
            RepoContextDrainOutcome.Completed,
            TimeSpan.FromSeconds(90),
            TimeSpan.FromSeconds(30),
            100));

        Assert.That(
            RepoContextDrainHistory.Read(Path_())!.Value.Outcome,
            Is.EqualTo(RepoContextDrainOutcome.Completed),
            "the terminal record must supersede the start marker, or every clean stop would look like a kill");
    }

    [Test]
    public void The_per_activation_cost_is_null_when_either_half_of_the_division_is_missing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                new RepoContextDrainObservation(
                    Observed, RepoContextDrainOutcome.Completed, TimeSpan.FromSeconds(90), null, 10)
                    .PerActivationCost,
                Is.Null,
                "no duration means no cost");
            Assert.That(
                new RepoContextDrainObservation(
                    Observed, RepoContextDrainOutcome.Completed, TimeSpan.FromSeconds(90), TimeSpan.FromSeconds(10), null)
                    .PerActivationCost,
                Is.Null,
                "no residency means no cost");
            Assert.That(
                new RepoContextDrainObservation(
                    Observed, RepoContextDrainOutcome.Completed, TimeSpan.FromSeconds(90), TimeSpan.FromSeconds(10), 0)
                    .PerActivationCost,
                Is.Null,
                "a zero residency must not divide by zero and report an infinite cost");
        });
    }

    [Test]
    public void The_per_activation_cost_divides_the_measured_drain_by_the_set_it_got_through()
    {
        var observation = new RepoContextDrainObservation(
            Observed,
            RepoContextDrainOutcome.Abandoned,
            TimeSpan.FromSeconds(90),
            TimeSpan.FromSeconds(102.1),
            10_000);

        Assert.That(
            observation.PerActivationCost!.Value.TotalMilliseconds,
            Is.EqualTo(10.21).Within(0.001));
    }
}
