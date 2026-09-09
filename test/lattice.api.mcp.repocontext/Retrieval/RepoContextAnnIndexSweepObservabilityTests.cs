using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the approximate-index sweep's observability: the counter that
/// partitions every sweep outcome, and the transitions that earn a log line.
/// <para>
/// <b>Why this fixture exists.</b> Before it, the arming path emitted nothing at
/// information level in <i>any</i> of its states. A sweep arming successfully
/// logged at debug; a sweep throwing logged at debug and swallowed the exception;
/// a sweep that never started logged nothing because it never ran. On the deployed
/// container all three read the same: across 45,572 log lines, no build
/// coordinator was ever observed armed, the string
/// <c>repo-context-vector-index</c> never appeared at all, and nothing in the log
/// could say which of the three states produced that. These tests pin the
/// distinctions that close the gap.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexSweepObservabilityTests
{
    private static readonly RepoContextAnnSweepOutcome[] AllOutcomes =
    [
        RepoContextAnnSweepOutcome.Armed,
        RepoContextAnnSweepOutcome.Empty,
        RepoContextAnnSweepOutcome.Faulted,
    ];

    [Test]
    public void Every_outcome_is_counted_so_a_zero_on_one_arm_is_denominated_by_a_rising_total()
    {
        // The point of the partition. A counter that only counted successful arming
        // would read zero at the highest rate of the very fault it exists to catch,
        // which is the shape declined on issue #2314. Here the total rises once per
        // sweep whatever happens, so 'armed' at zero beside 'faulted' climbing is a
        // measured absence of arming rather than an absent measurement.
        var measurements = new List<(string Outcome, long Value)>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = ListenTo(reporter, measurements);

        reporter.Record(RepoContextAnnSweepOutcome.Faulted);
        reporter.Record(RepoContextAnnSweepOutcome.Faulted);
        reporter.Record(RepoContextAnnSweepOutcome.Faulted);

        Assert.Multiple(() =>
        {
            Assert.That(measurements, Has.Count.EqualTo(3),
                "every sweep that runs must be counted, including the ones that throw");
            Assert.That(
                measurements.Where(m => m.Outcome == RepoContextAnnIndexSweepReporter.OutcomeFaultedTag).Sum(m => m.Value),
                Is.EqualTo(3));
            Assert.That(
                measurements.Any(m => m.Outcome == RepoContextAnnIndexSweepReporter.OutcomeArmedTag),
                Is.False,
                "'armed' must stay absent while faulting, so its zero is readable against the total");
        });
    }

    [Test]
    public void The_outcome_partition_is_total_over_the_enumeration()
    {
        // Guards the partition against a later value being added and quietly
        // escaping the counter, which would reintroduce an outcome that is not
        // denominated by anything.
        var measurements = new List<(string Outcome, long Value)>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = ListenTo(reporter, measurements);

        foreach (var outcome in Enum.GetValues<RepoContextAnnSweepOutcome>())
        {
            reporter.Record(outcome);
        }

        Assert.Multiple(() =>
        {
            Assert.That(AllOutcomes, Is.EquivalentTo(Enum.GetValues<RepoContextAnnSweepOutcome>()),
                "a new outcome must be added to this fixture's expectations too");
            Assert.That(measurements, Has.Count.EqualTo(Enum.GetValues<RepoContextAnnSweepOutcome>().Length));
            Assert.That(
                measurements.Select(m => m.Outcome).Distinct(),
                Is.EquivalentTo(new[]
                {
                    RepoContextAnnIndexSweepReporter.OutcomeArmedTag,
                    RepoContextAnnIndexSweepReporter.OutcomeEmptyTag,
                    RepoContextAnnIndexSweepReporter.OutcomeFaultedTag,
                }),
                "every outcome must resolve to a distinct bounded tag value");
        });
    }

    [Test]
    public void A_persisting_fault_announces_once_and_counts_the_rest()
    {
        // The swallow used to log at debug, so a deployment at information level saw
        // nothing at all. Raising it unconditionally would write a line every thirty
        // seconds for as long as the fault lasts - roughly 2,880 a day - so the first
        // fault of a run carries the exception and the repetitions go to the counter.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        var first = reporter.Record(RepoContextAnnSweepOutcome.Faulted);
        var second = reporter.Record(RepoContextAnnSweepOutcome.Faulted);
        var hundredth = default(RepoContextAnnSweepReport);
        for (var i = 0; i < 98; i++)
        {
            hundredth = reporter.Record(RepoContextAnnSweepOutcome.Faulted);
        }

        Assert.Multiple(() =>
        {
            Assert.That(first.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.FaultBegan));
            Assert.That(first.ConsecutiveFaults, Is.EqualTo(1));
            Assert.That(second.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.None));
            Assert.That(hundredth.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.None));
            Assert.That(reporter.Read().Faulted, Is.EqualTo(100),
                "the repetitions the log omits must still be counted");
            Assert.That(reporter.Read().ConsecutiveFaults, Is.EqualTo(100));
        });
    }

    [Test]
    public void A_recovery_closes_the_fault_episode_and_reports_its_length()
    {
        // A fault run with no closing line leaves an operator unable to tell an
        // episode that ended from one still in progress.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        for (var i = 0; i < 7; i++)
        {
            reporter.Record(RepoContextAnnSweepOutcome.Faulted);
        }

        var recovery = reporter.Record(RepoContextAnnSweepOutcome.Armed);

        Assert.Multiple(() =>
        {
            Assert.That(recovery.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.Recovered));
            Assert.That(recovery.ConsecutiveFaults, Is.EqualTo(7),
                "the closing line must carry how long the episode lasted");
            Assert.That(reporter.Read().ConsecutiveFaults, Is.Zero, "the run must reset on recovery");
        });
    }

    [Test]
    public void A_second_fault_run_announces_again_rather_than_staying_silent_forever()
    {
        // Announce-once must mean once per episode, not once per process: a flapping
        // sweep that went quiet after its first fault would be indistinguishable from
        // one that recovered and stayed healthy.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        reporter.Record(RepoContextAnnSweepOutcome.Faulted);
        reporter.Record(RepoContextAnnSweepOutcome.Armed);
        var secondEpisode = reporter.Record(RepoContextAnnSweepOutcome.Faulted);

        Assert.That(secondEpisode.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.FaultBegan));
    }

    [Test]
    public void A_sweep_that_armed_nothing_is_announced_separately_from_one_that_armed_something()
    {
        // A sweep that completes with no repository to arm returns cleanly, settles
        // into the long cadence, and schedules nothing - which reads exactly like
        // success in every signal except this one.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        var empty = reporter.Record(RepoContextAnnSweepOutcome.Empty);
        var armed = reporter.Record(RepoContextAnnSweepOutcome.Armed);

        Assert.Multiple(() =>
        {
            Assert.That(empty.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.NoRepositories));
            Assert.That(armed.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.FirstArmed));
            Assert.That(
                reporter.Record(RepoContextAnnSweepOutcome.Armed).Announcement,
                Is.EqualTo(RepoContextAnnSweepAnnouncement.None),
                "the first-armed line is announced once; later sweeps are counted");
        });
    }

    [Test]
    public void The_counter_carries_the_platform_tenant_label_and_a_bounded_outcome_tag()
    {
        // Cardinality: the outcome tag must resolve against a closed set so no
        // unrecognised value can reach the meter as free text.
        var tagSets = new List<KeyValuePair<string, object?>[]>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextAnnIndexSweepReporter.SweepInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) => tagSets.Add(tags.ToArray()));
        listener.Start();

        reporter.Record(RepoContextAnnSweepOutcome.Armed);

        Assert.Multiple(() =>
        {
            Assert.That(tagSets, Has.Count.EqualTo(1));
            Assert.That(
                tagSets[0].Select(t => t.Key),
                Contains.Item(RepoContextAnnIndexSweepReporter.OutcomeTagKey));
            Assert.That(
                tagSets[0].Any(t => t.Key == LatticeTenantLabel.Platform.Key
                    && Equals(t.Value, LatticeTenantLabel.Platform.Value)),
                Is.True,
                "the sweep is host-process work, so it is labelled platform rather than per-tenant");
        });
    }

    [Test]
    public void Describing_an_unrecognised_outcome_falls_back_to_the_faulted_tag()
    {
        // Fail closed on cardinality: an out-of-range cast must not become a new
        // series name.
        Assert.That(
            RepoContextAnnIndexSweepReporter.DescribeOutcome((RepoContextAnnSweepOutcome)int.MaxValue),
            Is.EqualTo(RepoContextAnnIndexSweepReporter.OutcomeFaultedTag));
    }

    [Test]
    public void The_scheduling_decision_names_every_blocking_condition_not_just_the_first()
    {
        // The sentence this replaced named the disjunction - "switch disabled, exact
        // retrieval configured, or no embedding provider bound" - leaving an operator
        // to work out which disjunct held, and to discover only on the next restart
        // that another did too.
        var grainFactory = Substitute.For<IGrainFactory>();
        var allBlocked = new RepoContextAnnIndexScheduler(
            grainFactory,
            new RepoContextIndexingOptions
            {
                AnnIndexScheduling = false,
                SemanticRetrieval = RepoContextSemanticRetrievalMode.Exact,
            },
            NullLogger<RepoContextAnnIndexScheduler>.Instance,
            embedder: null);

        var decision = allBlocked.DescribeSchedulingState();

        Assert.Multiple(() =>
        {
            Assert.That(decision, Does.Contain("embedding provider"));
            Assert.That(decision, Does.Contain(RepoContextIndexingOptions.AnnIndexSchedulingKey));
            Assert.That(decision, Does.Contain(RepoContextIndexingOptions.SemanticRetrievalKey));
        });
    }

    [Test]
    public void The_scheduling_decision_reads_on_when_nothing_blocks_it()
    {
        // The defaults ship scheduling on (AnnIndexScheduling true, retrieval
        // approximate), so a host that has set neither environment variable and has
        // an embedder bound must report 'on' - which is what makes the startup line's
        // presence meaningful on the deployed container, where neither variable is set.
        var scheduler = new RepoContextAnnIndexScheduler(
            Substitute.For<IGrainFactory>(),
            new RepoContextIndexingOptions(),
            NullLogger<RepoContextAnnIndexScheduler>.Instance,
            StubEmbedder.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(scheduler.DescribeSchedulingState(), Is.EqualTo("on"));
            Assert.That(scheduler.CanSchedule, Is.True);
        });
    }

    private static MeterListener ListenTo(
        RepoContextAnnIndexSweepReporter reporter, List<(string Outcome, long Value)> sink)
    {
        _ = reporter;
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextAnnIndexSweepReporter.SweepInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            var outcome = string.Empty;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextAnnIndexSweepReporter.OutcomeTagKey)
                {
                    outcome = tag.Value?.ToString() ?? string.Empty;
                }
            }

            lock (sink)
            {
                sink.Add((outcome, value));
            }
        });
        listener.Start();
        return listener;
    }

    /// <summary>
    /// A minimal embedding provider that advertises a space and nothing else. The
    /// scheduler only ever reads <see cref="IEmbeddingProvider.Space"/>.
    /// </summary>
    private sealed class StubEmbedder : IEmbeddingProvider
    {
        public static StubEmbedder Instance { get; } = new();

        public EmbeddingSpace Space { get; } = new("test-model", 8, normalized: true);

        public Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(true);

        public Task<EmbeddingResult> EmbedAsync(
            IReadOnlyList<string> texts,
            EmbeddingTextType textType,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException("The sweep never embeds.");
    }
}
