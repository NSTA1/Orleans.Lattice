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

    private static readonly RepoContextAnnSweepFaultCause[] AllCauses =
    [
        RepoContextAnnSweepFaultCause.Unexpected,
        RepoContextAnnSweepFaultCause.AuthorityUnavailable,
        RepoContextAnnSweepFaultCause.ListingUnavailable,
        RepoContextAnnSweepFaultCause.PlaneRejected,
        RepoContextAnnSweepFaultCause.DependencyUnavailable,
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

        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);

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
            if (outcome == RepoContextAnnSweepOutcome.Faulted)
            {
                reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
            }
            else
            {
                reporter.RecordCompleted(outcome);
            }
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

        var first = reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        var second = reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        var hundredth = default(RepoContextAnnSweepReport);
        for (var i = 0; i < 98; i++)
        {
            hundredth = reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
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
            reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        }

        var recovery = reporter.RecordCompleted(RepoContextAnnSweepOutcome.Armed);

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

        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        reporter.RecordCompleted(RepoContextAnnSweepOutcome.Armed);
        var secondEpisode = reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);

        Assert.That(secondEpisode.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.FaultBegan));
    }

    [Test]
    public void A_sweep_that_armed_nothing_is_announced_separately_from_one_that_armed_something()
    {
        // A sweep that completes without arming anything returns cleanly, settles
        // into the long cadence, and schedules nothing - which reads exactly like
        // success in every signal except this one.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        var empty = reporter.RecordCompleted(RepoContextAnnSweepOutcome.Empty);
        var armed = reporter.RecordCompleted(RepoContextAnnSweepOutcome.Armed);

        Assert.Multiple(() =>
        {
            Assert.That(empty.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.ArmedNothing));
            Assert.That(armed.Announcement, Is.EqualTo(RepoContextAnnSweepAnnouncement.FirstArmed));
            Assert.That(
                reporter.RecordCompleted(RepoContextAnnSweepOutcome.Armed).Announcement,
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

        reporter.RecordCompleted(RepoContextAnnSweepOutcome.Armed);

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

    [Test]
    public void Every_fault_cause_reaches_the_meter_as_its_own_bounded_tag_value()
    {
        // Acceptance criterion 1 of issue #2578. The final scrape of the gate run 2
        // container read 'armed 5, faulted 4': four faults, one number, and four
        // causes underneath it that need four different responses. Each must be
        // separately reachable or the reader supplies a cause, and the one a reader
        // supplies is always the benign one.
        var measurements = new List<(string Outcome, string? Cause)>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = ListenToCauses(measurements);

        foreach (var cause in Enum.GetValues<RepoContextAnnSweepFaultCause>())
        {
            reporter.RecordFaulted(cause);
        }

        Assert.Multiple(() =>
        {
            Assert.That(AllCauses, Is.EquivalentTo(Enum.GetValues<RepoContextAnnSweepFaultCause>()),
                "a new cause must be added to this fixture's expectations too");
            Assert.That(
                measurements.Select(m => m.Cause).Distinct(),
                Is.EquivalentTo(new[]
                {
                    RepoContextAnnIndexSweepReporter.CauseUnexpectedTag,
                    RepoContextAnnIndexSweepReporter.CauseAuthorityUnavailableTag,
                    RepoContextAnnIndexSweepReporter.CauseListingUnavailableTag,
                    RepoContextAnnIndexSweepReporter.CausePlaneRejectedTag,
                    RepoContextAnnIndexSweepReporter.CauseDependencyUnavailableTag,
                }),
                "every cause must resolve to a distinct bounded tag value");
            Assert.That(
                measurements.Select(m => m.Outcome).Distinct().Single(),
                Is.EqualTo(RepoContextAnnIndexSweepReporter.OutcomeFaultedTag),
                "the cause dimension refines the faulted arm and must not appear under another outcome");
        });
    }

    [Test]
    public void A_completing_sweep_carries_no_cause_tag_at_all()
    {
        // The paired negative for the test above, and the one that gives it its
        // meaning: an assertion that can only fire positively cannot tell "the
        // dimension landed" from "the check is broken". If the cause tag leaked onto
        // the completing arms, the test above would still pass while the armed and
        // empty series silently doubled their cardinality.
        var measurements = new List<(string Outcome, string? Cause)>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = ListenToCauses(measurements);

        reporter.RecordCompleted(RepoContextAnnSweepOutcome.Armed);
        reporter.RecordCompleted(RepoContextAnnSweepOutcome.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(measurements, Has.Count.EqualTo(2));
            Assert.That(
                measurements.Where(m => m.Cause is not null),
                Is.Empty,
                "'armed' and 'empty' must keep exactly the cardinality they had before the cause dimension");
            var tally = reporter.Read().FaultedByCause;
            foreach (var cause in Enum.GetValues<RepoContextAnnSweepFaultCause>())
            {
                Assert.That(tally.For(cause), Is.Zero,
                    $"no completing sweep may increment the '{cause}' arm");
            }
        });
    }

    [Test]
    public void The_cause_partition_sums_to_the_faulted_arm()
    {
        // Totality, which is what makes a future unclassified fault path detectable
        // rather than merely undocumented: a path that faulted without a cause would
        // break this equality rather than quietly landing on a benign-looking arm.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.AuthorityUnavailable);
        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.ListingUnavailable);
        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.PlaneRejected);
        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.DependencyUnavailable);
        reporter.RecordFaulted(RepoContextAnnSweepFaultCause.Unexpected);

        var snapshot = reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Faulted, Is.EqualTo(6));
            Assert.That(snapshot.FaultedByCause.Total, Is.EqualTo(snapshot.Faulted),
                "every faulted sweep must land on exactly one cause");
            Assert.That(snapshot.FaultedByCause.ListingUnavailable, Is.EqualTo(2));
            Assert.That(snapshot.FaultedByCause.AuthorityUnavailable, Is.EqualTo(1));
            Assert.That(snapshot.Armed, Is.Zero);
            Assert.That(snapshot.Empty, Is.Zero);
        });
    }

    [Test]
    public void Recording_one_cause_leaves_every_other_cause_reading_zero()
    {
        // The paired negative for the tally. A For(...) that ignored its argument, or
        // an increment that fanned out across the arms, would satisfy every positive
        // assertion above and still be useless to a reader trying to tell four faults
        // apart.
        foreach (var recorded in Enum.GetValues<RepoContextAnnSweepFaultCause>())
        {
            using var reporter = new RepoContextAnnIndexSweepReporter();
            reporter.RecordFaulted(recorded);
            var tally = reporter.Read().FaultedByCause;

            Assert.Multiple(() =>
            {
                Assert.That(tally.For(recorded), Is.EqualTo(1), $"'{recorded}' must be counted");
                foreach (var other in Enum.GetValues<RepoContextAnnSweepFaultCause>())
                {
                    if (other != recorded)
                    {
                        Assert.That(tally.For(other), Is.Zero,
                            $"recording '{recorded}' must leave '{other}' at zero");
                    }
                }
            });
        }
    }

    [Test]
    public void A_fault_cannot_be_counted_through_the_completing_entry_point()
    {
        // Acceptance criterion 2 - that no path may emit a default or empty cause -
        // held structurally rather than by discipline. A single
        // Record(outcome, cause = default) would let a new fault path compile while
        // emitting the default value, and a default is exactly how the next reader is
        // handed a benign-looking number again.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => reporter.RecordCompleted(RepoContextAnnSweepOutcome.Faulted),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(reporter.Read().Faulted, Is.Zero,
                "a rejected recording must not be counted either");
        });
    }

    [Test]
    public void Describing_an_unrecognised_cause_falls_back_to_the_unexpected_tag()
    {
        // Fail closed on cardinality, and fail open onto the arm that pages rather
        // than onto one with a benign explanation.
        Assert.That(
            RepoContextAnnIndexSweepReporter.DescribeCause((RepoContextAnnSweepFaultCause)int.MaxValue),
            Is.EqualTo(RepoContextAnnIndexSweepReporter.CauseUnexpectedTag));
    }

    [Test]
    public void No_recognised_cause_describes_itself_as_unexpected()
    {
        // The paired negative for the fallback. A DescribeCause that returned the
        // unexpected tag for everything would satisfy the test above perfectly, and
        // would collapse the whole vocabulary back into the single undiagnosable
        // number this change exists to split.
        Assert.Multiple(() =>
        {
            foreach (var cause in Enum.GetValues<RepoContextAnnSweepFaultCause>())
            {
                if (cause == RepoContextAnnSweepFaultCause.Unexpected)
                {
                    continue;
                }

                Assert.That(
                    RepoContextAnnIndexSweepReporter.DescribeCause(cause),
                    Is.Not.EqualTo(RepoContextAnnIndexSweepReporter.CauseUnexpectedTag),
                    $"'{cause}' is a recognised cause and must not read as unclassified");
            }
        });
    }

    private static MeterListener ListenToCauses(List<(string Outcome, string? Cause)> sink)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextAnnIndexSweepReporter.SweepInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            var outcome = string.Empty;
            string? cause = null;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextAnnIndexSweepReporter.OutcomeTagKey)
                {
                    outcome = tag.Value?.ToString() ?? string.Empty;
                }
                else if (tag.Key == RepoContextAnnIndexSweepReporter.CauseTagKey)
                {
                    cause = tag.Value?.ToString();
                }
            }

            lock (sink)
            {
                sink.Add((outcome, cause));
            }
        });
        listener.Start();
        return listener;
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
