using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the per-repository arming counter, <c>repocontext.ann.sweep.arming</c>.
/// <para>
/// <b>Why this fixture exists.</b> The sweep outcome counter partitions
/// <i>sweeps</i>, and a sweep visits many repositories. That made a whole outcome
/// class unobservable, which is issue #2751. A timeout from a coordinator already
/// inside a long build turn is deliberately not a fault - that classification is
/// correct and issue #2252 is the record of what happens when it is not - but "not
/// a fault" had been implemented as "not counted anywhere", so the deferral reached
/// only a log line. The consequences were two, and the second is the one that
/// matters: where a sweep armed something, its deferrals vanished outright, and
/// where it armed nothing, a wholly wedged build plane fell through to the
/// <c>empty</c> arm and became indistinguishable from a store with no repositories
/// in it.
/// </para>
/// <para>
/// These tests pin the counter that separates them, and pin the separation itself -
/// that this instrument is a different population from the sweep counter and never
/// a decomposition of it.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexSweepArmingObservabilityTests
{
    [Test]
    public void Every_arming_arm_is_minted_at_zero_when_the_reporter_is_constructed()
    {
        // The listener has to exist before the reporter does: the pre-mint happens
        // in the constructor, so a listener started afterwards sees nothing and the
        // fixture would pass while asserting over an empty set.
        var observed = new List<KeyValuePair<string, long>>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(
                        instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(
                        instrument.Name,
                        RepoContextAnnIndexSweepReporter.ArmingInstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? result = null;
            foreach (var tag in tags)
            {
                if (string.Equals(
                        tag.Key, RepoContextAnnIndexSweepReporter.ArmingResultTagKey, StringComparison.Ordinal))
                {
                    result = tag.Value?.ToString();
                }
            }

            lock (observed)
            {
                observed.Add(new KeyValuePair<string, long>(result ?? "<untagged>", value));
            }
        });

        listener.Start();

        // Construct and immediately dispose. Nothing records an arming call, so
        // every measurement seen below can only have come from the constructor.
        using (var reporter = new RepoContextAnnIndexSweepReporter())
        {
            _ = reporter.Read();
        }

        listener.Dispose();

        List<KeyValuePair<string, long>> minted;
        lock (observed)
        {
            minted = [.. observed];
        }

        var arms = minted.Select(m => m.Key).ToArray();

        // Reflected over the enum rather than listed by hand, so an arm added later
        // cannot be left unminted by a fixture that still passes over the arms that
        // came before it. Non-emptiness is asserted first so the reflection can
        // never go vacuously green.
        var expectedArms = Enum.GetValues<RepoContextAnnArmingResult>()
            .Select(RepoContextAnnIndexSweepReporter.DescribeArmingResult)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(minted, Is.Not.Empty,
                "positive control: the listener must have observed measurements, or every "
                + "assertion below passes over an empty set and pins nothing");
            Assert.That(expectedArms, Is.Not.Empty,
                "positive control: the reflection must find members, or the arm assertion "
                + "below passes over an empty set");
            Assert.That(minted.Select(m => m.Value), Is.All.Zero,
                "a pre-mint must not move the reading it is minting, or the counter starts "
                + "life lying about work that never happened");
            Assert.That(arms, Is.EquivalentTo(expectedArms),
                "every arm must exist, at zero, on the first scrape. The deferred arm's zero "
                + "is the whole diagnosis - it is what separates 'every coordinator is busy' "
                + "from 'the store is empty' - and a claim only a present series can make");
        });
    }

    [Test]
    public void The_arming_partition_is_total_over_the_enumeration()
    {
        // Guards against a result being added later and quietly escaping the
        // counter, which would put a repository visit into no arm at all and break
        // the property the total denominator depends on.
        var observed = new List<string>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = ListenToArming(observed);

        var all = Enum.GetValues<RepoContextAnnArmingResult>();
        foreach (var result in all)
        {
            reporter.RecordArming(result);
        }

        Assert.Multiple(() =>
        {
            Assert.That(all, Is.Not.Empty,
                "positive control: the enumeration must have members, or everything below "
                + "passes over an empty set");
            Assert.That(observed, Has.Count.EqualTo(all.Length),
                "every result must reach the counter exactly once");
            Assert.That(observed.Distinct().Count(), Is.EqualTo(all.Length),
                "every result must resolve to a DISTINCT bounded tag value, or two of them "
                + "collapse into one series and the partition stops being readable");
        });
    }

    [Test]
    public void An_unmapped_arming_result_fails_open_onto_the_faulted_arm()
    {
        // A branch nobody mapped is closer to a failure than to a success. Reading
        // as 'armed' would overstate exactly the thing this instrument is trusted
        // for, so the fail-open direction is asserted rather than left to chance.
        var observed = new List<string>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = ListenToArming(observed);

        reporter.RecordArming((RepoContextAnnArmingResult)999);

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextAnnIndexSweepReporter.DescribeArmingResult((RepoContextAnnArmingResult)999),
                Is.EqualTo(RepoContextAnnIndexSweepReporter.ResultFaultedTag));
            Assert.That(observed, Is.EqualTo(new[] { RepoContextAnnIndexSweepReporter.ResultFaultedTag }),
                "an out-of-range cast must land on the arm that understates nothing");
            Assert.That(reporter.Read().Arming.Faulted, Is.EqualTo(1),
                "the tally must land on the same arm as the meter, or the two disagree about "
                + "where the increment went and neither can be trusted");
        });
    }

    [Test]
    public void The_arming_tally_totals_the_repository_visits_and_reads_back_by_result()
    {
        // The total is the denominator. Without it an arm is a bare count with no
        // population to be a proportion of, which is the state the sweep counter's
        // deferrals were already in.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        reporter.RecordArming(RepoContextAnnArmingResult.Armed);
        reporter.RecordArming(RepoContextAnnArmingResult.Deferred);
        reporter.RecordArming(RepoContextAnnArmingResult.Deferred);
        reporter.RecordArming(RepoContextAnnArmingResult.Faulted);

        var arming = reporter.Read().Arming;

        Assert.Multiple(() =>
        {
            Assert.That(arming.Armed, Is.EqualTo(1));
            Assert.That(arming.Deferred, Is.EqualTo(2));
            Assert.That(arming.Faulted, Is.EqualTo(1));
            Assert.That(arming.Total, Is.EqualTo(4),
                "the total must be the number of repository visits, so every arm is readable "
                + "as a proportion of it");

            foreach (var result in Enum.GetValues<RepoContextAnnArmingResult>())
            {
                Assert.That(arming.For(result), Is.GreaterThan(0),
                    $"the '{result}' arm must be readable through For, or a caller has to "
                    + "switch on the enum itself and can silently miss a member");
            }
        });
    }

    [Test]
    public void Recording_an_arming_call_does_not_move_the_sweep_outcome_counter()
    {
        // The separation, asserted in both directions. These two instruments count
        // different populations - repository visits here, completed sweeps there -
        // so neither decomposes the other, and a reader who treats one as a
        // refinement of the other is back to the reading issue #2751 is about.
        // If a later change folded arming onto the sweep counter, this is what
        // catches it.
        using var reporter = new RepoContextAnnIndexSweepReporter();

        reporter.RecordArming(RepoContextAnnArmingResult.Deferred);
        reporter.RecordArming(RepoContextAnnArmingResult.Deferred);
        reporter.RecordArming(RepoContextAnnArmingResult.Armed);

        var afterArming = reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(afterArming.Arming.Total, Is.EqualTo(3),
                "positive control: the arming calls must have been recorded, or the "
                + "assertions below pass because nothing happened at all");
            Assert.That(afterArming.Armed, Is.Zero,
                "arming a coordinator is not a completed sweep and must not advance the "
                + "sweep partition");
            Assert.That(afterArming.Empty, Is.Zero);
            Assert.That(afterArming.Faulted, Is.Zero,
                "a deferral must not reach the sweep's faulted arm - that is the false "
                + "failure signal issue #2252 records");
        });

        reporter.RecordCompleted(RepoContextAnnSweepOutcome.Armed);
        var afterSweep = reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(afterSweep.Armed, Is.EqualTo(1),
                "positive control: the sweep must have been recorded");
            Assert.That(afterSweep.Arming.Total, Is.EqualTo(3),
                "completing a sweep must not advance the per-repository counter, or one "
                + "sweep over ten repositories and ten sweeps over one become the same "
                + "reading");
        });
    }

    [Test]
    public void The_arming_counter_tags_by_result_and_never_by_the_sweep_counters_outcome_key()
    {
        // The tag key is load-bearing and the reason is not local to this file.
        // Both instruments render with the prefix `repocontext_ann_sweep`, so a
        // name-prefix selector reaches both whatever they are called. What stops a
        // dashboard summing them is that they share no tag key to group by: a
        // `sum by (outcome)` cannot fold in a series tagged `result`.
        //
        // Asserting the two constants against each other is not enough on its own,
        // because renaming one keeps every relation between them true while
        // silently breaking the property. The literal is asserted too, so a rename
        // is red rather than invisible.
        var observed = new List<string>();
        var keys = new List<string>();
        using var reporter = new RepoContextAnnIndexSweepReporter();
        using var listener = ListenToArming(observed, keys);

        reporter.RecordArming(RepoContextAnnArmingResult.Deferred);

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextAnnIndexSweepReporter.ArmingResultTagKey, Is.EqualTo("result"),
                "the arming counter must tag by 'result'. Renaming it to the sweep counter's "
                + "key would let one selector group two incommensurable populations - "
                + "repository visits and completed sweeps - into a plausible-looking sum that "
                + "nobody questions");
            Assert.That(RepoContextAnnIndexSweepReporter.OutcomeTagKey, Is.EqualTo("outcome"),
                "positive control: the sweep counter's key is the one being kept distinct, so "
                + "if it moved the assertion above would be guarding nothing");
            Assert.That(
                RepoContextAnnIndexSweepReporter.ArmingResultTagKey,
                Is.Not.EqualTo(RepoContextAnnIndexSweepReporter.OutcomeTagKey),
                "the two instruments must share no tag key, which is the whole protection");
            Assert.That(keys, Is.Not.Empty,
                "positive control: a measurement must have been observed, or the assertions "
                + "below pass over an empty set");
            Assert.That(keys, Does.Contain("result"),
                "the key must reach the exported series, not merely exist as a constant");
            Assert.That(keys, Does.Not.Contain(RepoContextAnnIndexSweepReporter.OutcomeTagKey),
                "the exported series must carry no 'outcome' dimension at all, which is what "
                + "makes it unreachable by a selector grouping the sweep counter");
            Assert.That(keys, Does.Contain(LatticeTenantLabel.Platform.Key),
                "positive control on the tag set itself: this instrument is a platform "
                + "sentinel and carries the derived tenant label like its siblings, so the "
                + "assertions above are being made over the real emitted tags rather than a "
                + "single-tag measurement that happens to agree");
        });
    }

    /// <summary>
    /// Starts a listener over the arming instrument and collects the result tag of
    /// every measurement. Constructed after the reporter, so the constructor's
    /// pre-mints are deliberately not observed - a fixture counting recorded calls
    /// must not have four zero-valued mints folded into its total.
    /// </summary>
    /// <param name="sink">Receives the result tag of each measurement.</param>
    /// <param name="keySink">Optionally receives the tag key of each measurement.</param>
    /// <returns>The started listener.</returns>
    private static MeterListener ListenToArming(List<string> sink, List<string>? keySink = null)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (string.Equals(
                    instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                && string.Equals(
                    instrument.Name,
                    RepoContextAnnIndexSweepReporter.ArmingInstrumentName,
                    StringComparison.Ordinal))
            {
                l.EnableMeasurementEvents(instrument);
            }
        };

        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            var result = string.Empty;
            foreach (var tag in tags)
            {
                if (keySink is not null)
                {
                    lock (keySink)
                    {
                        keySink.Add(tag.Key);
                    }
                }

                if (string.Equals(
                        tag.Key, RepoContextAnnIndexSweepReporter.ArmingResultTagKey, StringComparison.Ordinal))
                {
                    result = tag.Value?.ToString() ?? string.Empty;
                }
            }

            lock (sink)
            {
                sink.Add(result);
            }
        });

        listener.Start();
        return listener;
    }
}
