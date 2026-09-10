using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests that every arm of the two partitioned repository-context counters exists
/// from the moment its reporter is constructed, and that the descriptions which
/// invite a reader to interpret a zero also name the condition under which that
/// interpretation stops holding.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2515. Both instruments carry a description telling the reader that a zero
/// on one arm beside a non-zero total is a <i>measured absence</i> rather than an
/// <i>absent measurement</i>. That guarantee is void when the Prometheus collector
/// has reached a series ceiling: a series whose <b>first</b> occurrence falls after
/// saturation is refused at creation and never appears, while series that already
/// exist keep updating. The exposition therefore looks busy and complete, and the
/// only thing missing is the arm that had never been exercised - which is precisely
/// the arm the description invites the reader to read as a measured zero.
/// </para>
/// <para>
/// Pre-minting is the structural half of the fix and is what these tests defend.
/// Every arm is created with a zero-valued add in its reporter's constructor, so
/// its first occurrence is at process start, long before saturation is plausible.
/// The prose half is defended too: a description that promises the reading must
/// also name the two series that establish whether the collector is saturated, and
/// the one description that named a specific wrong root cause for a saturated
/// reading must not name it again.
/// </para>
/// <para>
/// Every assertion here is paired with a control that fails if the listener is not
/// observing anything, because "the arm reads zero" and "nothing was observed at
/// all" are the same reading to a broken harness - which is the defect family this
/// whole fixture exists to close.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextPreMintedArmsTests
{
    private const string SaturationGaugeName = "lattice_metrics_series";
    private const string SaturationCounterName = "lattice_metrics_dropped_measurements_by_family_total";

    [Test]
    public void Every_sweep_outcome_arm_is_present_at_zero_on_a_fresh_reporter()
    {
        using var arms = new ArmObserver(
            RepoContextAnnIndexSweepReporter.SweepInstrumentName,
            RepoContextAnnIndexSweepReporter.OutcomeTagKey);

        using var reporter = new RepoContextAnnIndexSweepReporter();

        Assert.Multiple(() =>
        {
            Assert.That(
                arms.Observed,
                Is.EquivalentTo(new[]
                {
                    RepoContextAnnIndexSweepReporter.OutcomeArmedTag,
                    RepoContextAnnIndexSweepReporter.OutcomeEmptyTag,
                    RepoContextAnnIndexSweepReporter.OutcomeFaultedTag,
                }),
                "Every arm of the outcome partition has to exist from construction. An arm that is created "
                + "on its first occurrence is refused outright once the collector has reached a series "
                + "ceiling, so on a long-lived host the never-yet-exercised arm - the one the description "
                + "invites the reader to read as a measured zero - is the one most likely to be missing.");
            Assert.That(
                arms.Value(RepoContextAnnIndexSweepReporter.OutcomeFaultedTag),
                Is.Zero,
                "Pre-minting must not fabricate a count. The arm exists and reads zero; it does not read one.");
        });
    }

    [Test]
    public void Every_plane_state_arm_is_present_at_zero_on_a_fresh_reporter()
    {
        using var arms = new ArmObserver(
            RepoContextRetrievalGuardReporter.AnnSearchInstrumentName,
            RepoContextRetrievalGuardReporter.StateTagKey);

        using var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(
                arms.Observed,
                Is.EquivalentTo(new[]
                {
                    RepoContextRetrievalGuardReporter.StateBootstrappingTag,
                    RepoContextRetrievalGuardReporter.StateExhaustiveTag,
                    RepoContextRetrievalGuardReporter.StateApproximateTag,
                }),
                "'approximate' is the state issue #2252 says has never been observed in any deployment, so "
                + "it is the arm that would still be waiting for its first occurrence when a ceiling is "
                + "reached, and the arm an operator is most likely to read a zero from.");
            Assert.That(
                arms.Value(RepoContextRetrievalGuardReporter.StateApproximateTag),
                Is.Zero);
        });
    }

    [Test]
    public void A_pre_minted_arm_still_advances_when_the_outcome_it_names_occurs()
    {
        using var arms = new ArmObserver(
            RepoContextRetrievalGuardReporter.AnnSearchInstrumentName,
            RepoContextRetrievalGuardReporter.StateTagKey);

        using var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);
        reporter.RecordPlaneOutcome("acme", RepoContextAnnServingState.Approximate);

        Assert.Multiple(() =>
        {
            Assert.That(
                arms.Value(RepoContextRetrievalGuardReporter.StateApproximateTag),
                Is.EqualTo(1),
                "The control for the two tests above: this is what proves the observer records values at "
                + "all, so their 'the arm exists and reads zero' is an observation rather than the silence "
                + "of a harness that is measuring nothing.");
            Assert.That(
                arms.Value(RepoContextRetrievalGuardReporter.StateExhaustiveTag),
                Is.Zero,
                "The unexercised arms stay at zero. Pre-minting makes a zero readable; it does not make "
                + "every arm move together.");
        });
    }

    [Test]
    public void Every_description_that_promises_a_measured_zero_names_the_condition_that_voids_it()
    {
        using var descriptions = new DescriptionObserver(
            RepoContextAnnIndexSweepReporter.SweepInstrumentName,
            RepoContextRetrievalGuardReporter.AnnSearchInstrumentName);

        using var sweeps = new RepoContextAnnIndexSweepReporter();
        using var guards = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        Assert.That(
            descriptions.Captured,
            Has.Count.EqualTo(2),
            "Both instruments must have been published and their descriptions captured, or the assertions "
            + "below are inspecting an empty set and pass for that reason alone.");

        Assert.Multiple(() =>
        {
            foreach (var (instrument, description) in descriptions.Captured)
            {
                Assert.That(
                    description,
                    Does.Contain("measured absence"),
                    $"'{instrument}' is one of the instruments whose description makes the measured-absence "
                    + "promise. If that wording has gone, this guard is pinning a promise that is no longer "
                    + "made and should be revisited rather than left asserting nothing.");
                Assert.That(
                    description,
                    Does.Contain(SaturationGaugeName),
                    $"'{instrument}' promises the reader that a zero on one arm is a measured absence. That "
                    + "holds only while the collector is unsaturated, so the description has to name the "
                    + $"series that establishes it: '{SaturationGaugeName}'.");
                Assert.That(
                    description,
                    Does.Contain(SaturationCounterName),
                    $"'{instrument}' must also name '{SaturationCounterName}', which is the series that "
                    + "says a ceiling was actually reached rather than merely approached.");
            }
        });
    }

    [Test]
    public void The_sweep_description_no_longer_names_a_wrong_root_cause_for_an_all_zero_reading()
    {
        using var descriptions = new DescriptionObserver(
            RepoContextAnnIndexSweepReporter.SweepInstrumentName);

        using var sweeps = new RepoContextAnnIndexSweepReporter();

        Assert.That(
            descriptions.Captured,
            Has.Count.EqualTo(1),
            "The sweep instrument was not published, so this guard read no description at all.");

        var description = descriptions.Captured.Single().Description;

        Assert.That(
            description,
            Does.Not.Contain("is not running at all"),
            "The description used to read 'All three series reading zero means the sweep loop is not "
            + "running at all'. That is exactly the reading a saturated collector produces when the "
            + "counter's first occurrence was refused, and it sent the reader to diagnose a sweep fault "
            + "when the fault was in the collector. Worse, it offered a corroborating check - the startup "
            + "line - that would agree with the wrong conclusion, because the sweep loop really is running "
            + "and really did log its startup (issue #2515).");
    }

    /// <summary>
    /// Records the tag values seen on one instrument, and the summed value per tag
    /// value, from before the instrument's owner is constructed.
    /// </summary>
    /// <remarks>
    /// Distinguishing "this arm exists and reads zero" from "this arm was never
    /// recorded" is the entire point, so this observer tracks presence separately
    /// from value rather than folding an absent key into a default of zero.
    /// </remarks>
    private sealed class ArmObserver : IDisposable
    {
        private readonly Dictionary<string, long> _byTagValue = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();
        private readonly string _instrumentName;
        private readonly string _tagKey;

        public ArmObserver(string instrumentName, string tagKey)
        {
            _instrumentName = instrumentName;
            _tagKey = tagKey;

            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                    && instrument.Name == _instrumentName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key != _tagKey || tag.Value is not string value)
                    {
                        continue;
                    }

                    lock (_byTagValue)
                    {
                        _byTagValue[value] = _byTagValue.GetValueOrDefault(value) + measurement;
                    }
                }
            });
            _listener.Start();
        }

        /// <summary>The tag values that have been recorded at all, at any value.</summary>
        public IReadOnlyCollection<string> Observed
        {
            get
            {
                lock (_byTagValue)
                {
                    return _byTagValue.Keys.ToArray();
                }
            }
        }

        /// <summary>The summed value recorded against one tag value.</summary>
        public long Value(string tagValue)
        {
            lock (_byTagValue)
            {
                return _byTagValue.GetValueOrDefault(tagValue);
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Captures the description of named instruments as they are published.
    /// </summary>
    private sealed class DescriptionObserver : IDisposable
    {
        private readonly List<(string Instrument, string Description)> _captured = [];
        private readonly MeterListener _listener = new();

        public DescriptionObserver(params string[] instrumentNames)
        {
            var wanted = new HashSet<string>(instrumentNames, StringComparer.Ordinal);

            _listener.InstrumentPublished = (instrument, _) =>
            {
                if (instrument.Meter.Name != RepoContextUsageRecorder.MeterName
                    || !wanted.Contains(instrument.Name))
                {
                    return;
                }

                lock (_captured)
                {
                    _captured.Add((instrument.Name, instrument.Description ?? string.Empty));
                }
            };
            _listener.Start();
        }

        public IReadOnlyList<(string Instrument, string Description)> Captured
        {
            get
            {
                lock (_captured)
                {
                    return _captured.ToArray();
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
