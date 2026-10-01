using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Archive;

/// <summary>
/// Unit tests for <see cref="RepoContextMemoryRestoreReporter"/>, the instrument that
/// carries what each durable-memory restore attempt did to the memory tree.
/// <para>
/// <b>Why the pre-minting test is the important one.</b> The whole point of this
/// reporter (issue #2641) is that a tree left holding a partial import presents as a
/// normally populated store and will not announce itself. An absent
/// <c>partial</c> series and a <c>partial</c> series reading zero look identical on a
/// dashboard, but only the second is falsifiable - so "every arm is minted at zero"
/// is the property that makes a quiet dashboard a measured absence of damage rather
/// than an absent measurement.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextMemoryRestoreReporterTests
{
    /// <summary>
    /// One observed measurement: its value and the outcome tag it carried.
    /// </summary>
    private readonly record struct Measurement(long Value, string? Outcome, string? Tenant);

    /// <summary>
    /// Starts a listener over this reporter's instrument <b>before</b> the reporter is
    /// constructed, which is the only way to observe the zero-valued series its
    /// constructor mints. The filter is by meter name and instrument name because the
    /// reporter owns its <see cref="Meter"/> privately.
    /// </summary>
    private static MeterListener Listen(List<Measurement> sink)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(
                        instrument.Meter.Name,
                        RepoContextUsageRecorder.MeterName,
                        StringComparison.Ordinal)
                    && string.Equals(
                        instrument.Name,
                        RepoContextMemoryRestoreReporter.InstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? outcome = null;
            string? tenant = null;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextMemoryRestoreReporter.OutcomeTagKey)
                {
                    outcome = tag.Value as string;
                }
                else if (tag.Key == LatticeTenantLabel.TagTenant)
                {
                    tenant = tag.Value as string;
                }
            }

            sink.Add(new Measurement(value, outcome, tenant));
        });

        listener.Start();
        return listener;
    }

    private static string Tag(RepoContextMemoryRestoreOutcome outcome)
        => outcome.ToString().ToLowerInvariant();

    /// <summary>
    /// Every member of the outcome enum must reach the meter as its own zero-valued
    /// series the moment the reporter exists, so a dashboard can distinguish
    /// "no partial restore has happened" from "nothing is measuring partial restores".
    /// </summary>
    [Test]
    public void Constructing_the_reporter_mints_every_outcome_at_zero()
    {
        var observed = new List<Measurement>();
        using var listener = Listen(observed);

        using var reporter = new RepoContextMemoryRestoreReporter();

        var expected = Enum.GetValues<RepoContextMemoryRestoreOutcome>().Select(Tag).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(observed, Has.Count.EqualTo(expected.Length));
            Assert.That(observed.Select(m => m.Outcome), Is.EquivalentTo(expected));
            Assert.That(
                observed.Select(m => m.Value),
                Is.All.Zero,
                "a minted arm must carry zero, not a count");
        });
    }

    /// <summary>
    /// The minted arms must all carry the platform tenant tag. An instrument that
    /// minted untagged series would export a second, tag-less time series that no
    /// tenant-filtered panel can ever select.
    /// </summary>
    [Test]
    public void Every_minted_arm_carries_the_platform_tenant_tag()
    {
        var observed = new List<Measurement>();
        using var listener = Listen(observed);

        using var reporter = new RepoContextMemoryRestoreReporter();

        Assert.That(
            observed.Select(m => m.Tenant),
            Is.All.EqualTo(LatticeTenantLabel.Platform.Value as string));
    }

    /// <summary>
    /// Every outcome counts one on its own arm and on no other. The loop is here
    /// rather than in a <c>[Values]</c> parameter because the outcome enum is
    /// <see langword="internal"/> and cannot appear on a public test signature.
    /// </summary>
    [Test]
    public void Record_counts_one_on_the_arm_for_the_given_outcome()
    {
        foreach (var outcome in Enum.GetValues<RepoContextMemoryRestoreOutcome>())
        {
            var observed = new List<Measurement>();
            using var listener = Listen(observed);
            using var reporter = new RepoContextMemoryRestoreReporter();

            var minted = observed.Count;
            reporter.Record(outcome);

            var recorded = observed.Skip(minted).ToArray();
            Assert.Multiple(() =>
            {
                Assert.That(recorded, Has.Length.EqualTo(1), $"outcome {outcome}");
                Assert.That(recorded[0].Value, Is.EqualTo(1), $"outcome {outcome}");
                Assert.That(recorded[0].Outcome, Is.EqualTo(Tag(outcome)));
                Assert.That(
                    recorded[0].Tenant, Is.EqualTo(LatticeTenantLabel.Platform.Value as string));
            });
        }
    }

    /// <summary>
    /// Repeated attempts accumulate on their own arm and leave the others alone, so
    /// the instrument is a usable denominator rather than a latch.
    /// </summary>
    [Test]
    public void Repeated_attempts_accumulate_on_their_own_arm_only()
    {
        var observed = new List<Measurement>();
        using var listener = Listen(observed);
        using var reporter = new RepoContextMemoryRestoreReporter();

        var minted = observed.Count;
        reporter.Record(RepoContextMemoryRestoreOutcome.Partial);
        reporter.Record(RepoContextMemoryRestoreOutcome.Partial);
        reporter.Record(RepoContextMemoryRestoreOutcome.Restored);

        var recorded = observed.Skip(minted).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(
                recorded.Count(m => m.Outcome == Tag(RepoContextMemoryRestoreOutcome.Partial)),
                Is.EqualTo(2));
            Assert.That(
                recorded.Count(m => m.Outcome == Tag(RepoContextMemoryRestoreOutcome.Restored)),
                Is.EqualTo(1));
            Assert.That(recorded.Sum(m => m.Value), Is.EqualTo(3));
        });
    }

    /// <summary>
    /// The outcome tag is lower-cased so the exported label matches the values the
    /// bundled dashboards filter on. Asserting the exact strings is what stops a
    /// rename of an enum member silently retiring a panel's series.
    /// </summary>
    [Test]
    public void Outcome_tags_are_the_lower_case_member_names()
    {
        var observed = new List<Measurement>();
        using var listener = Listen(observed);

        using var reporter = new RepoContextMemoryRestoreReporter();

        Assert.That(
            observed.Select(m => m.Outcome),
            Is.EquivalentTo(new[] { "restored", "partial", "nothingtorestore", "notattempted", "failed" }));
    }

    /// <summary>
    /// The instrument is published on the package's shared meter under its documented
    /// name and unit, which is the contract the metrics documentation gate and the
    /// dashboards both resolve against.
    /// </summary>
    [Test]
    public void The_instrument_is_published_on_the_shared_meter_with_its_documented_name_and_unit()
    {
        Instrument? published = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, _) =>
            {
                if (instrument.Name == RepoContextMemoryRestoreReporter.InstrumentName)
                {
                    published = instrument;
                }
            },
        };
        listener.Start();

        using var reporter = new RepoContextMemoryRestoreReporter();

        Assert.That(published, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(published!.Meter.Name, Is.EqualTo(RepoContextUsageRecorder.MeterName));
            Assert.That(published.Unit, Is.EqualTo("{attempt}"));
            Assert.That(published.Description, Is.Not.Null.And.Not.Empty);
            Assert.That(published, Is.InstanceOf<Counter<long>>());
        });
    }

    /// <summary>
    /// Disposing the reporter disposes the meter it owns, so a reporter that has been
    /// retired stops feeding its series. Without the disposal a host that rebuilt the
    /// service would leak one live meter per rebuild, each still publishing.
    /// </summary>
    [Test]
    public void Disposing_the_reporter_stops_its_measurements_reaching_a_listener()
    {
        var observed = new List<Measurement>();
        using var listener = Listen(observed);
        var reporter = new RepoContextMemoryRestoreReporter();

        reporter.Dispose();
        var afterDispose = observed.Count;
        reporter.Record(RepoContextMemoryRestoreOutcome.Restored);

        Assert.That(
            observed.Count,
            Is.EqualTo(afterDispose),
            "a disposed reporter must not publish further measurements");
    }
}
