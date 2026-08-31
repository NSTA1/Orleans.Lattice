using System.Diagnostics.Metrics;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// Covers <see cref="GrainIndexMetrics"/>: the naming contract every dashboard
/// and subscriber depends on, the tag caching that keeps recording
/// allocation-free, and the fact that each recording helper actually publishes
/// on the shared core meter.
/// </summary>
/// <remarks>
/// The instruments are process-wide, so the fixture is not parallelizable: two
/// fixtures recording at once would see each other's measurements.
/// </remarks>
[TestFixture]
[NonParallelizable]
public sealed class GrainIndexMetricsTests
{
    private const string Index = "metrics-tests";

    [Test]
    public void Every_instrument_is_published_on_the_shared_core_meter()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexMetrics.MeterName, Is.EqualTo(LatticeMetrics.MeterName));
            Assert.That(GrainIndexMetrics.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.GrainsEnrolled.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.Entries.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.WriteFailures.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.ProjectionDuration.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.BackfillProcessed.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.BackfillTotal.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.BackfillPercentComplete.Meter, Is.SameAs(LatticeMetrics.Meter));
            Assert.That(GrainIndexMetrics.BackfillState.Meter, Is.SameAs(LatticeMetrics.Meter));
        });
    }

    [Test]
    public void Every_instrument_carries_its_published_name_and_unit()
    {
        Assert.Multiple(() =>
        {
            AssertInstrument(GrainIndexMetrics.GrainsEnrolled, GrainIndexMetrics.GrainsEnrolledName, "{grain}");
            AssertInstrument(GrainIndexMetrics.Entries, GrainIndexMetrics.EntriesName, "{entry}");
            AssertInstrument(GrainIndexMetrics.WriteFailures, GrainIndexMetrics.WriteFailuresName, "{failure}");
            AssertInstrument(GrainIndexMetrics.ProjectionDuration, GrainIndexMetrics.ProjectionDurationName, "ms");
            AssertInstrument(GrainIndexMetrics.BackfillProcessed, GrainIndexMetrics.BackfillProcessedName, "{grain}");
            AssertInstrument(GrainIndexMetrics.BackfillTotal, GrainIndexMetrics.BackfillTotalName, "{grain}");
            AssertInstrument(
                GrainIndexMetrics.BackfillPercentComplete,
                GrainIndexMetrics.BackfillPercentCompleteName,
                "%");
            AssertInstrument(GrainIndexMetrics.BackfillState, GrainIndexMetrics.BackfillStateName, "{state}");
        });
    }

    [Test]
    public void Every_instrument_name_sits_under_the_grain_index_prefix()
    {
        string[] names =
        [
            GrainIndexMetrics.GrainsEnrolledName,
            GrainIndexMetrics.EntriesName,
            GrainIndexMetrics.WriteFailuresName,
            GrainIndexMetrics.ProjectionDurationName,
            GrainIndexMetrics.BackfillProcessedName,
            GrainIndexMetrics.BackfillTotalName,
            GrainIndexMetrics.BackfillPercentCompleteName,
            GrainIndexMetrics.BackfillStateName,
        ];

        Assert.Multiple(() =>
        {
            Assert.That(names, Is.Unique);
            Assert.That(names, Is.All.StartsWith("orleans.lattice.grainindex."));
        });
    }

    [Test]
    public void The_tag_keys_and_path_values_are_the_documented_ones()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexMetrics.TagIndex, Is.EqualTo("index"));
            Assert.That(GrainIndexMetrics.TagPath, Is.EqualTo("path"));
            Assert.That(GrainIndexMetrics.PathActivation, Is.EqualTo("activation"));
            Assert.That(GrainIndexMetrics.PathBackfill, Is.EqualTo("backfill"));
            Assert.That(GrainIndexMetrics.PathOutbox, Is.EqualTo("outbox"));
        });
    }

    [Test]
    public void Index_tag_rejects_a_null_index_name() =>
        Assert.That(
            () => GrainIndexMetrics.IndexTag(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Index_tag_names_the_index()
    {
        var tag = GrainIndexMetrics.IndexTag(Index);

        Assert.Multiple(() =>
        {
            Assert.That(tag.Key, Is.EqualTo(GrainIndexMetrics.TagIndex));
            Assert.That(tag.Value, Is.EqualTo(Index));
        });
    }

    [Test]
    public void Index_tag_interns_one_tag_value_per_index_so_recording_allocates_none()
    {
        var first = GrainIndexMetrics.IndexTag("interned-index");
        var second = GrainIndexMetrics.IndexTag("interned-index");

        // Reference equality on the boxed value is the point: a fresh tag per
        // call would mean an allocation on every measurement.
        Assert.That(first.Value, Is.SameAs(second.Value));
    }

    [Test]
    public void Index_tag_gives_different_indexes_different_tags()
    {
        var left = GrainIndexMetrics.IndexTag("left-index");
        var right = GrainIndexMetrics.IndexTag("right-index");

        Assert.That(left.Value, Is.Not.EqualTo(right.Value));
    }

    [Test]
    public void The_prebuilt_path_tags_carry_the_path_key_and_their_own_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexMetrics.ActivationPathTag.Key, Is.EqualTo(GrainIndexMetrics.TagPath));
            Assert.That(GrainIndexMetrics.ActivationPathTag.Value, Is.EqualTo(GrainIndexMetrics.PathActivation));
            Assert.That(GrainIndexMetrics.BackfillPathTag.Value, Is.EqualTo(GrainIndexMetrics.PathBackfill));
            Assert.That(GrainIndexMetrics.OutboxPathTag.Value, Is.EqualTo(GrainIndexMetrics.PathOutbox));
        });
    }

    [Test]
    public void Recording_enrolled_grains_publishes_the_count_tagged_by_index_and_path()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordGrainsEnrolled(
            GrainIndexMetrics.IndexTag(Index),
            GrainIndexMetrics.BackfillPathTag,
            3);

        var recorded = recorder.For(GrainIndexMetrics.GrainsEnrolledName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].Value, Is.EqualTo(3d));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, Index), Is.True);
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagPath, GrainIndexMetrics.PathBackfill), Is.True);
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Recording_a_non_positive_enrolled_count_publishes_nothing(int count)
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordGrainsEnrolled(
            GrainIndexMetrics.IndexTag(Index),
            GrainIndexMetrics.ActivationPathTag,
            count);

        Assert.That(recorder.For(GrainIndexMetrics.GrainsEnrolledName), Is.Empty);
    }

    [TestCase(4)]
    [TestCase(-4)]
    public void Recording_an_entry_delta_publishes_it_tagged_by_index(int delta)
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordEntryDelta(GrainIndexMetrics.IndexTag(Index), delta);

        var recorded = recorder.For(GrainIndexMetrics.EntriesName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].Value, Is.EqualTo((double)delta));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, Index), Is.True);
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagPath, null), Is.False);
        });
    }

    [Test]
    public void Recording_a_zero_entry_delta_publishes_nothing()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordEntryDelta(GrainIndexMetrics.IndexTag(Index), 0);

        Assert.That(recorder.For(GrainIndexMetrics.EntriesName), Is.Empty);
    }

    [Test]
    public void Recording_write_failures_publishes_the_count_tagged_by_index_and_path()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordWriteFailures(
            GrainIndexMetrics.IndexTag(Index),
            GrainIndexMetrics.OutboxPathTag,
            2);

        var recorded = recorder.For(GrainIndexMetrics.WriteFailuresName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].Value, Is.EqualTo(2d));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagPath, GrainIndexMetrics.PathOutbox), Is.True);
        });
    }

    [Test]
    public void Recording_no_write_failures_publishes_nothing()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordWriteFailures(
            GrainIndexMetrics.IndexTag(Index),
            GrainIndexMetrics.ActivationPathTag,
            0);

        Assert.That(recorder.For(GrainIndexMetrics.WriteFailuresName), Is.Empty);
    }

    [Test]
    public void Recording_a_projection_publishes_a_non_negative_duration_tagged_by_index()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordProjectionDuration(
            GrainIndexMetrics.IndexTag(Index),
            System.Diagnostics.Stopwatch.GetTimestamp());

        var recorded = recorder.For(GrainIndexMetrics.ProjectionDurationName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));

            // Elapsed time is not asserted against a wall-clock bound; only that
            // a duration was published and that it is not negative.
            Assert.That(recorded[0].Value, Is.GreaterThanOrEqualTo(0d));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, Index), Is.True);
        });
    }

    [Test]
    public void Recording_enrolled_grains_carries_the_platform_tenant_dimension()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordGrainsEnrolled(
            GrainIndexMetrics.IndexTag(Index),
            GrainIndexMetrics.ActivationPathTag,
            1);

        var recorded = recorder.For(GrainIndexMetrics.GrainsEnrolledName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            // The index tag is still present, so the tenant dimension is additive.
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, Index), Is.True);
            Assert.That(
                recorded[0].HasTag(LatticeTenantLabel.TagTenant, LatticeTenantLabel.PlatformTenant),
                Is.True);
        });
    }

    [Test]
    public void Recording_an_entry_delta_carries_the_platform_tenant_dimension()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordEntryDelta(GrainIndexMetrics.IndexTag(Index), 1);

        var recorded = recorder.For(GrainIndexMetrics.EntriesName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, Index), Is.True);
            Assert.That(
                recorded[0].HasTag(LatticeTenantLabel.TagTenant, LatticeTenantLabel.PlatformTenant),
                Is.True);
        });
    }

    [Test]
    public void Recording_write_failures_carries_the_platform_tenant_dimension()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordWriteFailures(
            GrainIndexMetrics.IndexTag(Index),
            GrainIndexMetrics.OutboxPathTag,
            1);

        var recorded = recorder.For(GrainIndexMetrics.WriteFailuresName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, Index), Is.True);
            Assert.That(
                recorded[0].HasTag(LatticeTenantLabel.TagTenant, LatticeTenantLabel.PlatformTenant),
                Is.True);
        });
    }

    [Test]
    public void Recording_a_projection_carries_the_platform_tenant_dimension()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexMetrics.RecordProjectionDuration(
            GrainIndexMetrics.IndexTag(Index),
            System.Diagnostics.Stopwatch.GetTimestamp());

        var recorded = recorder.For(GrainIndexMetrics.ProjectionDurationName);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(1));
            Assert.That(recorded[0].HasTag(GrainIndexMetrics.TagIndex, Index), Is.True);
            Assert.That(
                recorded[0].HasTag(LatticeTenantLabel.TagTenant, LatticeTenantLabel.PlatformTenant),
                Is.True);
        });
    }

    private static void AssertInstrument(Instrument instrument, string name, string unit)
    {
        Assert.That(instrument.Name, Is.EqualTo(name));
        Assert.That(instrument.Unit, Is.EqualTo(unit));
        Assert.That(instrument.Description, Is.Not.Null.And.Not.Empty);
    }
}
