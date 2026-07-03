using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAdmissionMetrics"/>: the observable
/// per-tree admission-control gauge sink. Verifies that published samples
/// surface on the live-key and estimated-byte gauges, that the over-advisory
/// 0/1 gauge only emits for a tree that has set an advisory ceiling and
/// reflects the breach state, and that the utilisation ratio gauge emits per
/// dimension only when a ceiling (enforcing or advisory) is configured.
/// </summary>
[TestFixture]
public sealed class LatticeAdmissionMetricsTests
{
    private static AdmissionUsageSample Sample(
        string tree,
        long liveKeys = 0,
        long estimatedBytes = 0,
        long? maxKeys = null,
        long? maxBytes = null,
        long? advisoryKeys = null,
        long? advisoryBytes = null) => new()
        {
            TreeId = tree,
            LiveKeys = liveKeys,
            EstimatedBytes = estimatedBytes,
            MaxLiveKeys = maxKeys,
            MaxEstimatedBytes = maxBytes,
            AdvisoryLiveKeys = advisoryKeys,
            AdvisoryBytes = advisoryBytes,
        };

    private static long? ReadLong(string instrument, string tree, string? dimension = null)
    {
        long? found = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter) && inst.Name == instrument)
                    l.EnableMeasurementEvents(inst);
            },
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) =>
        {
            if (Matches(tags, tree, dimension)) found = value;
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return found;
    }

    private static double? ReadDouble(string instrument, string tree, string? dimension = null)
    {
        double? found = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter) && inst.Name == instrument)
                    l.EnableMeasurementEvents(inst);
            },
        };
        listener.SetMeasurementEventCallback<double>((inst, value, tags, _) =>
        {
            if (Matches(tags, tree, dimension)) found = value;
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return found;
    }

    private static bool Matches(ReadOnlySpan<KeyValuePair<string, object?>> tags, string tree, string? dimension)
    {
        var treeOk = false;
        var dimOk = dimension is null;
        foreach (var t in tags)
        {
            if (t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree) treeOk = true;
            if (dimension is not null && t.Key == LatticeMetrics.TagDimension && (string?)t.Value == dimension) dimOk = true;
        }
        return treeOk && dimOk;
    }

    [Test]
    public void Publish_surfaces_live_keys_and_estimated_bytes_gauges()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        sut.Publish(Sample(tree, liveKeys: 1234, estimatedBytes: 9999));

        Assert.Multiple(() =>
        {
            Assert.That(ReadLong(LatticeMetrics.AdmissionLiveKeysName, tree), Is.EqualTo(1234));
            Assert.That(ReadLong(LatticeMetrics.AdmissionEstimatedBytesName, tree), Is.EqualTo(9999));
        });
    }

    [Test]
    public void Publish_throws_on_null_treeId()
    {
        using var sut = new LatticeAdmissionMetrics();
        Assert.That(() => sut.Publish(Sample(null!)), Throws.ArgumentNullException);
    }

    [Test]
    public void Over_advisory_gauge_is_not_emitted_when_no_advisory_ceiling_is_set()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        sut.Publish(Sample(tree, liveKeys: 100, estimatedBytes: 100));

        Assert.That(ReadLong(LatticeMetrics.AdmissionOverAdvisoryName, tree), Is.Null,
            "a tree with no advisory ceiling has nothing to be over, so it must emit no over_advisory measurement");
    }

    [Test]
    public void Over_advisory_gauge_reports_zero_when_under_the_ceiling()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        sut.Publish(Sample(tree, liveKeys: 10, advisoryKeys: 100));

        Assert.That(ReadLong(LatticeMetrics.AdmissionOverAdvisoryName, tree), Is.EqualTo(0));
    }

    [Test]
    public void Over_advisory_gauge_reports_one_when_at_or_over_the_key_ceiling()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        sut.Publish(Sample(tree, liveKeys: 100, advisoryKeys: 100));

        Assert.That(ReadLong(LatticeMetrics.AdmissionOverAdvisoryName, tree), Is.EqualTo(1));
    }

    [Test]
    public void Over_advisory_gauge_reports_one_when_over_the_byte_ceiling_only()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        sut.Publish(Sample(tree, liveKeys: 1, estimatedBytes: 500, advisoryKeys: 100, advisoryBytes: 400));

        Assert.That(ReadLong(LatticeMetrics.AdmissionOverAdvisoryName, tree), Is.EqualTo(1),
            "exceeding either advisory dimension flags the tree as over-advisory");
    }

    [Test]
    public void Utilization_is_not_emitted_for_a_dimension_with_no_ceiling()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        sut.Publish(Sample(tree, liveKeys: 50, estimatedBytes: 50, maxKeys: 100));

        Assert.Multiple(() =>
        {
            Assert.That(ReadDouble(LatticeMetrics.AdmissionUtilizationName, tree, "keys"), Is.EqualTo(0.5),
                "keys dimension has a cap, so its utilisation is emitted");
            Assert.That(ReadDouble(LatticeMetrics.AdmissionUtilizationName, tree, "bytes"), Is.Null,
                "bytes dimension has no ceiling, so no utilisation is emitted");
        });
    }

    [Test]
    public void Utilization_prefers_the_enforcing_cap_over_the_advisory_ceiling()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        // Enforcing cap 200 preferred over advisory 100: 50 / 200 = 0.25.
        sut.Publish(Sample(tree, liveKeys: 50, maxKeys: 200, advisoryKeys: 100));

        Assert.That(ReadDouble(LatticeMetrics.AdmissionUtilizationName, tree, "keys"), Is.EqualTo(0.25));
    }

    [Test]
    public void Utilization_falls_back_to_the_advisory_ceiling_when_no_enforcing_cap()
    {
        using var sut = new LatticeAdmissionMetrics();
        var tree = $"adm-{Guid.NewGuid():N}";

        // No enforcing byte cap; advisory byte ceiling 400 is the denominator: 200 / 400 = 0.5.
        sut.Publish(Sample(tree, estimatedBytes: 200, advisoryBytes: 400));

        Assert.That(ReadDouble(LatticeMetrics.AdmissionUtilizationName, tree, "bytes"), Is.EqualTo(0.5));
    }
}
