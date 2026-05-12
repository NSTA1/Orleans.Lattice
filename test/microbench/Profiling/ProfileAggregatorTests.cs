using Orleans.Lattice.Benchmark.Microbench.Profiling;

namespace Orleans.Lattice.Benchmark.Microbench.Tests.Profiling;

/// <summary>
/// Unit tests for the pure aggregation logic in <see cref="ProfileAggregator"/>.
/// These tests do not open an EventPipe session; the
/// <c>BenchmarkProfilerSmokeTests</c> covers the live capture path.
/// </summary>
[TestFixture]
public sealed class ProfileAggregatorTests
{
    [Test]
    public void Empty_aggregator_builds_report_with_zero_totals_and_empty_top_lists()
    {
        var agg = new ProfileAggregator();
        var report = agg.Build("run-1", "deadbeef", ProfileMode.Alloc, TimeSpan.FromSeconds(2), topN: 10);

        Assert.That(report.TotalAllocationsB, Is.EqualTo(0));
        Assert.That(report.TotalCpuSamples, Is.EqualTo(0));
        Assert.That(report.TopAllocators, Is.Empty);
        Assert.That(report.TopCpu, Is.Empty);
        Assert.That(report.RunId, Is.EqualTo("run-1"));
        Assert.That(report.GitSha, Is.EqualTo("deadbeef"));
        Assert.That(report.Mode, Is.EqualTo(ProfileMode.Alloc));
        Assert.That(report.DurationMs, Is.EqualTo(2000));
    }

    [Test]
    public void RecordAllocation_accumulates_bytes_per_method_and_sorts_descending()
    {
        var agg = new ProfileAggregator();
        agg.RecordAllocation("AAA", "ModA", 100);
        agg.RecordAllocation("AAA", "ModA", 50);   // → AAA: 150
        agg.RecordAllocation("BBB", "ModA", 1000); // → BBB: 1000
        agg.RecordAllocation("CCC", "ModB", 25);   // → CCC: 25

        var report = agg.Build("r", "g", ProfileMode.Alloc, TimeSpan.FromSeconds(1), topN: 10);
        Assert.That(report.TotalAllocationsB, Is.EqualTo(1175));
        Assert.That(report.TopAllocators, Has.Count.EqualTo(3));
        Assert.That(report.TopAllocators[0].Method, Is.EqualTo("BBB"));
        Assert.That(report.TopAllocators[0].AllocB, Is.EqualTo(1000));
        Assert.That(report.TopAllocators[1].Method, Is.EqualTo("AAA"));
        Assert.That(report.TopAllocators[1].AllocB, Is.EqualTo(150));
        Assert.That(report.TopAllocators[2].Method, Is.EqualTo("CCC"));
        Assert.That(report.TopAllocators[2].AllocB, Is.EqualTo(25));
    }

    [Test]
    public void RecordAllocation_ignores_non_positive_byte_counts()
    {
        var agg = new ProfileAggregator();
        agg.RecordAllocation("Z", "M", 0);
        agg.RecordAllocation("Z", "M", -5);
        Assert.That(agg.TotalAllocBytes, Is.EqualTo(0));
        var report = agg.Build("r", "g", ProfileMode.Alloc, TimeSpan.Zero, topN: 5);
        Assert.That(report.TopAllocators, Is.Empty);
    }

    [Test]
    public void RecordAllocation_normalises_null_or_whitespace_method_to_unknown()
    {
        var agg = new ProfileAggregator();
        agg.RecordAllocation(null, null, 10);
        agg.RecordAllocation("   ", null, 5);
        var report = agg.Build("r", "g", ProfileMode.Alloc, TimeSpan.Zero, topN: 5);
        Assert.That(report.TopAllocators, Has.Count.EqualTo(1));
        Assert.That(report.TopAllocators[0].Method, Is.EqualTo("[unknown]"));
        Assert.That(report.TopAllocators[0].AllocB, Is.EqualTo(15));
    }

    [Test]
    public void RecordSample_accumulates_samples_per_method_and_sorts_descending()
    {
        var agg = new ProfileAggregator();
        for (var i = 0; i < 7; i++) agg.RecordSample("FOO", "Mod1");
        for (var i = 0; i < 3; i++) agg.RecordSample("BAR", "Mod1");
        agg.RecordSample("BAZ", "Mod2");

        var report = agg.Build("r", "g", ProfileMode.Cpu, TimeSpan.FromMilliseconds(100), topN: 10);
        Assert.That(report.TotalCpuSamples, Is.EqualTo(11));
        Assert.That(report.TopCpu, Has.Count.EqualTo(3));
        Assert.That(report.TopCpu[0].Method, Is.EqualTo("FOO"));
        Assert.That(report.TopCpu[0].Samples, Is.EqualTo(7));
        Assert.That(report.TopCpu[1].Method, Is.EqualTo("BAR"));
        Assert.That(report.TopCpu[1].Samples, Is.EqualTo(3));
        Assert.That(report.TopCpu[2].Method, Is.EqualTo("BAZ"));
        Assert.That(report.TopCpu[2].Samples, Is.EqualTo(1));
    }

    [Test]
    public void Build_caps_top_list_at_topN()
    {
        var agg = new ProfileAggregator();
        for (var i = 0; i < 20; i++) agg.RecordAllocation($"M{i:D2}", "X", i + 1);
        var report = agg.Build("r", "g", ProfileMode.Alloc, TimeSpan.Zero, topN: 5);
        Assert.That(report.TopAllocators, Has.Count.EqualTo(5));
        // Top entry must be the highest-byte method (M19 = 20 bytes).
        Assert.That(report.TopAllocators[0].Method, Is.EqualTo("M19"));
        Assert.That(report.TopAllocators[0].AllocB, Is.EqualTo(20));
    }

    [Test]
    public void Build_percentages_sum_to_at_most_100_within_rounding()
    {
        var agg = new ProfileAggregator();
        agg.RecordAllocation("A", "Mod", 60);
        agg.RecordAllocation("B", "Mod", 30);
        agg.RecordAllocation("C", "Mod", 10);
        var report = agg.Build("r", "g", ProfileMode.Alloc, TimeSpan.Zero, topN: 10);
        Assert.That(report.TopAllocators[0].AllocPct, Is.EqualTo(60.0).Within(0.1));
        Assert.That(report.TopAllocators[1].AllocPct, Is.EqualTo(30.0).Within(0.1));
        Assert.That(report.TopAllocators[2].AllocPct, Is.EqualTo(10.0).Within(0.1));
    }

    [Test]
    public void Build_throws_on_non_positive_topN()
    {
        var agg = new ProfileAggregator();
        Assert.That(() => agg.Build("r", "g", ProfileMode.Alloc, TimeSpan.Zero, topN: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That(() => agg.Build("r", "g", ProfileMode.Alloc, TimeSpan.Zero, topN: -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Build_throws_on_null_runId_or_gitSha()
    {
        var agg = new ProfileAggregator();
        Assert.That(() => agg.Build(null!, "g", ProfileMode.Alloc, TimeSpan.Zero, topN: 1),
            Throws.InstanceOf<ArgumentNullException>());
        Assert.That(() => agg.Build("r", null!, ProfileMode.Alloc, TimeSpan.Zero, topN: 1),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
