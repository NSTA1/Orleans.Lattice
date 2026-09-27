using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing.Hygiene;
using static Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host.LockAttributionTestSupport;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextGrainStorageLockMeter"/> and the
/// <see cref="RepoContextGrainStorageConvoy"/> it reads (issue #2431).
/// </summary>
/// <remarks>
/// A lock failure is rare by design, so what these tests defend is that its absence
/// is readable: every failure arm exists at zero on the exposition before anything
/// has failed, and the convoy high-water mark survives the convoy that set it.
/// </remarks>
// NonParallelizable: the exposition test renders through a process-wide collector,
// which would also see a sibling fixture's meter instances.
[TestFixture]
[NonParallelizable]
public sealed class RepoContextGrainStorageLockMeterTests
{
    [Test]
    public void Every_failure_arm_is_on_the_exposition_at_zero_before_any_failure()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new RepoContextGrainStorageLockMeter();

        var lines = collector.Render().Split('\n');

        Assert.Multiple(() =>
        {
            foreach (var operation in new[] { "read", "write", "clear" })
            {
                foreach (var wait in new[] { RepoContextGrainStorageLockMeter.WaitExhausted, RepoContextGrainStorageLockMeter.WaitEarly })
                {
                    Assert.That(
                        lines.Any(line => line.StartsWith(RepoContextGrainStorageLockMeter.LockFailuresCounterName + "{", StringComparison.Ordinal)
                            && line.Contains($"operation=\"{operation}\"", StringComparison.Ordinal)
                            && line.Contains($"wait=\"{wait}\"", StringComparison.Ordinal)
                            && line.EndsWith(" 0", StringComparison.Ordinal)),
                        Is.True,
                        $"The {operation}/{wait} arm must be published at zero from construction. Without it, "
                        + "'no lock failure has happened' and 'this counter was never published' are the "
                        + "same reading.");
                }
            }

            Assert.That(lines.Any(line => line.StartsWith(RepoContextGrainStorageLockMeter.WritesInFlightGaugeName + " ", StringComparison.Ordinal)), Is.True);
            Assert.That(lines.Any(line => line.StartsWith(RepoContextGrainStorageLockMeter.PeakWritesInFlightGaugeName + " ", StringComparison.Ordinal)), Is.True);
        });
    }

    [Test]
    public void A_recorded_failure_counts_on_its_arm_and_records_the_convoy_width()
    {
        using var meter = new RepoContextGrainStorageLockMeter();
        using var recorder = new MeterRecorder(meter);

        meter.RecordLockFailure(RepoContextGrainStorageOperation.Clear, exhausted: true, writesInFlight: 7);

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "clear"),
                    (RepoContextGrainStorageLockMeter.WaitTag, RepoContextGrainStorageLockMeter.WaitExhausted)),
                Is.EqualTo(1d));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "write")),
                Is.Zero);
            Assert.That(recorder.Values(RepoContextGrainStorageLockMeter.LockConvoyWidthHistogramName),
                Is.EqualTo(new[] { 7d }));
        });
    }

    [Test]
    public void The_gauges_read_the_shared_convoy()
    {
        var convoy = new RepoContextGrainStorageConvoy();
        using var meter = new RepoContextGrainStorageLockMeter(convoy);
        using var recorder = new MeterRecorder(meter);

        convoy.Enter(RepoContextGrainStorageOperation.Write);
        convoy.Enter(RepoContextGrainStorageOperation.Write);
        recorder.Sample();
        var during = recorder.Last(RepoContextGrainStorageLockMeter.WritesInFlightGaugeName);
        convoy.Exit(RepoContextGrainStorageOperation.Write);
        convoy.Exit(RepoContextGrainStorageOperation.Write);
        recorder.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(meter.Convoy, Is.SameAs(convoy));
            Assert.That(during, Is.EqualTo(2d));
            Assert.That(recorder.Last(RepoContextGrainStorageLockMeter.WritesInFlightGaugeName), Is.Zero);
            Assert.That(recorder.Last(RepoContextGrainStorageLockMeter.PeakWritesInFlightGaugeName), Is.EqualTo(2d));
        });
    }

    [Test]
    public void The_operation_tag_values_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextGrainStorageLockMeter.OperationValue(RepoContextGrainStorageOperation.Read), Is.EqualTo("read"));
            Assert.That(RepoContextGrainStorageLockMeter.OperationValue(RepoContextGrainStorageOperation.Write), Is.EqualTo("write"));
            Assert.That(RepoContextGrainStorageLockMeter.OperationValue(RepoContextGrainStorageOperation.Clear), Is.EqualTo("clear"));
            Assert.Throws<ArgumentOutOfRangeException>(
                () => RepoContextGrainStorageLockMeter.OperationValue((RepoContextGrainStorageOperation)99));
        });
    }

    [Test]
    public void Writes_and_clears_share_the_convoy_and_reads_are_counted_apart()
    {
        var convoy = new RepoContextGrainStorageConvoy();

        var write = convoy.Enter(RepoContextGrainStorageOperation.Write);
        var clear = convoy.Enter(RepoContextGrainStorageOperation.Clear);
        var read = convoy.Enter(RepoContextGrainStorageOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.EqualTo(1L));
            Assert.That(clear, Is.EqualTo(2L), "A clear queues for the write lock exactly as a write does.");
            Assert.That(read, Is.EqualTo(1L), "A read returns its own count and does not widen the write convoy.");
            Assert.That(convoy.WritesInFlight, Is.EqualTo(2L));
            Assert.That(convoy.ReadsInFlight, Is.EqualTo(1L));
            Assert.That(convoy.PeakWritesInFlight, Is.EqualTo(2L));
        });

        convoy.Exit(RepoContextGrainStorageOperation.Write);
        convoy.Exit(RepoContextGrainStorageOperation.Clear);
        convoy.Exit(RepoContextGrainStorageOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(convoy.WritesInFlight, Is.Zero);
            Assert.That(convoy.ReadsInFlight, Is.Zero);
            Assert.That(convoy.PeakWritesInFlight, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task The_convoy_peak_is_exact_under_concurrent_entry()
    {
        var convoy = new RepoContextGrainStorageConvoy();
        using var start = new ManualResetEventSlim();

        var entrants = Enumerable.Range(0, 32)
            .Select(_ => Task.Run(() =>
            {
                start.Wait();
                convoy.Enter(RepoContextGrainStorageOperation.Write);
            }))
            .ToArray();
        start.Set();
        await Task.WhenAll(entrants);

        Assert.Multiple(() =>
        {
            Assert.That(convoy.WritesInFlight, Is.EqualTo(32L));
            Assert.That(convoy.PeakWritesInFlight, Is.EqualTo(32L),
                "The peak is raised by compare-and-swap, so racing entrants cannot lose the maximum.");
        });
    }

    [Test]
    public void The_host_constructs_the_meter_eagerly_rather_than_registering_a_lazy_factory()
    {
        var builder = Path.Combine(
            HygieneRepository.FindRepoRoot(), "apps", "repocontext", "Hosting", "RepoContextHostBuilder.cs");

        Assert.That(File.Exists(builder), Is.True, "The host builder was not found, so this guard inspected nothing.");

        Assert.That(
            RepoContextGarbageCollectionMeterTests.ConstructsEagerly(File.ReadAllText(builder), nameof(RepoContextGrainStorageLockMeter)),
            Is.True,
            "The failure arms are pre-minted and the gauges are observable, so none of them reach the "
            + "exposition until the meter exists. A lazily-registered meter would be created only when "
            + "the storage decorator is first resolved, leaving every series absent until then.");
    }
}
