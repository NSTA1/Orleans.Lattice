using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

[TestFixture]
[NonParallelizable]
public sealed class RepoContextAnnLifecycleObservabilityTests
{
    [Test]
    public async Task Gauges_follow_banked_and_restored_progress_and_retire_with_registry()
    {
        var readings = new List<(string Name, long Value, Dictionary<string, object?> Tags)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                    && instrument.Name is "repocontext.ann.vectors" or "repocontext.ann.partitions")
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            readings.Add((instrument.Name, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value))));
        listener.Start();
        using var rig = new AnnPlaneFixture();
        listener.RecordObservableInstruments();
        Assert.That(readings, Is.Empty, "A registry with no handles must not invent a plane.");
        rig.SeedRing(64);
        var ct = TestContext.CurrentContext.CancellationToken;
        await rig.Registry.BuildStepAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, ct);
        AssertGauges();
        await rig.Registry.BuildStepAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, ct);
        AssertGauges();
        await rig.BuildAsync(ct);
        AssertGauges();
        rig.Restart();
        await rig.Registry.BuildStepAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, ct);
        AssertGauges();
        rig.Dispose();
        readings.Clear();
        listener.RecordObservableInstruments();
        Assert.That(readings, Is.Empty, "Disposed registries must not leave phantom planes.");

        void AssertGauges()
        {
            readings.Clear();
            listener.RecordObservableInstruments();
            Assert.That(rig.Registry.TryGetProgress(
                AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var progress), Is.True);
            Assert.That(readings, Has.Count.EqualTo(3));
            Assert.Multiple(() =>
            {
                Assert.That(readings.Single(r => r.Name == "repocontext.ann.vectors"
                    && Equals(r.Tags["count"], "held")).Value, Is.EqualTo(progress.VectorsIndexed));
                Assert.That(readings.Single(r => r.Name == "repocontext.ann.vectors"
                    && Equals(r.Tags["count"], "expected")).Value, Is.EqualTo(progress.VectorsExpected));
                Assert.That(readings.Single(r => r.Name == "repocontext.ann.partitions").Value,
                    Is.EqualTo(progress.PartitionsTotal));
                Assert.That(readings.All(r => Equals(r.Tags["repository"], AnnPlaneFixture.RepoId)), Is.True);
                Assert.That(readings.All(r => Equals(r.Tags["space"],
                    RepoContextAnnBuildSliceReporter.DescribeSpace(AnnPlaneFixture.Space))), Is.True);
                Assert.That(readings.All(r => Equals(r.Tags["tenant"], "_platform_")), Is.True);
            });
        }
    }

    [TestCase(VectorIndexLoadDiscardReason.None)]
    [TestCase(VectorIndexLoadDiscardReason.UnloadableRecord)]
    [TestCase(VectorIndexLoadDiscardReason.CountMismatch)]
    [TestCase(VectorIndexLoadDiscardReason.EmbeddingSpaceChange)]
    public async Task Durable_load_exposes_the_verified_discard_reason(VectorIndexLoadDiscardReason expected)
    {
        using var rig = new AnnPlaneFixture();
        rig.SeedRing(24);
        var ct = TestContext.CurrentContext.CancellationToken;
        await rig.BuildAsync(ct);
        rig.Registry.Dispose();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space);
        var key = VectorIndexStorageKeys.Manifest(prefix);
        Assert.That(VectorIndexManifest.TryReadRecord(await rig.Store.ReadAsync(key, ct), out var manifest), Is.True);
        byte[] bytes;
        switch (expected)
        {
            case VectorIndexLoadDiscardReason.CountMismatch:
                bytes = (manifest with
                {
                    IndexedCount = manifest.IndexedCount + 1,
                    Header = manifest.Header with { Count = manifest.Header.Count + 1 },
                }).ToRecord();
                break;
            case VectorIndexLoadDiscardReason.EmbeddingSpaceChange:
                bytes = (manifest with
                {
                    Header = manifest.Header with { Dimensions = manifest.Header.Dimensions + 1 },
                }).ToRecord();
                break;
            case VectorIndexLoadDiscardReason.UnloadableRecord:
                bytes = [1, 2, 3];
                break;
            default:
                bytes = manifest.ToRecord();
                break;
        }
        await rig.Store.WriteAsync([new(key, bytes)], ct);

        var loaded = await DurableVectorIndex.OpenAsync(rig.Store, rig.Source,
            rig.Options.ToDurableOptions(AnnPlaneFixture.Space, prefix), cancellationToken: ct);

        Assert.That(loaded.LoadDiscardReason, Is.EqualTo(expected));
        Assert.That(loaded.Count, Is.EqualTo(expected == VectorIndexLoadDiscardReason.None ? 24 : 0));
    }

    [TestCase("other")]
    [TestCase("timeout")]
    [TestCase("admission_refused")]
    public void Unsuccessful_load_emits_a_bounded_reason(string reason)
    {
        using var rig = new AnnPlaneFixture();
        var store = Substitute.For<IVectorIndexStore>();
        store.ReadAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<byte[]?>(_ => throw (reason switch
            {
                "timeout" => new TimeoutException("not a metric label"),
                "admission_refused" => new LatticeSaturatedException("not a metric label", "test-tree"),
                _ => new IOException("not a metric label"),
            }));
        // An empty completed key walk reaches the manifest read that faults.
        store.ScanAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(rig.Store.ScanAsync("empty"));
        using var reporter = new RepoContextAnnIndexLoadReporter();
        using var handle = new RepoContextAnnIndexHandle(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space,
            rig.Source, store, rig.Options, "index/", NullLogger.Instance, load: reporter);
        var events = new List<Dictionary<string, object?>>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Name == RepoContextAnnIndexLoadReporter.LoadInstrumentName)
                    meterListener.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value > 0) events.Add(tags.ToArray().ToDictionary(t => t.Key, t => t.Value));
        });
        listener.Start();

        if (reason == "admission_refused")
            Assert.DoesNotThrowAsync(() => handle.AdvanceAsync(TestContext.CurrentContext.CancellationToken));
        else
            Assert.That(async () => await handle.AdvanceAsync(TestContext.CurrentContext.CancellationToken),
                Throws.TypeOf(reason == "timeout" ? typeof(TimeoutException) : typeof(IOException)));

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0]["outcome"], Is.EqualTo(reason == "admission_refused" ? "refused" : "faulted"));
        Assert.That(events[0]["reason"], Is.EqualTo(reason));
    }

    [Test]
    public async Task Corrupt_manifest_records_discard_reason_instead_of_a_fresh_load()
    {
        using var rig = new AnnPlaneFixture();
        rig.SeedRing(24);
        var ct = TestContext.CurrentContext.CancellationToken;
        await rig.BuildAsync(ct);
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space);
        await rig.Store.WriteAsync(
            [new(VectorIndexStorageKeys.Manifest(prefix), new byte[] { 1, 2, 3 })], ct);
        rig.Registry.Dispose();
        var logger = Substitute.For<ILogger<RepoContextAnnIndexRegistry>>();
        using var registry = new RepoContextAnnIndexRegistry(rig.Factory, rig.Options, logger);
        var events = new List<Dictionary<string, object?>>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Name == RepoContextAnnIndexLoadReporter.LoadInstrumentName)
                    meterListener.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value > 0)
                events.Add(tags.ToArray().ToDictionary(t => t.Key, t => t.Value));
        });
        listener.Start();

        await registry.BuildStepAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, ct);
        await registry.BuildStepAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, ct);

        Assert.That(events, Has.Count.EqualTo(1));
        var warnings = logger.ReceivedCalls().Where(call =>
            call.GetMethodInfo().Name == nameof(ILogger.Log)
            && Equals(call.GetArguments()[0], LogLevel.Warning)).ToArray();
        Assert.That(warnings, Has.Length.EqualTo(1));
        Assert.That(warnings[0].GetArguments()[2]!.ToString(), Does.Contain("unloadable_record"));
        Assert.Multiple(() =>
        {
            Assert.That(events[0]["outcome"], Is.EqualTo("discarded"));
            Assert.That(events[0].GetValueOrDefault("reason"), Is.EqualTo("unloadable_record"));
        });
    }
}
