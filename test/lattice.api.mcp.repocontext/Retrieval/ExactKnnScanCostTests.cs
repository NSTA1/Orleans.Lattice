using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

[TestFixture]
[NonParallelizable]
public sealed class ExactKnnScanCostTests
{
    private const string Prefix = "repocontext.retrieval.exact_scan.";
    private static readonly EmbeddingSpaceTag Space = new("cost-test", 3, VectorNormalization.UnitL2);
    private readonly Dictionary<string, double> _totals = new(StringComparer.Ordinal);
    private MeterListener _listener = null!;
    private ServiceProvider _services = null!;
    private RepoContextRetrievalGuardReporter _reporter = null!;
    private SubstitutedVectorTrees _trees = null!;

    [SetUp]
    public void SetUp()
    {
        _totals.Clear();
        _listener = new MeterListener();
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name.StartsWith(Prefix, StringComparison.Ordinal))
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
        _listener.Start();
        _reporter = new RepoContextRetrievalGuardReporter();
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _trees = new SubstitutedVectorTrees(_services.GetRequiredService<Serializer>());
    }

    [TearDown]
    public void TearDown()
    {
        _reporter.Dispose();
        _services.Dispose();
        _listener.Dispose();
    }

    [Test]
    public void Construction_primes_every_bounded_arm_without_claiming_work()
    {
        Assert.That(_totals.Keys, Is.EquivalentTo(new[]
        {
            "vectors", "pages", "duration", "gathers/completed", "gathers/faulted", "gathers/cancelled",
            "budget/unbounded", "budget/corpus_unknown", "budget/within_budget", "budget/exceeded",
        }));
        Assert.That(_totals.Values, Is.All.Zero);
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Warmed_page_and_gather_recording_allocates_no_bytes(bool listening)
    {
        _listener.Dispose();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, target) =>
        {
            if (listening && instrument.Name.StartsWith(Prefix, StringComparison.Ordinal))
                target.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
        listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
        listener.Start();
        for (var i = 0; i < 1000; i++)
        {
            _reporter.RecordExactPage(256);
            _reporter.RecordExactGather(0.1, "completed");
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 4096; i++)
        {
            _reporter.RecordExactPage(256);
            _reporter.RecordExactGather(0.1, "completed");
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.Zero, "Telemetry must not add allocations as visited vectors/pages grow.");
    }

    [Test]
    public async Task Search_counts_all_metadata_pages_but_a_cache_hit_does_not_scan()
    {
        for (var i = 0; i < 257; i++)
            _trees.Write("repo", $"v{i:D4}", $"repo/repo/file/{i}", Space, [1f, 0f, 0f]);
        _trees.Write("repo", "wrong-space", "repo/repo/file/wrong", Space with { ModelId = "other" }, [1f, 0f, 0f]);
        var index = Create();

        var first = await index.SearchAsync("repo", new float[] { 1f, 0f, 0f }, Space, 3, CancellationToken.None);
        var second = await index.SearchAsync("repo", new float[] { 1f, 0f, 0f }, Space, 3, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Has.Count.EqualTo(3));
            Assert.That(second, Is.EqualTo(first));
            Assert.That(Total("vectors"), Is.EqualTo(258), "Count visited metadata, including filtered spaces, not matches.");
            Assert.That(Total("pages"), Is.EqualTo(2), "Count returned logical pages, not vectors or storage RPCs.");
            Assert.That(Total("gathers/completed"), Is.EqualTo(1));
            Assert.That(Total("duration"), Is.GreaterThan(0));
        });
    }

    [Test]
    public void Search_payload_fault_keeps_completed_page_work_and_records_elapsed_time()
    {
        _trees.Write("repo", "v1", "repo/repo/file/a", Space, [1f, 0f, 0f]);
        _trees.PayloadTree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<Dictionary<string, byte[]>>(new TimeoutException("payload")));

        Assert.ThrowsAsync<TimeoutException>(async () =>
            await Create().SearchAsync("repo", new float[] { 1f, 0f, 0f }, Space, 1, CancellationToken.None));
        Assert.Multiple(() =>
        {
            Assert.That(Total("pages"), Is.EqualTo(1));
            Assert.That(Total("vectors"), Is.EqualTo(1));
            Assert.That(Total("gathers/faulted"), Is.EqualTo(1));
            Assert.That(Total("gathers/completed"), Is.Zero);
            Assert.That(Total("duration"), Is.GreaterThan(0));
        });
    }

    [Test]
    public void Search_caller_cancellation_records_no_returned_page()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await Create().SearchAsync("repo", new float[] { 1f, 0f, 0f }, Space, 1, cancellation.Token));
        Assert.Multiple(() =>
        {
            Assert.That(Total("gathers/cancelled"), Is.EqualTo(1));
            Assert.That(Total("gathers/faulted"), Is.Zero);
            Assert.That(Total("pages"), Is.Zero);
            Assert.That(Total("vectors"), Is.Zero);
        });
    }

    [TestCase(0, false, "corpus_unknown", 1)]
    [TestCase(1, false, "within_budget", 1)]
    [TestCase(90000, false, "exceeded", 0)]
    [TestCase(90000, true, "unbounded", 1)]
    public async Task Search_exports_the_actual_budget_decision(int corpus, bool unbounded, string outcome, int gathers)
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.SearchAsync(Arg.Any<string>(), Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RepoContextAnnSearchOutcome>(RepoContextAnnSearchOutcome.Bootstrapping));
        plane.TryGetProgress(Arg.Any<string>(), Arg.Any<EmbeddingSpaceTag>(), out Arg.Any<VectorIndexBuildProgress>())
            .Returns(call =>
            {
                call[2] = new VectorIndexBuildProgress { VectorsExpected = corpus };
                return true;
            });
        var index = new AnnRepoContextSemanticIndex(plane, Create(),
            unbounded ? RepoContextExactScanBudgets.Unbounded() : RepoContextExactScanBudgets.Default(),
            new RepoContextExactScanBreaker(), _reporter, NullLogger<AnnRepoContextSemanticIndex>.Instance);

        await index.SearchAsync("repo", new float[] { 1f, 0f, 0f }, Space, 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Total("budget/" + outcome), Is.EqualTo(1));
            Assert.That(Total("gathers/completed"), Is.EqualTo(gathers));
            Assert.That(Total("pages"), Is.EqualTo(gathers), "An empty completed gather returns one empty page.");
        });
    }

    private ExactKnnSemanticIndex Create() => new(
        _trees.GrainFactory, _services.GetRequiredService<Serializer>(),
        new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()), _reporter);

    private double Total(string name) => _totals.GetValueOrDefault(name);

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var name = instrument.Name[Prefix.Length..];
        foreach (var tag in tags)
        {
            Assert.That(tag.Key, Is.AnyOf("outcome", "tenant"));
            if (tag.Key == "outcome") name += "/" + tag.Value;
        }
        _totals[name] = _totals.GetValueOrDefault(name) + value;
    }
}
