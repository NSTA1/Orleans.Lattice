using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins what a batched embed-and-store pass REPORTS about itself, as opposed to
/// what it achieves (issue #2346).
/// <para>
/// Two defects are guarded here, and they share a cause: every count the arm used
/// to emit was a numerator.
/// </para>
/// <para>
/// <b>The denominator.</b> A failed batch named itself; a successful batch said
/// nothing at all. So "63 batch-record failures spanning 1,662 source slots" could
/// not be turned into a rate, and 63 failures in 70 attempts warrants an opposite
/// response to 63 in 70,000. Establishing the rate on the deployed container meant
/// reconstructing the attempt total from the SIDECAR EMBEDDER'S HTTP access log -
/// one POST per embedding call - an instrument that belongs to a different
/// container and vanishes when it is recycled. The pass census makes the attempt
/// total readable from this arm's own log, and it is emitted on a CLEAN pass too:
/// a line that appears only when something failed is another numerator, and
/// inferring "no failures" from the ABSENCE of a line cannot be told apart from an
/// arm that never ran, one that selected nothing, or a short log fetch.
/// </para>
/// <para>
/// <b>The stage.</b> One try block spans two distinct operations - storing the
/// vectors and recording their membership - and its single catch reported both as
/// "could not record". The only fault ever captured on the deployed container was a
/// stalled paged metadata scan inside <c>StoreAsync</c>'s retire-stale walk: a
/// READ, in the store stage, that never reached the membership write at all.
/// Reported as a record failure, it framed the defect as "the membership write
/// fails while the embed batch succeeds" and sent the investigation hunting a write
/// fault that does not exist. The distinction is load-bearing rather than cosmetic:
/// a store fault leaves neither vectors nor membership, a record fault leaves
/// vectors with no membership, and only the second is a candidate for an in-pass
/// retry.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, and the faults are injected at the grain
/// call, which is where they really occur.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorBatchCensusTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// A harness whose silo fails the first <paramref name="failFirst"/> calls to
    /// <paramref name="method"/> on <paramref name="treeId"/>.
    /// </summary>
    private static (RepoContextMcpHarnessOptions Options, LatticeTreeFaultInjector Injector) FaultingOptions(
        string treeId, string method, int failFirst)
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = treeId,
            Method = method,
            FailFirst = failFirst,
        };

        return (new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            },
        }, injector);
    }

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, ILogger<EmbeddingRepoContextVectorIngestor> logger)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            logger,
            new FakeEmbeddingProvider());

    /// <summary>
    /// Seeds symbols spanning whole embedding batches, one passage each, so batch
    /// arithmetic in the assertions is exact.
    /// </summary>
    private static async Task<List<string>> SeedSymbolsAsync(
        RepoContextMcpHarness harness, int count, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var keys = new List<string>(count);
        for (var i = 0; i < count; i++)
        {
            var fqn = $"Acme.Census.Symbol{i:D3}";
            var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Method };
            var key = RepoContextKeys.Symbol(RepoId, fqn);
            await tree.SetAsync(key, serializer.SerializeToArray(record), ct);
            keys.Add(key);
        }

        return keys;
    }

    private static IReadOnlyList<string> MessagesFrom(CapturingLoggerProvider capture)
        => capture.Entries.Select(e => e.Message).ToArray();

    private static string CensusLine(CapturingLoggerProvider capture)
        => MessagesFrom(capture).SingleOrDefault(m => m.Contains("pass census", StringComparison.Ordinal))
            ?? string.Empty;

    /// <summary>
    /// The denominator, on a pass where nothing fails. This is the arm of the guard
    /// that a failure-only log line cannot satisfy: without it, a rate has no
    /// denominator on exactly the observations that establish the baseline.
    /// </summary>
    [Test]
    public async Task A_clean_pass_reports_its_batch_attempt_total_so_a_failure_count_becomes_a_rate()
    {
        var capture = new CapturingLoggerProvider();
        var logger = new LoggerFactory(new[] { (ILoggerProvider)capture })
            .CreateLogger<EmbeddingRepoContextVectorIngestor>();

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        // Exactly three full batches, one passage per symbol.
        var keys = await SeedSymbolsAsync(
            harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, Ct);

        var embedded = await Ingestor(harness, logger)
            .IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        var census = CensusLine(capture);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(keys.Count), "arranged: a clean pass");
            Assert.That(census, Is.Not.Empty,
                "A clean pass still publishes its census. Reading 'no failures' from the ABSENCE of "
                + "a line cannot be told apart from an arm that never ran.");
            Assert.That(census, Does.Contain("3 batch(es) attempted"),
                "The attempt total is the denominator the failure counts are a rate against.");
            Assert.That(census, Does.Contain("3 succeeded"));
            Assert.That(census, Does.Contain("0 left unmarked"),
                "A measured zero, not a bare absence.");
            Assert.That(census, Does.Contain(EmbeddingRepoContextVectorIngestor.SymbolArm),
                "Attributed to the arm, so three arms sharing this body stay distinguishable.");
        });
    }

    /// <summary>
    /// A fault in the STORE stage is reported as a store fault. The metadata write
    /// is inside <c>StoreAsync</c> and cannot reach the membership write, so any
    /// line naming the record stage here is a mis-attribution.
    /// </summary>
    [Test]
    public async Task A_store_stage_fault_is_reported_as_store_and_never_as_record()
    {
        var capture = new CapturingLoggerProvider();
        var logger = new LoggerFactory(new[] { (ILoggerProvider)capture })
            .CreateLogger<EmbeddingRepoContextVectorIngestor>();

        var (options, injector) = FaultingOptions(
            RepoContextTrees.VectorMetadata, nameof(ILattice.SetAsync), failFirst: 1);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);

        var keys = await SeedSymbolsAsync(
            harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 2, Ct);

        await Ingestor(harness, logger).IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        var messages = MessagesFrom(capture);
        var storeLines = messages.Where(m => m.Contains("could not store a batch", StringComparison.Ordinal)).ToArray();
        var recordLines = messages.Where(m => m.Contains("could not record a batch", StringComparison.Ordinal)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.EqualTo(1), "arranged: exactly one metadata write was faulted");
            Assert.That(storeLines, Is.Not.Empty,
                "A fault raised inside StoreAsync names the store stage. This is the discrimination "
                + "whose absence framed a stalled metadata SCAN as a failing membership WRITE.");
            Assert.That(recordLines, Is.Empty,
                "And it is never reported as a record failure: the membership write was not reached.");
            Assert.That(CensusLine(capture), Does.Contain("1 failed to store"),
                "The census attributes the loss to the stage that actually threw.");
        });
    }

    /// <summary>
    /// The other side of the discrimination: a fault in the membership write really
    /// is a record fault, and still reports as one. Without this arm the test above
    /// would be satisfied by a build that simply renamed every failure "store".
    /// </summary>
    [Test]
    public async Task A_record_stage_fault_is_still_reported_as_record()
    {
        var capture = new CapturingLoggerProvider();
        var logger = new LoggerFactory(new[] { (ILoggerProvider)capture })
            .CreateLogger<EmbeddingRepoContextVectorIngestor>();

        var (options, injector) = FaultingOptions(
            RepoContextTrees.VectorMembership, nameof(ILattice.ApplyCrdtDeltaManyAsync), failFirst: 1);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);

        var keys = await SeedSymbolsAsync(
            harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 2, Ct);

        await Ingestor(harness, logger).IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        var messages = MessagesFrom(capture);
        var storeLines = messages.Where(m => m.Contains("could not store a batch", StringComparison.Ordinal)).ToArray();
        var recordLines = messages.Where(m => m.Contains("could not record a batch", StringComparison.Ordinal)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.EqualTo(1), "arranged: exactly one membership write was faulted");
            Assert.That(recordLines, Is.Not.Empty, "The membership write is the record stage.");
            Assert.That(storeLines, Is.Empty, "Its vectors landed, so nothing failed to store.");
            Assert.That(CensusLine(capture), Does.Contain("1 failed to record"));
            Assert.That(CensusLine(capture), Does.Contain("2 batch(es) attempted"),
                "The denominator is reported alongside the failure, which is the whole point.");
        });
    }
}
