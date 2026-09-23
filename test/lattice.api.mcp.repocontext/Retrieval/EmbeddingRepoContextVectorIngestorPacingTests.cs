using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The embedding drain driven through the indexing pacer (issue #3447): every batch
/// is paced and its outcome fed back, a failing vector plane raises the inter-batch
/// delay, and the pacer never changes what a pass embeds.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorPacingTests
{
    private const string RepoId = "acme";

    /// <summary>Several batches' worth, so the pass paces more than once.</summary>
    private const int SymbolCount = 96;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// A real-clock pacer with a tiny delay ceiling, so a backed-off pass still runs
    /// in milliseconds, and a fixed memory load, so GC pressure on the test host can
    /// never be mistaken for congestion.
    /// </summary>
    private static RepoContextIndexingPacer Pacer() => new(
        new RepoContextIndexingOptions { PacingMaxBatchDelay = TimeSpan.FromMilliseconds(20) },
        TimeProvider.System,
        NullLogger<RepoContextIndexingPacer>.Instance,
        memoryLoad: () => 0.1);

    private static EmbeddingRepoContextVectorIngestor Ingestor(RepoContextMcpHarness harness, RepoContextIndexingPacer pacer)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            new FakeEmbeddingProvider(),
            pacer: pacer);

    private static RepoContextMcpHarnessOptions Options(LatticeTreeFaultInjector? injector) => new()
    {
        Posture = RepoContextMcpAuthPosture.Writer,
        ConfigureSilo = silo =>
        {
            if (injector is not null)
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            }
        },
    };

    private static async Task SeedSymbolsAsync(RepoContextMcpHarness harness, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        for (var i = 0; i < SymbolCount; i++)
        {
            var fqn = $"Acme.Generated.Type{i:D3}";
            var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Type };
            await tree.SetAsync(RepoContextKeys.Symbol(RepoId, fqn), serializer.SerializeToArray(record), ct);
        }
    }

    [Test]
    public async Task IngestSymbolsAsync_paced_pass_embeds_everything_and_leaves_the_pacer_active()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector: null), Ct);
        await SeedSymbolsAsync(harness, Ct);
        var pacer = Pacer();

        var embedded = await Ingestor(harness, pacer)
            .IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        var snapshot = pacer.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(SymbolCount), "Pacing only spaces batches; it never drops one.");
            Assert.That(snapshot.State, Is.Not.EqualTo(RepoIndexPaceState.Idle),
                "Every batch went through the pacer, so it has left Idle.");
            Assert.That(snapshot.ForegroundRequests, Is.Zero);
        });
    }

    [Test]
    public async Task IngestSymbolsAsync_failing_membership_writes_back_the_pacer_off()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = "ApplyCrdtDeltaManyAsync",
            FailFirst = int.MaxValue,
        };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector), Ct);
        await SeedSymbolsAsync(harness, Ct);
        var pacer = Pacer();

        Assert.That(
            async () => await Ingestor(harness, pacer)
                .IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>(),
            "Precondition: nothing landed, so the arm reports the fault.");

        var snapshot = pacer.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.State, Is.EqualTo(RepoIndexPaceState.Backoff));
            Assert.That(snapshot.BatchDelayMilliseconds, Is.EqualTo(20),
                "Repeated failures double the delay up to its ceiling.");
            Assert.That(snapshot.Reason, Is.EqualTo("an embedding batch failed"));
        });
    }
}
