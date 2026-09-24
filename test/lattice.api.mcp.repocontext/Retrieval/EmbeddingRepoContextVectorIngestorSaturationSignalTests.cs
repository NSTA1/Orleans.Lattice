using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The batch loop's saturation deferral consults the platform's
/// <see cref="IWalSaturationSignal"/> instead of inferring saturation from a run of
/// <see cref="EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures"/>
/// store or record failures (issue #2683).
/// <para>
/// Before this, three consecutive failed membership writes ended the arm for the
/// pass on the belief that the vector plane was saturated - while the platform's
/// own signal measured the tree Throttled, not Saturated, and the bound fired with
/// no success in the pass to compare against. Having stopped, the arm never
/// attempted the batch that would have shown the plane admitting writes again.
/// </para>
/// <para>
/// The fault is injected at the grain call that times out in production (the
/// batched membership apply), and the signal is a substitute reporting a chosen
/// state per tree, so each test pins one verdict against the real write path.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorSaturationSignalTests
{
    private const string RepoId = "acme";

    /// <summary>Three batches past the run bound, so the tail proves whether the arm kept attempting.</summary>
    private const int Batches = EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures + 3;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static (RepoContextMcpHarnessOptions Options, LatticeTreeFaultInjector Injector) FaultingOptions(
        int failFirst, bool registerEmbeddingProvider = false)
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = nameof(ILattice.ApplyCrdtDeltaManyAsync),
            FailFirst = failFirst,
        };

        return (new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
                if (registerEmbeddingProvider)
                {
                    silo.Services.AddSingleton<IEmbeddingProvider>(new FakeEmbeddingProvider());
                }
            },
        }, injector);
    }

    private static IWalSaturationSignal Signal(string tree, WalSaturationState state)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Healthy);
        signal.GetCurrentState(tree).Returns(state);
        return signal;
    }

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, IWalSaturationSignal? saturation)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            new FakeEmbeddingProvider(),
            saturation: saturation);

    private static async Task<List<string>> SeedSymbolsAsync(
        RepoContextMcpHarness harness, int count, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var keys = new List<string>(count);
        for (var i = 0; i < count; i++)
        {
            var fqn = $"Acme.Signal.Symbol{i:D3}";
            var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Method };
            var key = RepoContextKeys.Symbol(RepoId, fqn);
            await tree.SetAsync(key, serializer.SerializeToArray(record), ct);
            keys.Add(key);
        }

        return keys;
    }

    private static async Task<int> EmbeddedCountAsync(
        RepoContextMcpHarness harness, IEnumerable<string> keys, CancellationToken ct)
    {
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, ct);
        return keys.Count(key => members.Contains(VectorCodec.SourceId(key)));
    }

    [Test]
    public async Task A_run_of_record_failures_against_a_throttled_tree_keeps_attempting_and_lands_the_rest()
    {
        // The issue's measured case: the membership tree was Throttled - appends
        // still land, just slower - and the first batches failed. The old bound
        // declared the plane saturated after three and stopped with nothing landed,
        // so the pass surfaced a fault and the three healthy batches after it were
        // never tried.
        var (options, injector) = FaultingOptions(
            failFirst: EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedSymbolsAsync(harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * Batches, Ct);
        var signal = Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled);

        var embedded = await Ingestor(harness, signal)
            .IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        var expected = EmbeddingRepoContextVectorIngestor.EmbedBatchSize
            * (Batches - EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                injector.Failed,
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures),
                "arranged: the first batches failed to record");
            Assert.That(
                injector.Matched,
                Is.GreaterThan(injector.Failed),
                "the arm attempted batches past the run bound because the signal reported Throttled, not Saturated");
            Assert.That(embedded, Is.EqualTo(expected), "every batch after the run landed");
            Assert.That(await EmbeddedCountAsync(harness, keys, Ct), Is.EqualTo(expected));
        });
        signal.Received().GetCurrentState(RepoContextTrees.VectorMembership);
    }

    [Test]
    public async Task A_run_of_record_failures_against_a_healthy_plane_attempts_every_batch()
    {
        // No tree reports Saturated, so nothing licenses deferral: every batch is
        // attempted and each one re-tests the plane. Nothing landed, so the fault
        // still surfaces rather than reading as a clean pass.
        var (options, injector) = FaultingOptions(failFirst: int.MaxValue);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedSymbolsAsync(harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * Batches, Ct);
        var signal = Signal(RepoContextTrees.VectorMembership, WalSaturationState.Healthy);

        Assert.That(
            async () => await Ingestor(harness, signal).IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>());

        Assert.That(injector.Failed, Is.EqualTo(Batches), "every batch was attempted");
    }

    [Test]
    public async Task A_run_of_record_failures_against_a_saturated_tree_defers_the_rest_of_the_pass()
    {
        // The case the bound was for, now confirmed by the platform rather than
        // guessed: the membership tree reports Saturated, so the arm stops adding
        // load at the run bound and leaves every deferred source unmarked.
        var (options, injector) = FaultingOptions(failFirst: int.MaxValue);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedSymbolsAsync(harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * Batches, Ct);
        var signal = Signal(RepoContextTrees.VectorMembership, WalSaturationState.Saturated);

        Assert.That(
            async () => await Ingestor(harness, signal).IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>());

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                injector.Failed,
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures),
                "the arm deferred at the run bound because the signal confirmed saturation");
            Assert.That(await EmbeddedCountAsync(harness, keys, Ct), Is.Zero);
        });
    }

    [Test]
    public async Task A_saturated_tree_outside_the_vector_plane_does_not_defer_the_pass()
    {
        // Only the trees the store and record stages write can justify deferring
        // them; a saturated symbol tree says nothing about the membership write.
        var (options, _) = FaultingOptions(
            failFirst: EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedSymbolsAsync(harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * Batches, Ct);
        var signal = Signal(RepoContextTrees.Symbol, WalSaturationState.Saturated);

        var embedded = await Ingestor(harness, signal)
            .IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        Assert.That(
            embedded,
            Is.EqualTo(EmbeddingRepoContextVectorIngestor.EmbedBatchSize
                * (Batches - EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures)));
    }

    [Test]
    public async Task The_registered_ingestor_consults_the_silos_saturation_signal()
    {
        // The production wiring: the container-built ingestor must receive the
        // silo's signal. Without it the ingestor falls back to inferring saturation
        // from the run and stops at the bound, which the harness's healthy plane
        // would expose as a surfaced fault with nothing landed.
        var (options, injector) = FaultingOptions(
            failFirst: EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures,
            registerEmbeddingProvider: true);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        Assume.That(
            harness.Services.GetService<IWalSaturationSignal>(),
            Is.Not.Null,
            "arranged: the silo registers a WAL saturation signal");
        var keys = await SeedSymbolsAsync(harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * Batches, Ct);

        var ingestor = harness.Services.GetRequiredService<IRepoContextVectorIngestor>();
        var embedded = await ingestor.IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                injector.Failed,
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures));
            Assert.That(
                embedded,
                Is.EqualTo(EmbeddingRepoContextVectorIngestor.EmbedBatchSize
                    * (Batches - EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures)),
                "the registered ingestor kept attempting past the run bound on a plane its signal reports unsaturated");
        });
    }
}
