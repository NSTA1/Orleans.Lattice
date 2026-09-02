using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the failure boundary around each embedding batch (issue #1933).
/// <para>
/// Before this, a store or membership write that timed out unwound the whole
/// vectorisation arm, so a pass discarded every batch it had not reached yet.
/// The next pass rebuilt the same queue and died in the same place, and on the
/// live deployment the symbol arm was incomplete on 11 of 11 passes - it crawled
/// forward a few hundred vectors an hour but could never finish, so the run
/// never reported completion.
/// </para>
/// <para>
/// The fault is injected at the grain call, which is where it really occurs, so
/// these pin the behaviour against the actual seam rather than a stand-in.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorBatchResilienceTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// A harness whose silo fails the first <paramref name="failFirst"/> batched
    /// membership writes to the vector membership tree, which is exactly the call
    /// that times out in production.
    /// </summary>
    private static (RepoContextMcpHarnessOptions Options, LatticeTreeFaultInjector Injector) FaultingOptions(
        int failFirst)
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
            },
        }, injector);
    }

    private static EmbeddingRepoContextVectorIngestor Ingestor(RepoContextMcpHarness harness)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            new FakeEmbeddingProvider());

    /// <summary>
    /// Seeds enough symbols to span several embedding batches, so a fault on one
    /// batch leaves later batches to prove they still run.
    /// </summary>
    private static async Task<List<string>> SeedSymbolsAsync(
        RepoContextMcpHarness harness, int count, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var keys = new List<string>(count);
        for (var i = 0; i < count; i++)
        {
            var fqn = $"Acme.Batch.Symbol{i:D3}";
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
        var embedded = 0;
        foreach (var key in keys)
        {
            if (members.Contains(VectorCodec.SourceId(key)))
            {
                embedded++;
            }
        }

        return embedded;
    }

    [Test]
    public async Task A_failed_batch_does_not_discard_the_batches_after_it()
    {
        var (options, injector) = FaultingOptions(failFirst: 1);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);

        // Three batches' worth of symbols, one passage each.
        var keys = await SeedSymbolsAsync(
            harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, Ct);

        var embedded = await Ingestor(harness)
            .IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        var recorded = await EmbeddedCountAsync(harness, keys, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.EqualTo(1), "Exactly one membership write was faulted.");
            Assert.That(embedded, Is.GreaterThan(0),
                "The arm keeps going after a batch fails instead of unwinding the whole pass.");
            Assert.That(recorded, Is.GreaterThan(0),
                "The batches after the failed one still record their membership.");
            Assert.That(recorded, Is.LessThan(keys.Count),
                "And the failed batch's sources stay unmarked, so they are retried rather than "
                + "silently counted as embedded.");
        });
    }

    [Test]
    public async Task The_sources_of_a_failed_batch_are_retried_and_settle_on_the_next_pass()
    {
        var (options, injector) = FaultingOptions(failFirst: 1);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedSymbolsAsync(
            harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 2, Ct);

        await Ingestor(harness).IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);
        var afterFirst = await EmbeddedCountAsync(harness, keys, Ct);
        Assume.That(afterFirst, Is.LessThan(keys.Count), "arranged: one batch was lost");

        // Second pass, with the fault budget already spent: the back-fill picks up
        // exactly the sources that stayed unmarked.
        await Ingestor(harness).IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        var afterSecond = await EmbeddedCountAsync(harness, keys, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.EqualTo(1));
            Assert.That(afterSecond, Is.EqualTo(keys.Count),
                "A transient fault costs one batch and one extra pass, not permanent progress. "
                + "This is the property whose absence livelocked the live deployment.");
        });
    }

    [Test]
    public async Task A_pass_where_every_batch_fails_surfaces_the_fault()
    {
        var (options, injector) = FaultingOptions(failFirst: int.MaxValue);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedSymbolsAsync(harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize, Ct);

        // Nothing landed, so the caller must still learn the arm is broken rather
        // than reading a clean zero and reporting a healthy pass.
        Assert.That(
            async () => await Ingestor(harness).IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>());

        Assert.That(injector.Failed, Is.GreaterThan(0));
    }

    [Test]
    public async Task A_failed_coverage_probe_costs_one_page_not_the_whole_arm()
    {
        // The gap sweep's coverage probe is a read against the busiest tree in the
        // plane, so it is the call that times out under load - and on the live
        // deployment it was killing the arm before a single batch ran.
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = nameof(ILattice.GetManyAsync),
            FailFirst = 1,
        };

        var options = new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            },
        };

        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);

        // Two pages' worth: the walk pages at RepoContextPortability.DefaultPageSize
        // (256), and the whole point is that losing one page leaves the other to
        // make progress. With a single page there is nothing left to salvage and
        // surfacing the fault is the correct behaviour, which the all-failing test
        // above already covers.
        var keys = await SeedSymbolsAsync(harness, RepoContextPortability.DefaultPageSize + 44, Ct);

        var embedded = await Ingestor(harness)
            .IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.EqualTo(1), "Exactly one coverage probe was faulted.");
            Assert.That(embedded, Is.GreaterThan(0),
                "The walk continues past the unprobeable page instead of unwinding the arm.");
        });

        // The skipped page is not lost - it is simply re-examined next pass.
        await Ingestor(harness).IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(await EmbeddedCountAsync(harness, keys, Ct), Is.EqualTo(keys.Count),
            "Everything settles once the transient fault clears.");
    }

    [Test]
    public async Task An_unfaulted_pass_still_embeds_everything_in_one_go()
    {
        var (options, injector) = FaultingOptions(failFirst: 0);
        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedSymbolsAsync(
            harness, EmbeddingRepoContextVectorIngestor.EmbedBatchSize + 5, Ct);

        var embedded = await Ingestor(harness)
            .IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct);

        var recorded = await EmbeddedCountAsync(harness, keys, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.Zero, "No fault was injected.");
            Assert.That(embedded, Is.EqualTo(keys.Count));
            Assert.That(recorded, Is.EqualTo(keys.Count),
                "The failure boundary costs nothing on the healthy path.");
        });
    }
}
