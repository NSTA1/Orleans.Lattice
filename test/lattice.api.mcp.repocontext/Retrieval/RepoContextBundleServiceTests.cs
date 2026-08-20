using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextBundleService"/>, the read-only orchestration
/// behind <c>repocontext_context</c>. They drive a real service over substituted
/// Lattice trees in degraded keyword mode (no embedder is bound), so the whole
/// search -> resolve -> render -> pack pipeline runs without a cluster. The
/// load-bearing invariants are proven with the real exact-BPE counter: the hard
/// ceiling is never exceeded end to end, a non-fitting bundle fails closed with a
/// guaranteed-to-fit retry budget, an empty search yields an empty bundle, the
/// wire-supplied budget is clamped, and each detail level reports the concrete level
/// it ran with.
/// </summary>
[TestFixture]
public sealed partial class RepoContextBundleServiceTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly IRepoContextTokenCounter Counter =
        new TiktokenRepoContextTokenCounter(new RepoContextIndexingOptions());

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    // The body text the content projection stores after Create's normalisation, which
    // is what the slices detail level packs - used to compute exact expected costs.
    private static string StoredBody(string path, string body)
        => RepoContextValues.ReadString(ContentRecord.Create(RepoId, path, body, Clock(1)).Text)!;

    [Test]
    public async Task Build_bundles_matching_source_in_keyword_mode_under_the_budget()
    {
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget lattice bundle";
        var service = BuildService((path, body, 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        var expected = StoredBody(path, body);
        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("keyword"),
                "With no embedder bound the search degrades to keyword ranking and still bundles.");
            Assert.That(result.Detail, Is.EqualTo("slices"));
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            var entry = result.Entries[0];
            Assert.That(entry.Path, Is.EqualTo(path));
            Assert.That(entry.Content, Is.EqualTo(expected));
            Assert.That(entry.TokenCount, Is.EqualTo(Counter.CountTokens(expected)));
            Assert.That(result.TotalTokens, Is.EqualTo(entry.TokenCount));
            Assert.That(result.TotalTokens, Is.LessThanOrEqualTo(result.BudgetTokens),
                "The hard ceiling must never be exceeded.");
            Assert.That(result.Truncated, Is.False);
            Assert.That(result.RetryBudgetTokens, Is.Null);
        });
    }

    [Test]
    public async Task Build_never_exceeds_the_budget_across_many_files()
    {
        var service = BuildService(
            ("src/A.cs", "class A { } // widget one", 10L),
            ("src/B.cs", "class B { } // widget two three four five six", 20L),
            ("src/C.cs", "class C { } // widget " + string.Join(' ', Enumerable.Repeat("token", 80)), 300L));

        // A ceiling that admits some but not all of the three files.
        const int ceiling = 40;
        var result = await service.BuildAsync(
            RepoId, "widget", 10, ceiling, RepoContextContextDetail.Slices, CancellationToken.None);

        var exactSum = result.Entries.Sum(e => Counter.CountTokens(e.Content));
        Assert.Multiple(() =>
        {
            Assert.That(result.TotalTokens, Is.LessThanOrEqualTo(ceiling),
                "The exact BPE total must never exceed the clamped ceiling.");
            Assert.That(result.TotalTokens, Is.EqualTo(exactSum),
                "The reported total must equal the exact BPE sum of the packed content.");
            foreach (var entry in result.Entries)
            {
                Assert.That(entry.TokenCount, Is.EqualTo(Counter.CountTokens(entry.Content)),
                    $"Entry '{entry.Path}' must carry its own exact BPE count.");
            }
        });
    }

    [Test]
    public async Task Build_on_an_empty_search_returns_an_empty_bundle_with_no_retry_budget()
    {
        var service = BuildService();

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Auto, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("empty"));
            Assert.That(result.Entries, Is.Empty);
            Assert.That(result.TotalTokens, Is.Zero);
            Assert.That(result.Truncated, Is.False);
            Assert.That(result.RetryBudgetTokens, Is.Null,
                "A null retry budget alongside an empty bundle means no larger budget would help.");
            Assert.That(result.Detail, Is.EqualTo("paths"),
                "An auto request with nothing to pack reports the floor level, never 'auto'.");
        });
    }

    [Test]
    public async Task Build_fails_closed_and_reports_a_guaranteed_retry_budget()
    {
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget lattice bundle";
        var service = BuildService((path, body, 4096L));

        // A ceiling of 1 cannot admit the multi-token body.
        var failed = await service.BuildAsync(
            RepoId, "widget", 10, 1, RepoContextContextDetail.Slices, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(failed.Entries, Is.Empty, "Nothing fits a one-token ceiling.");
            Assert.That(failed.TotalTokens, Is.Zero);
            Assert.That(failed.RetryBudgetTokens, Is.Not.Null);
            Assert.That(failed.RetryBudgetTokens, Is.GreaterThan(1));
        });

        // Retrying with the reported budget must admit the candidate.
        var retried = await service.BuildAsync(
            RepoId, "widget", 10, failed.RetryBudgetTokens!.Value, RepoContextContextDetail.Slices, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(retried.Entries, Is.Not.Empty,
                "The retry budget the service reports must fit at least one entry.");
            Assert.That(retried.TotalTokens, Is.LessThanOrEqualTo(failed.RetryBudgetTokens!.Value),
                "The retried bundle still respects its (retry) ceiling exactly.");
        });
    }

    [TestCase(0, 8192)]
    [TestCase(-5, 8192)]
    [TestCase(5_000_000, 200_000)]
    [TestCase(50, 50)]
    public async Task Build_clamps_the_wire_supplied_budget(int requested, int expected)
    {
        var service = BuildService();

        var result = await service.BuildAsync(
            RepoId, "widget", 10, requested, RepoContextContextDetail.Auto, CancellationToken.None);

        Assert.That(result.BudgetTokens, Is.EqualTo(expected),
            "The budget is clamped defensively so a wire caller cannot drive unbounded work.");
    }

    [Test]
    public async Task Build_clamps_top_above_the_maximum()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, "class Widget { } // widget", 12L));

        // A wildly out-of-range top must not throw and must still bundle the match.
        var result = await service.BuildAsync(
            RepoId, "widget", 10_000, 10_000, RepoContextContextDetail.Paths, CancellationToken.None);

        Assert.That(result.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Build_paths_detail_packs_the_path_and_carries_the_stored_full_read_count()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, "class Widget { } // widget", 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Paths, CancellationToken.None);

        var entry = result.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(result.Detail, Is.EqualTo("paths"));
            Assert.That(entry.Content, Is.EqualTo(path));
            Assert.That(entry.TokenCount, Is.EqualTo(Counter.CountTokens(path)));
            Assert.That(entry.FullReadTokenCount, Is.EqualTo(4096),
                "Paths detail reports the whole-file read cost from the stored FileNode.TokenCount.");
        });
    }

    [Test]
    public async Task Build_outline_detail_renders_the_symbol_skeleton()
    {
        const string path = "src/Widget.cs";
        const string signature = "void Assemble()";
        var service = BuildServiceWithSymbol(path, "class Widget { } // widget", "Acme.Widget.Assemble", signature, storedTokenCount: 100L);

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline, CancellationToken.None);

        var entry = result.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(result.Detail, Is.EqualTo("outline"));
            Assert.That(entry.Content, Is.EqualTo(signature),
                "Outline detail reuses the graph outline projection's symbol signature.");
            Assert.That(entry.FullReadTokenCount, Is.EqualTo(100),
                "Outline detail carries the outline projection's whole-file read cost.");
        });
    }

    [Test]
    public async Task Build_auto_reports_the_concrete_level_it_ran_with()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, "class Widget { } // widget body text", 30L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Auto, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Detail, Is.EqualTo("slices"),
                "Auto packs the richest level that fits and reports it, never 'auto'.");
            Assert.That(result.Entries, Is.Not.Empty);
        });
    }

    [Test]
    public void Build_null_arguments_throw()
    {
        var service = BuildService();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await service.BuildAsync(
                    null!, "t", 10, 100, RepoContextContextDetail.Auto, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await service.BuildAsync(
                    RepoId, null!, 10, 100, RepoContextContextDetail.Auto, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    private static RepoContextBundleService BuildService(params (string Path, string Body, long TokenCount)[] files)
        => BuildService(NoOpUsageRecorder.Instance, files);

    private static RepoContextBundleService BuildService(
        IRepoContextUsageRecorder recorder, params (string Path, string Body, long TokenCount)[] files)
    {
        var structuralEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var contentEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var file in files)
        {
            var node = new FileNode
            {
                RepoId = RepoId,
                Path = file.Path,
                TokenCount = RepoContextValues.Lww(file.TokenCount, Clock(1)),
            };
            structuralEntries[RepoContextKeys.File(RepoId, file.Path)] = Serializer.SerializeToArray(node);
            contentEntries[RepoContextKeys.Content(RepoId, file.Path)] =
                Serializer.SerializeToArray(ContentRecord.Create(RepoId, file.Path, file.Body, Clock(1)));
        }

        return Assemble(
            Tree(structuralEntries),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            Tree(contentEntries),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            recorder);
    }

    private static RepoContextBundleService BuildServiceWithSymbol(
        string path, string body, string fullyQualifiedName, string signature, long storedTokenCount)
    {
        var node = new FileNode
        {
            RepoId = RepoId,
            Path = path,
            TokenCount = RepoContextValues.Lww(storedTokenCount, Clock(1)),
            DeclaredSymbols = RepoContextValues.Lww(DeclaredSymbolNames.Encode([fullyQualifiedName]), Clock(1)),
        };
        var structuralEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.File(RepoId, path)] = Serializer.SerializeToArray(node),
        };
        var contentEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.Content(RepoId, path)] =
                Serializer.SerializeToArray(ContentRecord.Create(RepoId, path, body, Clock(1))),
        };
        var symbolRecord = new SymbolRecord
        {
            RepoId = RepoId,
            FullyQualifiedName = fullyQualifiedName,
            Signature = RepoContextValues.Lww(signature, Clock(1)),
        };
        var symbolEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.Symbol(RepoId, fullyQualifiedName)] = Serializer.SerializeToArray(symbolRecord),
        };

        return Assemble(
            Tree(structuralEntries),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            Tree(contentEntries),
            Tree(symbolEntries));
    }

    // Builds a service whose single file declares several symbols (distinct start lines,
    // so the outline order is deterministic), used by the reuse tests that need an
    // outline entry carrying more than one independently reusable unit.
    private static RepoContextBundleService BuildServiceWithSymbols(
        string path, string body, long storedTokenCount, params (string FullyQualifiedName, string Signature)[] symbols)
    {
        var names = symbols.Select(s => s.FullyQualifiedName).ToArray();
        var node = new FileNode
        {
            RepoId = RepoId,
            Path = path,
            TokenCount = RepoContextValues.Lww(storedTokenCount, Clock(1)),
            DeclaredSymbols = RepoContextValues.Lww(DeclaredSymbolNames.Encode(names), Clock(1)),
        };
        var structuralEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.File(RepoId, path)] = Serializer.SerializeToArray(node),
        };
        var contentEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.Content(RepoId, path)] =
                Serializer.SerializeToArray(ContentRecord.Create(RepoId, path, body, Clock(1))),
        };
        var symbolEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        for (var i = 0; i < symbols.Length; i++)
        {
            var record = new SymbolRecord
            {
                RepoId = RepoId,
                FullyQualifiedName = symbols[i].FullyQualifiedName,
                Signature = RepoContextValues.Lww(symbols[i].Signature, Clock(1)),
                StartLine = RepoContextValues.Lww(i + 1L, Clock(1)),
            };
            symbolEntries[RepoContextKeys.Symbol(RepoId, symbols[i].FullyQualifiedName)] =
                Serializer.SerializeToArray(record);
        }

        return Assemble(
            Tree(structuralEntries),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            Tree(contentEntries),
            Tree(symbolEntries));
    }

    private static RepoContextBundleService Assemble(
        ILattice structural, ILattice memory, ILattice content, ILattice symbol,
        IRepoContextUsageRecorder? recorder = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var sessionTree = MutableTree(new Dictionary<string, byte[]>(StringComparer.Ordinal));
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural).Returns(structural);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Memory).Returns(memory);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Content).Returns(content);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol).Returns(symbol);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Session).Returns(sessionTree);

        var store = new RepoContextStore(
            grainFactory,
            Substitute.For<IRepoIndexRunner>(),
            Serializer,
            new RepoContextVectorWriter(grainFactory, Serializer, Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory)),
            Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

        var search = new RepoContextSearchService(
            grainFactory,
            Serializer,
            Substitute.For<IRepoContextSemanticIndex>(),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            embeddingProvider: null);

        var graph = new RepoContextGraphService(grainFactory, Serializer, Counter, new RepoContextWorkspaceGuard([]));
        var sessions = new RepoContextSessionStore(grainFactory, Serializer);

        return new RepoContextBundleService(
            search, graph, sessions, grainFactory, Serializer, Counter, recorder ?? NoOpUsageRecorder.Instance);
    }

    private static ILattice Tree(IReadOnlyDictionary<string, byte[]> map)
    {
        var tree = Substitute.For<ILattice>();
        var items = map.Select(kv => new KeyValuePair<string, byte[]>(kv.Key, kv.Value)).ToArray();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Yield(items));
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<byte[]?>(
                map.TryGetValue(ci.ArgAt<string>(0), out var value) ? value : null));
        return tree;
    }

    // A substitute whose SetAsync writes back into the backing dictionary, so bookkeeping
    // written by one BuildAsync call is observable by the next call on the same service.
    // Both the two-arg and the TTL overload persist; TTL itself is not modelled (these are
    // deterministic unit tests with no wall-clock dependence).
    private static ILattice MutableTree(Dictionary<string, byte[]> map)
    {
        var tree = Substitute.For<ILattice>();
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<byte[]?>(
                map.TryGetValue(ci.ArgAt<string>(0), out var value) ? value : null));
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                map[ci.ArgAt<string>(0)] = ci.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                map[ci.ArgAt<string>(0)] = ci.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });
        return tree;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Yield(
        params KeyValuePair<string, byte[]>[] items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }
}
