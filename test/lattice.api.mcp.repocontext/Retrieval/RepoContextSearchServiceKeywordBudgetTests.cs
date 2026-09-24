using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Regression tests for the candidate bound on <see cref="RepoContextSearchService"/>'s
/// keyword fallback. The bound used to be shared across the structural, memory, and
/// content trees, scanned in that order, so a repository whose structural tree held
/// more records than the bound exhausted it before the memory tree was opened: memory
/// and content each contributed exactly their first record, and a memory entry was
/// unfindable by keyword even when queried by a word only it contained. Each tree now
/// scans under its own <see cref="RepoContextSearchService.MaxKeywordScanPerTree"/>.
/// </summary>
[TestFixture]
public sealed class RepoContextSearchServiceKeywordBudgetTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public async Task Keyword_search_finds_a_memory_entry_behind_a_structural_tree_larger_than_the_bound()
    {
        var service = CreateService(StructuralOverTheBound(), MemoryTree(), ContentTree());

        var result = await service.SearchAsync(RepoId, "zebrafish", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("keyword"),
                "A memory entry that is not the first key of its tree must still be a keyword candidate.");
            Assert.That(result.Hits.Select(h => h.Entry.Key),
                Does.Contain(RepoContextKeys.Memory(RepoId, "notes", "b-zebrafish")));
        });
    }

    [Test]
    public async Task Keyword_search_finds_file_content_behind_a_structural_tree_larger_than_the_bound()
    {
        var service = CreateService(StructuralOverTheBound(), MemoryTree(), ContentTree());

        var result = await service.SearchAsync(RepoId, "quokka", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("keyword"),
                "Body text of a file that is not the first key of the content tree must still be a keyword candidate.");
            Assert.That(result.Hits.Select(h => h.Entry.Key),
                Does.Contain(RepoContextKeys.Content(RepoId, "src/B.cs")));
        });
    }

    [Test]
    public async Task Keyword_search_still_bounds_the_candidates_it_reads_from_one_tree()
    {
        // A match placed past the per-tree bound in the structural tree is outside the
        // candidate set, so the bound still holds for each tree on its own.
        var beyond = $"src/F{RepoContextSearchService.MaxKeywordScanPerTree + 5:D6}Yak.cs";
        var service = CreateService(StructuralOverTheBound(), MemoryTree(), ContentTree());

        var result = await service.SearchAsync(RepoId, "yak", 10, CancellationToken.None);

        Assert.That(result.Hits.Select(h => h.Entry.Key),
            Does.Not.Contain(RepoContextKeys.File(RepoId, beyond)));
    }

    private static SortedDictionary<string, byte[]> StructuralOverTheBound()
    {
        var entries = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var count = RepoContextSearchService.MaxKeywordScanPerTree + 10;
        for (var i = 0; i < count; i++)
        {
            var path = i == RepoContextSearchService.MaxKeywordScanPerTree + 5
                ? $"src/F{i:D6}Yak.cs"
                : $"src/F{i:D6}.cs";
            entries[RepoContextKeys.File(RepoId, path)] =
                Serializer.SerializeToArray(new FileNode { RepoId = RepoId, Path = path });
        }

        return entries;
    }

    private static SortedDictionary<string, byte[]> MemoryTree()
    {
        var entries = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        AddMemory(entries, "a-first", "An unrelated note");
        AddMemory(entries, "b-zebrafish", "Zebrafish husbandry");
        return entries;
    }

    private static void AddMemory(SortedDictionary<string, byte[]> entries, string id, string title)
    {
        var record = new MemoryRecord
        {
            RepoId = RepoId,
            Topic = "notes",
            Id = id,
            Kind = MemoryKind.Note,
            Title = RepoContextValues.Lww(title, Clock(1)),
        };
        entries[RepoContextKeys.Memory(RepoId, "notes", id)] =
            MemoryRegisterTestEncoding.EncodeSingle(Serializer, "r", record);
    }

    private static SortedDictionary<string, byte[]> ContentTree()
    {
        var entries = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        AddContent(entries, "src/A.cs", "nothing of note");
        AddContent(entries, "src/B.cs", "a quokka lives here");
        return entries;
    }

    private static void AddContent(SortedDictionary<string, byte[]> entries, string path, string text)
    {
        var record = new ContentRecord
        {
            RepoId = RepoId,
            Path = path,
            Text = RepoContextValues.Lww(text, Clock(1)),
        };
        entries[RepoContextKeys.Content(RepoId, path)] = Serializer.SerializeToArray(record);
    }

    private static RepoContextSearchService CreateService(
        SortedDictionary<string, byte[]> structural,
        SortedDictionary<string, byte[]> memory,
        SortedDictionary<string, byte[]> content)
    {
        var structuralTree = RangeTree(structural);
        var memoryTree = RangeTree(memory);
        var contentTree = RangeTree(content);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural).Returns(structuralTree);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Memory).Returns(memoryTree);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Content).Returns(contentTree);

        var store = new RepoContextStore(
            grainFactory,
            Substitute.For<IRepoIndexRunner>(),
            Serializer,
            new RepoContextVectorWriter(grainFactory, Serializer, Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory)),
            Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

        return new RepoContextSearchService(
            grainFactory,
            Serializer,
            Substitute.For<IRepoContextSemanticIndex>(),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            new RepoContextRetrievalLatencyReporter(),
            embeddingProvider: null);
    }

    /// <summary>
    /// A tree double whose entry scan honours the requested key range, so the paged
    /// enumeration the keyword fallback drives advances and terminates as it does
    /// against a real tree.
    /// </summary>
    private static ILattice RangeTree(SortedDictionary<string, byte[]> entries)
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync(default, default, default, default, default).ReturnsForAnyArgs(call =>
        {
            var start = call.ArgAt<string?>(0);
            var end = call.ArgAt<string?>(1);
            return Yield(entries.Where(e =>
                (start is null || string.CompareOrdinal(e.Key, start) >= 0)
                && (end is null || string.CompareOrdinal(e.Key, end) < 0)));
        });
        return tree;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Yield(
        IEnumerable<KeyValuePair<string, byte[]>> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }
}
