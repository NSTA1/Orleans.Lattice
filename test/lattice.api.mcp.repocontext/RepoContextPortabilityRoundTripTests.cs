using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// End-to-end round-trip tests for the repository-context portability primitive,
/// on the in-memory MCP harness cluster (issue #1438): export a prefix-scoped
/// range from one tree, import it into a fresh empty tree, and assert the logical
/// state (including opaque vectors and embedding-space tags) is identical; that a
/// re-import is idempotent and does not duplicate; and that the enumeration cursor
/// is resumable page by page.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextPortabilityRoundTripTests
{
    private static string SourceTree => $"repocontext-portability-src-{Guid.NewGuid():N}";
    private static string TargetTree => $"repocontext-portability-dst-{Guid.NewGuid():N}";

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    private static async Task SeedFilesAsync(ILattice tree, Serializer serializer, int count)
    {
        for (var i = 0; i < count; i++)
        {
            var path = $"src/file{i:D2}.cs";
            var node = new FileNode
            {
                RepoId = "acme",
                Path = path,
                Language = RepoContextValues.Lww("csharp", Clock(100 + i)),
            };
            await tree.SetAsync(RepoContextKeys.File("acme", path), serializer.SerializeToArray(node));
        }
    }

    [Test]
    public async Task Export_then_import_into_fresh_store_reproduces_identical_state_with_vectors()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        await SeedFilesAsync(source, serializer, 4);
        // A memory record under a different prefix must NOT be captured by a file-prefix export.
        await source.SetAsync(
            RepoContextKeys.Memory("acme", "notes", "n1"),
            serializer.SerializeToArray(new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "n1" }));

        // Opaque vector payloads keyed off the store key, resolved at export and captured at import.
        var vectors = new Dictionary<string, byte[]>
        {
            [RepoContextKeys.File("acme", "src/file00.cs")] = [1, 2, 3],
            [RepoContextKeys.File("acme", "src/file02.cs")] = [4, 5],
        };
        RepoContextVectorExport export = (key, _) =>
            ValueTask.FromResult<RepoContextVectorPayload?>(
                vectors.TryGetValue(key, out var v) ? new RepoContextVectorPayload(v, "onyx-v1") : null);

        var captured = new Dictionary<string, RepoContextVectorPayload>();
        RepoContextVectorImport import = (key, payload, _) =>
        {
            captured[key] = payload;
            return ValueTask.CompletedTask;
        };

        using var stream = new MemoryStream();
        var written = await RepoContextPortability.ExportAsync(
            source, RepoContextKeys.FilesPrefix("acme"), stream, serializer, export,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        stream.Position = 0;
        var result = await RepoContextPortability.ImportAsync(
            target, stream, serializer, vectorImport: import,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(written, Is.EqualTo(4), "Only the four file records fall under the file prefix.");
            Assert.That(result.RecordsRead, Is.EqualTo(4));
            Assert.That(result.RecordsMerged, Is.Zero, "The target started empty, so nothing was merged.");
            Assert.That(result.VectorsApplied, Is.EqualTo(2));
            Assert.That(result.FormatVersion, Is.EqualTo(RepoContextSnapshotFormat.CurrentVersion));
        });

        // The memory record was not exported, so the target has exactly the four files.
        Assert.That(await target.CountAsync(TestContext.CurrentContext.CancellationToken), Is.EqualTo(4));

        for (var i = 0; i < 4; i++)
        {
            var key = RepoContextKeys.File("acme", $"src/file{i:D2}.cs");
            var sourceBytes = await source.GetAsync(key, TestContext.CurrentContext.CancellationToken);
            var targetBytes = await target.GetAsync(key, TestContext.CurrentContext.CancellationToken);
            Assert.That(targetBytes, Is.EqualTo(sourceBytes), $"Value for {key} must round-trip byte-identically.");
        }

        Assert.Multiple(() =>
        {
            Assert.That(captured.Keys, Is.EquivalentTo(vectors.Keys));
            Assert.That(captured[RepoContextKeys.File("acme", "src/file00.cs")].Vector, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(captured[RepoContextKeys.File("acme", "src/file00.cs")].EmbeddingSpace, Is.EqualTo("onyx-v1"));
        });
    }

    [Test]
    public async Task Re_import_is_idempotent_and_does_not_duplicate()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        await SeedFilesAsync(source, serializer, 3);

        using var stream = new MemoryStream();
        await RepoContextPortability.ExportAsync(
            source, RepoContextKeys.FilesPrefix("acme"), stream, serializer,
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var snapshot = stream.ToArray();

        // First import lands three fresh records.
        using (var first = new MemoryStream(snapshot))
        {
            var r1 = await RepoContextPortability.ImportAsync(
                target, first, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(r1.RecordsMerged, Is.Zero);
        }

        var countAfterFirst = await target.CountAsync(TestContext.CurrentContext.CancellationToken);

        // Second import of the same snapshot merges every record (all now present) and converges.
        using (var second = new MemoryStream(snapshot))
        {
            var r2 = await RepoContextPortability.ImportAsync(
                target, second, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(r2.RecordsMerged, Is.EqualTo(3), "A re-import merges into the existing records.");
        }

        var countAfterSecond = await target.CountAsync(TestContext.CurrentContext.CancellationToken);
        var bytesAfterSecond = await target.GetAsync(
            RepoContextKeys.File("acme", "src/file01.cs"), TestContext.CurrentContext.CancellationToken);

        // Third import: the merge has reached a fixpoint, so the stored bytes no longer change.
        using (var third = new MemoryStream(snapshot))
        {
            await RepoContextPortability.ImportAsync(
                target, third, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);
        }

        var countAfterThird = await target.CountAsync(TestContext.CurrentContext.CancellationToken);
        var bytesAfterThird = await target.GetAsync(
            RepoContextKeys.File("acme", "src/file01.cs"), TestContext.CurrentContext.CancellationToken);
        var converged = serializer.Deserialize<FileNode>(bytesAfterThird!);

        Assert.Multiple(() =>
        {
            Assert.That(countAfterSecond, Is.EqualTo(countAfterFirst), "Re-import must not add duplicate keys.");
            Assert.That(countAfterThird, Is.EqualTo(countAfterFirst), "A further re-import still adds no keys.");
            Assert.That(countAfterThird, Is.EqualTo(3));
            Assert.That(bytesAfterThird, Is.EqualTo(bytesAfterSecond), "CRDT merge is a fixpoint: the value stops changing.");
            Assert.That(RepoContextValues.ReadString(converged.Language), Is.EqualTo("csharp"),
                "The logical value is preserved across re-imports.");
        });
    }

    [Test]
    public async Task Enumeration_cursor_is_resumable_across_pages()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);

        await SeedFilesAsync(source, serializer, 5);
        var prefix = RepoContextKeys.FilesPrefix("acme");

        var seen = new List<string>();
        string? token = null;
        var pages = 0;
        while (true)
        {
            var page = await RepoContextPortability.EnumerateAsync(
                source, prefix, token, pageSize: 2, vectorExport: null,
                cancellationToken: TestContext.CurrentContext.CancellationToken);
            pages++;
            seen.AddRange(page.Records.Select(r => r.Key));
            if (!page.HasMore)
            {
                break;
            }

            token = page.ContinuationToken;
            Assert.That(token, Is.Not.Null, "A has-more page must carry a continuation token.");
        }

        var expected = Enumerable.Range(0, 5)
            .Select(i => RepoContextKeys.File("acme", $"src/file{i:D2}.cs"))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(expected), "Every key is yielded exactly once, in ascending order.");
            Assert.That(pages, Is.GreaterThanOrEqualTo(3), "Five records at page size two span at least three pages.");
        });
    }
}
