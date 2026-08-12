using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// End-to-end tests for the read-only <c>repocontext_search</c> tool over the real
/// MCP protocol via <see cref="RepoContextMcpHarness"/>: fail-closed authorization
/// gating (offered to a reader and a writer, withheld from an unauthenticated
/// caller), graceful degradation to keyword recall when no embedder is configured,
/// the bootstrap-to-vectorisation wiring (a bound embedder lands vectors that the
/// tool then finds by meaning), hydration of canonical records from index hits, and
/// the value-size/churn discipline that keeps live membership bounded and never
/// accumulates multiple payloads in a single vector value.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo and an
/// in-process MCP server. A deterministic <see cref="FakeEmbeddingProvider"/>
/// stands in for the Onyx model server so ranking is reproducible without a
/// container.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextSearchToolTests
{
    private const string RepoId = "sample-repo";
    private const string ToolName = "repocontext_search";

    private readonly List<string> _tempRoots = new();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [TearDown]
    public void TearDown()
    {
        foreach (var root in _tempRoots)
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }

        _tempRoots.Clear();
    }

    private string NewRepo()
    {
        var root = Path.Combine(Path.GetTempPath(), "rcs-tool-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private static void Write(string root, string relativePath, string content)
    {
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content, Encoding.UTF8);
    }

    private static RepoContextMcpHarnessOptions WithEmbedder(
        RepoContextMcpAuthPosture posture, FakeEmbeddingProvider embedder)
        => new()
        {
            Posture = posture,
            ConfigureServices = services => services.AddSingleton<IEmbeddingProvider>(embedder),
        };

    private async Task<JsonElement> SearchAsync(McpClient client, string query, int k = 0)
    {
        var args = new Dictionary<string, object?> { ["repoId"] = RepoId, ["query"] = query };
        if (k > 0)
        {
            args["k"] = k;
        }

        var result = await client.CallToolAsync(ToolName, args, cancellationToken: Ct);
        return result.RequireStructuredContent();
    }

    private async Task BootstrapAsync(McpClient client, string root)
    {
        var args = new Dictionary<string, object?> { ["repoRoot"] = root, ["repoId"] = RepoId };
        await client.CallToolAsync("repocontext_bootstrap", args, cancellationToken: Ct);
        // Onboarding runs asynchronously; wait for the job to settle so the search
        // assertions run against a fully ingested and vectorised store.
        await client.WaitForIndexAsync(RepoId, Ct);
    }

    // -- Authorization gating --------------------------------------------------

    [Test]
    public async Task Reader_is_offered_the_search_tool()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Reader }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Does.Contain(ToolName),
            "The read-only search tool is offered to any authorized reader.");
    }

    [Test]
    public async Task Writer_is_offered_the_search_tool()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Does.Contain(ToolName),
            "A writer sees the whole read-only surface, including search.");
    }

    [Test]
    public async Task Unauthenticated_caller_is_not_offered_and_cannot_call_search()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Unauthenticated }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(names, Is.Empty, "A fail-closed session is offered no tools at all.");
            Assert.That(
                () => client.CallToolAsync(
                    ToolName,
                    new Dictionary<string, object?> { ["repoId"] = RepoId, ["query"] = "x" },
                    cancellationToken: Ct).AsTask(),
                Throws.InstanceOf<McpException>(),
                "An unauthenticated caller is denied the tool at the protocol layer.");
        });
    }

    // -- Graceful degradation --------------------------------------------------

    [Test]
    public async Task Search_degrades_to_keyword_recall_when_no_embedder_is_configured()
    {
        var root = NewRepo();
        Write(root, "src/OrderService.cs", "class OrderService {}");
        Write(root, "src/Unrelated.cs", "class Unrelated {}");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);
        var json = await SearchAsync(client, "OrderService");

        Assert.Multiple(() =>
        {
            Assert.That(json.GetProperty("mode").GetString(), Is.EqualTo("keyword"),
                "With no embedder bound, search falls back to structural keyword recall.");
            var hits = json.GetProperty("hits");
            Assert.That(hits.GetArrayLength(), Is.GreaterThan(0));
            Assert.That(hits[0].GetProperty("entry").GetProperty("key").GetString(),
                Does.Contain("OrderService"));
            var hasVectorId = hits[0].TryGetProperty("vectorId", out var vectorId)
                && vectorId.ValueKind != JsonValueKind.Null;
            Assert.That(hasVectorId, Is.False, "A keyword hit carries no vector id.");
        });
    }

    [Test]
    public async Task Search_falls_back_to_keyword_when_the_embedder_is_unavailable()
    {
        var root = NewRepo();
        Write(root, "src/Widget.cs", "class Widget {}");

        var embedder = new FakeEmbeddingProvider { Available = false };
        await using var harness = await RepoContextMcpHarness.StartAsync(
            WithEmbedder(RepoContextMcpAuthPosture.Writer, embedder), Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);
        var json = await SearchAsync(client, "Widget");

        Assert.That(json.GetProperty("mode").GetString(), Is.EqualTo("keyword"),
            "An unreachable embedder degrades to keyword recall rather than throwing.");
    }

    // -- Bootstrap to vectorisation to search ----------------------------------

    [Test]
    public async Task Bootstrap_with_a_bound_embedder_lands_vectors_that_search_finds_by_meaning()
    {
        var root = NewRepo();
        Write(root, "src/OrderService.cs", "class OrderService { void PlaceOrder() {} }");
        Write(root, "src/PaymentGateway.cs", "class PaymentGateway { void Charge() {} }");

        var embedder = new FakeEmbeddingProvider();
        await using var harness = await RepoContextMcpHarness.StartAsync(
            WithEmbedder(RepoContextMcpAuthPosture.Writer, embedder), Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);

        // The vectors must have landed on the reserved metadata tree.
        var metadata = await CollectKeysAsync(harness, RepoContextTrees.VectorMetadata, RepoContextKeys.VectorsPrefix(RepoId));
        Assert.That(metadata, Has.Count.EqualTo(2), "Both files were embedded and stored as vectors.");

        var json = await SearchAsync(client, "class OrderService PlaceOrder");

        Assert.Multiple(() =>
        {
            Assert.That(json.GetProperty("mode").GetString(), Is.EqualTo("semantic"),
                "With a bound, available embedder search runs the semantic path.");
            var hits = json.GetProperty("hits");
            Assert.That(hits.GetArrayLength(), Is.GreaterThan(0));
            // The order-service file shares the most tokens with the query, so it ranks first.
            Assert.That(hits[0].GetProperty("entry").GetProperty("key").GetString(),
                Does.Contain("OrderService"));
            Assert.That(hits[0].GetProperty("vectorId").GetString(), Is.Not.Null.And.Not.Empty,
                "A semantic hit carries the identity of the matched vector.");
        });
    }

    [Test]
    public async Task Bootstrap_skips_empty_files_so_one_does_not_poison_the_embedding_batch()
    {
        // Regression: a real embedding server (Onyx) rejects a whole batch that
        // contains any empty string, so a single contentless file in the repo
        // would fail-close the entire run's vectorisation and silently drop
        // search to keyword mode. The ingestor must filter empty/whitespace
        // files out of the batch before embedding.
        var root = NewRepo();
        Write(root, "src/OrderService.cs", "class OrderService { void PlaceOrder() {} }");
        Write(root, "src/Empty.cs", string.Empty);
        Write(root, "src/Whitespace.cs", "   \n\t  ");

        var embedder = new FakeEmbeddingProvider { RejectEmptyStrings = true };
        await using var harness = await RepoContextMcpHarness.StartAsync(
            WithEmbedder(RepoContextMcpAuthPosture.Writer, embedder), Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);

        // Only the one file with content is embedded; the empty and
        // whitespace-only files are skipped rather than poisoning the batch.
        var metadata = await CollectKeysAsync(harness, RepoContextTrees.VectorMetadata, RepoContextKeys.VectorsPrefix(RepoId));
        Assert.That(metadata, Has.Count.EqualTo(1), "Only the contentful file is vectorised; empty files are skipped.");

        var json = await SearchAsync(client, "class OrderService PlaceOrder");
        Assert.That(json.GetProperty("mode").GetString(), Is.EqualTo("semantic"),
            "The empty file must not fail-close the batch; semantic search still runs.");
    }

    [Test]
    public async Task Bootstrap_vectorises_more_files_than_one_embed_batch()
    {
        // Regression: a real repository has far more files than fit in a single
        // embed request. The ingestor must chunk the run so a large repository
        // never builds one oversized call (which tripped the provider's HTTP
        // timeout and fail-closed the whole run) - and every file's vector must
        // still land across the batches.
        var root = NewRepo();
        const int fileCount = 70; // > EmbeddingRepoContextVectorIngestor.EmbedBatchSize (32)
        for (var i = 0; i < fileCount; i++)
        {
            Write(root, $"src/File{i:D3}.cs", $"class File{i:D3} {{ void Method{i:D3}() {{}} }}");
        }

        var embedder = new FakeEmbeddingProvider();
        await using var harness = await RepoContextMcpHarness.StartAsync(
            WithEmbedder(RepoContextMcpAuthPosture.Writer, embedder), Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);

        var metadata = await CollectKeysAsync(harness, RepoContextTrees.VectorMetadata, RepoContextKeys.VectorsPrefix(RepoId));
        Assert.That(metadata, Has.Count.EqualTo(fileCount),
            "Every file must be vectorised across multiple embed batches.");

        var json = await SearchAsync(client, "class File042 Method042");
        Assert.That(json.GetProperty("mode").GetString(), Is.EqualTo("semantic"),
            "Chunked vectorisation still yields a semantic index.");
    }

    [Test]
    public async Task Semantic_hit_hydrates_the_canonical_record_from_the_store_of_record()
    {
        var root = NewRepo();
        Write(root, "src/OrderService.cs", "class OrderService { void PlaceOrder() {} }");

        var embedder = new FakeEmbeddingProvider();
        await using var harness = await RepoContextMcpHarness.StartAsync(
            WithEmbedder(RepoContextMcpAuthPosture.Writer, embedder), Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);
        var json = await SearchAsync(client, "class OrderService PlaceOrder");

        var entry = json.GetProperty("hits")[0].GetProperty("entry");
        Assert.Multiple(() =>
        {
            Assert.That(entry.GetProperty("exists").GetBoolean(), Is.True,
                "The hit is hydrated from the live canonical record, not a copy held by the index.");
            Assert.That(entry.GetProperty("key").GetString(),
                Is.EqualTo(RepoContextKeys.File(RepoId, "src/OrderService.cs")));
            Assert.That(entry.GetProperty("kind").GetString(), Is.EqualTo("File"));
        });
    }

    // -- Value-size and churn discipline ---------------------------------------

    [Test]
    public async Task Repeated_reembed_keeps_live_membership_bounded_and_one_payload_per_key()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var space = new EmbeddingSpace("churn-model", 4, normalized: true);
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");

        // Re-embed the same source many times with a distinct vector each time.
        for (var i = 0; i < 6; i++)
        {
            var vector = new[] { 0.1f * i, 1f - (0.1f * i), 0.25f, 0.5f };
            await writer.StoreAsync(RepoId, sourceKey, space, vector, Ct);
        }

        // Live membership stays bounded: exactly one live source identifier.
        var membershipTree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var membershipBytes = await membershipTree.GetAsync(
            RepoContextKeys.VectorMembership(RepoId, RepoContextVectorWriter.SourceCollection), Ct);
        Assert.That(membershipBytes, Is.Not.Null);
        var membership = serializer.Deserialize<VectorMembershipRecord>(membershipBytes!);
        Assert.That(membership.Members.Elements().Count(), Is.EqualTo(1),
            "The stable source identifier is a member exactly once, however many times it re-embeds.");

        // Exactly one live metadata (presence) key for the source: the prior ones
        // were deleted, leaving tree tombstones for the compactor to reclaim.
        var liveVectors = await CollectKeysAsync(harness, RepoContextTrees.VectorMetadata, RepoContextKeys.VectorsPrefix(RepoId));
        Assert.That(liveVectors, Has.Count.EqualTo(1),
            "A re-embed retires the source's prior presence key, so only one vector is live.");

        // No single vector value accumulates multiple payloads: every payload key
        // holds exactly one immutable, content-addressed vector.
        var payloadTree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorPayload);
        var payloadKeys = await CollectKeysAsync(harness, RepoContextTrees.VectorPayload, RepoContextKeys.VectorPayloadsPrefix(RepoId));
        Assert.That(payloadKeys, Is.Not.Empty);
        foreach (var key in payloadKeys)
        {
            var bytes = await payloadTree.GetAsync(key, Ct);
            var payload = serializer.Deserialize<VectorPayloadRecord>(bytes!);
            Assert.That(payload.Payload.Count, Is.EqualTo(1),
                "One vector per key: a payload value never accumulates multiple embeddings.");
        }
    }

    private static async Task<List<string>> CollectKeysAsync(
        RepoContextMcpHarness harness, string treeName, string prefix)
    {
        var tree = harness.GrainFactory.GetGrain<ILattice>(treeName);
        var keys = new List<string>();
        string? token = null;
        do
        {
            var page = await RepoContextPortability.EnumerateAsync(
                tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, CancellationToken.None);
            keys.AddRange(page.Records.Select(r => r.Key));
            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        return keys;
    }
}
