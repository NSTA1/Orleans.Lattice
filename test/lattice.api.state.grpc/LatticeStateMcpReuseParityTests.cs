namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Transport-neutrality (MCP-reuse) parity test. A future MCP surface is meant
/// to be a thin adapter over the same <see cref="ILatticeStateQuery"/> /
/// <see cref="ILatticeStateObserver"/> facade the gRPC binding adapts, with no
/// query logic of its own. This test stands up a minimal in-process adapter
/// over the facade (no gRPC) and asserts it returns results identical to the
/// gRPC path, demonstrating the facade is transport-neutral and MCP-ready
/// rather than merely asserting it.
/// </summary>
[Category("Integration")]
[TestFixture]
public class LatticeStateMcpReuseParityTests
{
    private const string TreeId = "mcp-parity";
    private const int KeyCount = 18;
    private const int ShardCount = 2;

    private GrpcStateClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new GrpcStateClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task in_process_adapter_matches_grpc_path()
    {
        await _fixture.CreatePopulatedTreeAsync(TreeId, KeyCount, ShardCount);

        await using var host = await _fixture.CreateGrpcHostAsync();
        var grpcClient = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        // The "MCP-style" adapter: a direct, transport-free consumer of the
        // facade. It performs no query logic itself, exactly as a real MCP
        // binding would delegate straight to the shared surface.
        var adapter = new InProcessStateAdapter(_fixture.Query);

        // Discovery parity.
        var grpcCatalog = await grpcClient.ListTreesAsync(new CatalogRequest { PageSize = 100 });
        var adapterCatalog = await adapter.ListTreesAsync(new CatalogRequest { PageSize = 100 });
        Assert.That(
            adapterCatalog.Entries.Select(e => (e.TreeId, e.ShardCount)),
            Is.EqualTo(grpcCatalog.Entries.Select(e => (e.TreeId, e.ShardCount))));

        // Structure parity (live-key sum across shard roots).
        var structureRequest = new StructureRequest
        {
            TreeId = TreeId,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        };
        var grpcStructure = await grpcClient.GetTreeStructureAsync(structureRequest);
        var adapterStructure = await adapter.GetTreeStructureAsync(structureRequest);
        Assert.That(
            adapterStructure.Roots.Sum(r => r.SubtreeKeyCount),
            Is.EqualTo(grpcStructure.Roots.Sum(r => r.SubtreeKeyCount)));
        Assert.That(adapterStructure.Roots, Has.Count.EqualTo(grpcStructure.Roots.Count));

        // Entry-scan parity (full key set, in order).
        var grpcKeys = await ScanAllKeysAsync(grpcClient, TreeId);
        var adapterKeysList = await adapter.ScanAllKeys(TreeId);
        Assert.That(adapterKeysList, Is.EqualTo(grpcKeys));
        Assert.That(adapterKeysList, Has.Count.EqualTo(KeyCount));
    }

    private static async Task<List<string>> ScanAllKeysAsync(LatticeStateApiGrpcClient client, string treeId)
    {
        var keys = new List<string>();
        string? token = null;
        do
        {
            var page = await client.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = treeId,
                PageSize = 7,
                ContinuationToken = token,
            });
            keys.AddRange(page.Entries.Select(e => e.Key));
            token = page.ContinuationToken;
        }
        while (!string.IsNullOrEmpty(token));

        return keys;
    }

    /// <summary>
    /// A deliberately thin, transport-free consumer of the read facade, standing
    /// in for a future MCP binding. It owns no query logic - every method simply
    /// projects the facade's result records, the same way the gRPC service does.
    /// </summary>
    private sealed class InProcessStateAdapter(ILatticeStateQuery query)
    {
        public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request)
            => query.ListTreesAsync(request);

        public async Task<StructureResponse> GetTreeStructureAsync(StructureRequest request)
        {
            var result = await query.GetTreeStructureAsync(request);
            return new StructureResponse
            {
                Status = result.Status,
                TreeId = result.TreeId,
                Roots = result.Roots,
                Truncated = result.Truncated,
            };
        }

        public async Task<List<string>> ScanAllKeys(string treeId)
        {
            var keys = new List<string>();
            string? token = null;
            do
            {
                var result = await query.ScanEntriesAsync(new EntryScanRequest
                {
                    TreeId = treeId,
                    PageSize = 7,
                    ContinuationToken = token,
                });
                keys.AddRange(result.Entries.Select(e => e.Key));
                token = result.ContinuationToken;
            }
            while (!string.IsNullOrEmpty(token));

            return keys;
        }
    }
}
