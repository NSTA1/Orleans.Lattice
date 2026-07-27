using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// End-to-end coverage that a fault escaping a facade-backed tool is translated
/// by <see cref="McpToolFaultTranslator"/> at the
/// <see cref="CredentialStampingTool"/> seam and surfaced to a real
/// <see cref="McpClient"/> as an actionable error result - never the SDK's
/// opaque generic mask (issue #1352). A real client connects to an in-process
/// Kestrel host with the data tool module registered, an
/// <see cref="AllowAllMcpAuthorizer"/> so the granted data tools are reachable,
/// and a stub <see cref="ILatticeDataApi"/> whose read throws the exception under
/// test. The test drives <c>lattice_data_get</c> and asserts the returned
/// <see cref="CallToolResult"/> is an error whose text carries the translated,
/// class-specific message rather than the bare
/// <c>"An error occurred invoking 'lattice_data_get'."</c> mask.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it binds a loopback TCP port and drives the full
/// MCP streamable-HTTP handshake, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class McpToolFaultTranslationEndToEndTests
{
    [Test]
    public async Task Remote_rpc_fault_of_any_status_is_surfaced_with_status_and_detail_end_to_end()
    {
        var fault = new RpcException(
            new Status(StatusCode.Internal, "internal boom"));

        var text = await InvokeDataGetAndReadErrorTextAsync(fault);

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("server-side fault"),
                "A remote Internal fault must surface as a server-side fault, not the generic SDK mask.");
            Assert.That(text, Does.Contain("Internal"),
                "The gRPC status code must be surfaced.");
            Assert.That(text, Does.Not.Contain("An error occurred invoking 'lattice_data_get'."),
                "The bare SDK mask (no detail) must never be what the caller sees.");
        });
    }

    [Test]
    public async Task Remote_failed_precondition_detail_is_surfaced_verbatim_end_to_end()
    {
        var fault = new RpcException(
            new Status(StatusCode.FailedPrecondition, "replication is already enabled"));

        var text = await InvokeDataGetAndReadErrorTextAsync(fault);

        Assert.That(text, Does.Contain("replication is already enabled"),
            "A FailedPrecondition detail is operator guidance and must be surfaced verbatim.");
    }

    [Test]
    public async Task Local_process_fault_is_surfaced_with_type_and_message_end_to_end()
    {
        var fault = new FileNotFoundException("Could not load Orleans.Lattice.Replication.");

        var text = await InvokeDataGetAndReadErrorTextAsync(fault);

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("failed locally"),
                "A local MCP-host fault must be surfaced as a local failure.");
            Assert.That(text, Does.Contain(nameof(FileNotFoundException)),
                "The local fault's type name must be surfaced so an operator can diagnose it.");
            Assert.That(text, Does.Contain("Could not load Orleans.Lattice.Replication."),
                "The local fault's message must be surfaced - it never crossed the trust boundary.");
        });
    }

    private static async Task<string> InvokeDataGetAndReadErrorTextAsync(Exception fault)
    {
        await using var host = await StartHostAsync(fault);
        var endpoint = new Uri(host.Urls.First(), UriKind.Absolute);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var transport = new HttpClientTransport(
            new HttpClientTransportOptions
            {
                Endpoint = endpoint,
                TransportMode = HttpTransportMode.StreamableHttp,
            });
        await using var client = await McpClient.CreateAsync(transport, cancellationToken: cts.Token);

        var result = await client.CallToolAsync(
            "lattice_data_get",
            new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k" },
            cancellationToken: cts.Token);

        await host.StopAsync(cts.Token);

        Assert.That(result.IsError, Is.True,
            "A tool whose facade throws must return an error result.");
        var block = result.Content.OfType<TextContentBlock>().FirstOrDefault();
        Assert.That(block, Is.Not.Null, "The error result must carry a text content block.");
        return block!.Text;
    }

    private static async Task<WebApplication> StartHostAsync(Exception fault)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        // Grant the caller the data group and let the coarse authorizer through,
        // so the data tools are reachable and the only thing exercised is the
        // fault-translation seam on the invocation path.
        builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge>(
            new StubBridge(new LatticeCredential("agent")));
        builder.Services.AddSingleton<ILatticeApiMcpPermissionResolver>(
            new StubResolver(LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data)));
        builder.Services.AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer());

        // The facade the data tool resolves per invocation; its read throws the
        // fault under test so the seam has something to translate.
        builder.Services.AddSingleton<ILatticeDataApi>(new ThrowingDataApi(fault));

        builder.Services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = false;
            o.EnableDataTools = true;
        });
        builder.Services.AddDataTools();

        var app = builder.Build();
        app.MapLatticeMcp();
        await app.StartAsync();
        return app;
    }

    private sealed class StubBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(Microsoft.AspNetCore.Http.HttpContext context) => credential;
    }

    private sealed class StubResolver(LatticeApiMcpAccessSet access) : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
            LatticeCredential credential,
            CancellationToken cancellationToken)
            => new(access);
    }

    private sealed class ThrowingDataApi(Exception fault) : ILatticeDataApi
    {
        public Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => throw fault;

        public Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
            => throw fault;

        public Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => throw fault;

        public Task SetManyAtomicAsync(string treeId, DataAtomicBatch batch, string operationId, CancellationToken cancellationToken = default)
            => throw fault;

        public Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(IReadOnlyList<DataTreeBatch> batches, string operationId, CancellationToken cancellationToken = default)
            => throw fault;

        public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)
            => throw fault;

        public Task SetManyAsync(string treeId, IReadOnlyList<DataEntry> upserts, CancellationToken cancellationToken = default)
            => throw fault;

        public Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default) => throw fault;
        public Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default) => throw fault;
        public Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default) => throw fault;
        public Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default) => throw fault;
        public Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw fault;
        public Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw fault;
        public Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw fault;
        public Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw fault;
        public Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw fault;
        public Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw fault;
        public Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default) => throw fault;
        public Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
        public Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw fault;
        public Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default) => throw fault;
        public Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw fault;
    }
}
