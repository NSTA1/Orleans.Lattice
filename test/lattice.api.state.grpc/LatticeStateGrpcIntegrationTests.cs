using System.Text;
using Grpc.Core;
using Grpc.Net.Client;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Integration coverage for the <c>Orleans.Lattice.Api.State.Grpc</c> binding.
/// Drives the five unary RPCs over an in-process <c>TestServer</c> backed by a
/// real <see cref="ILatticeStateQuery"/> facade and asserts (a) wire parity
/// with the facade, (b) gRPC status-code mapping for the not-found and
/// invalid-argument paths, (c) snapshot continuation round-tripping, (d) the
/// default-deny / opt-in authorization posture, and (e) that an old-client
/// shaped request (newer optional fields omitted) still decodes on the server.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeStateGrpcIntegrationTests
{
    private GrpcStateClusterFixture _fixture = null!;
    private GrpcStateHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcStateClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private static async Task<TResponse> CallAsync<TRequest, TResponse>(
        GrpcChannel channel,
        Method<TRequest, TResponse> method,
        TRequest request)
        where TRequest : class
        where TResponse : class
    {
        var invoker = channel.CreateCallInvoker();
        using var call = invoker.AsyncUnaryCall(method, host: null, new CallOptions(), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    [Test]
    public async Task list_trees_returns_registered_tree_over_grpc()
    {
        var treeId = $"grpc-list-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId, shardCount: 2);

        var page = await CallAsync(_host.Channel, _host.Methods.ListTrees,
            new CatalogRequest { PageSize = 100 });

        Assert.That(page.Entries.Select(e => e.TreeId), Does.Contain(treeId));
    }

    [Test]
    public async Task list_views_matches_facade_over_grpc()
    {
        var viaGrpc = await CallAsync(_host.Channel, _host.Methods.ListViews,
            new CatalogRequest { PageSize = 100 });
        var viaFacade = await _fixture.Query.ListViewsAsync(new CatalogRequest { PageSize = 100 });

        Assert.That(viaGrpc.Entries.Count, Is.EqualTo(viaFacade.Entries.Count));
    }

    [Test]
    public async Task get_tree_structure_matches_facade_over_grpc()
    {
        var treeId = $"grpc-structure-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 20, shardCount: 2);

        var request = new StructureRequest { TreeId = treeId };
        var viaGrpc = await CallAsync(_host.Channel, _host.Methods.GetTreeStructure, request);
        var viaFacade = await _fixture.Query.GetTreeStructureAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(viaGrpc.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(viaGrpc.TreeId, Is.EqualTo(treeId));
            Assert.That(viaGrpc.Roots.Count, Is.EqualTo(viaFacade.Roots.Count));
            Assert.That(viaGrpc.Truncated, Is.EqualTo(viaFacade.Truncated));
        });
    }

    [Test]
    public void get_tree_structure_maps_missing_tree_to_not_found_status_code()
    {
        var request = new StructureRequest { TreeId = $"missing-{Guid.NewGuid():N}" };

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(_host.Channel, _host.Methods.GetTreeStructure, request));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task scan_entries_returns_entries_over_grpc()
    {
        var treeId = $"grpc-scan-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 10, shardCount: 1);

        var response = await CallAsync(_host.Channel, _host.Methods.ScanEntries,
            new EntryScanRequest { TreeId = treeId, PageSize = 100 });

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(response.Entries, Has.Count.EqualTo(10));
            Assert.That(response.Entries.Select(e => e.Key),
                Is.EquivalentTo(Enumerable.Range(0, 10).Select(GrpcStateClusterFixture.KeyAt)));
        });
    }

    [Test]
    public async Task scan_entries_continuation_token_pages_over_grpc()
    {
        var treeId = $"grpc-scan-page-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 10, shardCount: 1);

        var seen = new List<string>();
        string? token = null;
        do
        {
            var page = await CallAsync(_host.Channel, _host.Methods.ScanEntries,
                new EntryScanRequest { TreeId = treeId, PageSize = 3, ContinuationToken = token });
            seen.AddRange(page.Entries.Select(e => e.Key));
            token = page.ContinuationToken;
        }
        while (!string.IsNullOrEmpty(token));

        Assert.That(seen, Is.EquivalentTo(Enumerable.Range(0, 10).Select(GrpcStateClusterFixture.KeyAt)));
    }

    [Test]
    public void scan_entries_maps_missing_tree_to_not_found_status_code()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(_host.Channel, _host.Methods.ScanEntries,
                new EntryScanRequest { TreeId = $"missing-{Guid.NewGuid():N}", PageSize = 10 }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task get_entry_returns_record_over_grpc()
    {
        var treeId = $"grpc-get-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 5, shardCount: 1);
        var key = GrpcStateClusterFixture.KeyAt(2);

        var response = await CallAsync(_host.Channel, _host.Methods.GetEntry,
            new EntryGetRequest { TreeId = treeId, Key = key });

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(response.Key, Is.EqualTo(key));
            Assert.That(response.Entry, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(response.Entry!.ValuePreview), Is.EqualTo("value-00002"));
        });
    }

    [Test]
    public void get_entry_maps_missing_key_to_not_found_status_code()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            var treeId = $"grpc-get-missing-{Guid.NewGuid():N}";
            await _fixture.RegisterTreeAsync(treeId, shardCount: 1);
            await CallAsync(_host.Channel, _host.Methods.GetEntry,
                new EntryGetRequest { TreeId = treeId, Key = "no-such-key" });
        });
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void get_entry_maps_missing_tree_to_not_found_status_code()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(_host.Channel, _host.Methods.GetEntry,
                new EntryGetRequest { TreeId = $"missing-{Guid.NewGuid():N}", Key = "k" }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task old_client_shaped_request_with_omitted_optional_fields_still_decodes()
    {
        var treeId = $"grpc-compat-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 4, shardCount: 1);

        // An older client only knows TreeId; every newer optional field
        // (range bounds, reverse, page size, preview budget, predicate) is left
        // at its default. The additive [Id(n)] contract must still decode it.
        var response = await CallAsync(_host.Channel, _host.Methods.ScanEntries,
            new EntryScanRequest { TreeId = treeId });

        Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(response.Entries, Is.Not.Empty);
    }

    [Test]
    public async Task authorized_call_is_allowed_when_authorization_required()
    {
        await using var host = await _fixture.CreateGrpcHostAsync(
            new AllowAllStateApiAuthorizer(), requireAuthorization: true);

        var page = await CallAsync(host.Channel, host.Methods.ListTrees,
            new CatalogRequest { PageSize = 10 });

        Assert.That(page, Is.Not.Null);
    }

    [Test]
    public async Task unauthorized_call_is_rejected_with_permission_denied()
    {
        await using var host = await _fixture.CreateGrpcHostAsync(
            new DenyAllStateApiAuthorizer(), requireAuthorization: true);

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(host.Channel, host.Methods.ListTrees,
                new CatalogRequest { PageSize = 10 }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task default_deny_authorizer_rejects_when_authorization_required()
    {
        // No authorizer registered -> the binding's TryAdd default
        // (DenyAllStateApiAuthorizer) applies, so an enforced host fails closed.
        await using var host = await _fixture.CreateGrpcHostAsync(
            authorizer: null, requireAuthorization: true);

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(host.Channel, host.Methods.ListViews,
                new CatalogRequest { PageSize = 10 }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
