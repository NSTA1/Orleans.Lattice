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
    public async Task get_tree_structure_maps_missing_tree_to_tree_not_found_status()
    {
        var request = new StructureRequest { TreeId = $"missing-{Guid.NewGuid():N}" };

        // An unknown tree is part of the typed contract (issue #1396): it rides
        // as a structured Status, not an opaque NotFound transport fault.
        var response = await CallAsync(_host.Channel, _host.Methods.GetTreeStructure, request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(response.TreeId, Is.EqualTo(request.TreeId));
            Assert.That(response.Roots, Is.Empty);
        });
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
    public async Task scan_entries_maps_missing_tree_to_tree_not_found_status()
    {
        var request = new EntryScanRequest { TreeId = $"missing-{Guid.NewGuid():N}", PageSize = 10 };

        // An unknown tree is part of the typed contract (issue #1396): it rides
        // as a structured Status, not an opaque NotFound transport fault.
        var response = await CallAsync(_host.Channel, _host.Methods.ScanEntries, request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(response.TreeId, Is.EqualTo(request.TreeId));
            Assert.That(response.Entries, Is.Empty);
        });
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
    public async Task get_entry_maps_missing_key_to_a_typed_key_not_found_status()
    {
        // Issue #1339 Finding 1: a missing key in an existing tree is part of the
        // typed contract, not a transport fault. The binding returns a structured
        // response carrying StateQueryStatus.KeyNotFound with a null entry rather
        // than throwing a NotFound RpcException.
        var treeId = $"grpc-get-missing-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId, shardCount: 1);

        var response = await CallAsync(_host.Channel, _host.Methods.GetEntry,
            new EntryGetRequest { TreeId = treeId, Key = "no-such-key" });

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
            Assert.That(response.TreeId, Is.EqualTo(treeId));
            Assert.That(response.Key, Is.EqualTo("no-such-key"));
            Assert.That(response.Entry, Is.Null);
        });
    }

    [Test]
    public async Task get_entry_maps_missing_tree_to_a_typed_tree_not_found_status()
    {
        // Issue #1339 Finding 1: an unknown tree is distinguishable from a missing
        // key by status (TreeNotFound vs KeyNotFound), and neither throws.
        var treeId = $"missing-{Guid.NewGuid():N}";

        var response = await CallAsync(_host.Channel, _host.Methods.GetEntry,
            new EntryGetRequest { TreeId = treeId, Key = "k" });

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(response.TreeId, Is.EqualTo(treeId));
            Assert.That(response.Entry, Is.Null);
        });
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

    [Test]
    public async Task disabled_authorization_bypasses_even_a_denying_authorizer()
    {
        // With RequireAuthorization=false an outer boundary owns access control,
        // so the per-call authorizer must not run at all: a deny-all recorder is
        // never consulted and the call proceeds. This is the documented
        // "outer boundary guards the endpoint" escape hatch.
        var recorder = new RecordingStateApiAuthorizer(allow: false);
        await using var host = await _fixture.CreateGrpcHostAsync(recorder, requireAuthorization: false);

        var page = await CallAsync(host.Channel, host.Methods.ListTrees,
            new CatalogRequest { PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(page, Is.Not.Null);
            Assert.That(recorder.Count, Is.Zero, "the authorizer must not be consulted when enforcement is off");
        });
    }

    [Test]
    public async Task authorizer_receives_scan_operation_and_target_tree()
    {
        var treeId = $"grpc-authz-scan-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 4, shardCount: 1);

        var recorder = new RecordingStateApiAuthorizer(allow: true);
        await using var host = await _fixture.CreateGrpcHostAsync(recorder, requireAuthorization: true);

        await CallAsync(host.Channel, host.Methods.ScanEntries,
            new EntryScanRequest { TreeId = treeId, PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Last.Operation, Is.EqualTo(LatticeStateApiOperation.ScanEntries));
            Assert.That(recorder.Last.TargetTreeId, Is.EqualTo(treeId));
        });
    }

    [Test]
    public async Task authorizer_receives_get_entry_operation_and_target_tree()
    {
        var treeId = $"grpc-authz-get-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 4, shardCount: 1);

        var recorder = new RecordingStateApiAuthorizer(allow: true);
        await using var host = await _fixture.CreateGrpcHostAsync(recorder, requireAuthorization: true);

        await CallAsync(host.Channel, host.Methods.GetEntry,
            new EntryGetRequest { TreeId = treeId, Key = GrpcStateClusterFixture.KeyAt(1) });

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Last.Operation, Is.EqualTo(LatticeStateApiOperation.GetEntry));
            Assert.That(recorder.Last.TargetTreeId, Is.EqualTo(treeId));
        });
    }

    [Test]
    public async Task authorizer_receives_structure_operation_and_target_tree()
    {
        var treeId = $"grpc-authz-struct-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 8, shardCount: 1);

        var recorder = new RecordingStateApiAuthorizer(allow: true);
        await using var host = await _fixture.CreateGrpcHostAsync(recorder, requireAuthorization: true);

        await CallAsync(host.Channel, host.Methods.GetTreeStructure, new StructureRequest { TreeId = treeId });

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Last.Operation, Is.EqualTo(LatticeStateApiOperation.GetTreeStructure));
            Assert.That(recorder.Last.TargetTreeId, Is.EqualTo(treeId));
        });
    }

    [Test]
    public async Task authorizer_receives_null_target_tree_for_catalog_operations()
    {
        var recorder = new RecordingStateApiAuthorizer(allow: true);
        await using var host = await _fixture.CreateGrpcHostAsync(recorder, requireAuthorization: true);

        await CallAsync(host.Channel, host.Methods.ListTrees, new CatalogRequest { PageSize = 10 });
        var afterListTrees = recorder.Last;

        await CallAsync(host.Channel, host.Methods.ListViews, new CatalogRequest { PageSize = 10 });
        var afterListViews = recorder.Last;

        Assert.Multiple(() =>
        {
            Assert.That(afterListTrees.Operation, Is.EqualTo(LatticeStateApiOperation.ListTrees));
            Assert.That(afterListTrees.TargetTreeId, Is.Null, "cluster-wide catalog ops are not tree-scoped");
            Assert.That(afterListViews.Operation, Is.EqualTo(LatticeStateApiOperation.ListViews));
            Assert.That(afterListViews.TargetTreeId, Is.Null);
        });
    }

    [Test]
    public async Task tree_scoped_authorizer_allows_one_tree_and_denies_another()
    {
        var allowedTree = $"grpc-authz-allow-{Guid.NewGuid():N}";
        var deniedTree = $"grpc-authz-deny-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(allowedTree, keyCount: 4, shardCount: 1);
        await _fixture.CreatePopulatedTreeAsync(deniedTree, keyCount: 4, shardCount: 1);

        var authorizer = new TreeScopedStateApiAuthorizer(allowedTree);
        await using var host = await _fixture.CreateGrpcHostAsync(authorizer, requireAuthorization: true);

        var allowed = await CallAsync(host.Channel, host.Methods.ScanEntries,
            new EntryScanRequest { TreeId = allowedTree, PageSize = 10 });
        Assert.That(allowed.Status, Is.EqualTo(StateQueryStatus.Found));

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(host.Channel, host.Methods.ScanEntries,
                new EntryScanRequest { TreeId = deniedTree, PageSize = 10 }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied),
            "a per-tree policy must deny a tree outside its allowed set");
    }

    [Test]
    public async Task scan_entries_maps_malformed_continuation_token_to_invalid_argument()
    {
        var treeId = $"grpc-bad-token-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 4, shardCount: 1);

        // A continuation token naming an unknown/stale cursor is a malformed
        // client request and must map to InvalidArgument, not Internal (which
        // would mask the client error as a server fault).
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(_host.Channel, _host.Methods.ScanEntries,
                new EntryScanRequest { TreeId = treeId, PageSize = 5, ContinuationToken = "not-a-real-cursor" }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public async Task scan_entries_over_grpc_reads_cluster_distributed_state_from_non_primary_silo()
    {
        var fixture = new GrpcStateClusterFixture();
        await fixture.InitializeAsync(siloCount: 3);
        try
        {
            var treeId = $"grpc-multisilo-{Guid.NewGuid():N}";
            await fixture.CreatePopulatedTreeAsync(treeId, keyCount: 24, shardCount: 6);

            // Bind the gRPC surface to a NON-primary silo's facade: the write
            // path targeted the cluster client, the grains are distributed
            // across all three silos, and the read must still observe them all.
            await using var host = await fixture.CreateGrpcHostAsync(facade: fixture.QueryOnSilo(1));

            var seen = new List<string>();
            string? token = null;
            do
            {
                var page = await CallAsync(host.Channel, host.Methods.ScanEntries,
                    new EntryScanRequest { TreeId = treeId, PageSize = 7, ContinuationToken = token });
                seen.AddRange(page.Entries.Select(e => e.Key));
                token = page.ContinuationToken;
            }
            while (!string.IsNullOrEmpty(token));

            Assert.That(seen, Is.EquivalentTo(Enumerable.Range(0, 24).Select(GrpcStateClusterFixture.KeyAt)));
        }
        finally
        {
            await fixture.DisposeAsync();
        }
    }
}

/// <summary>
/// Test authorizer that records the most recent
/// <see cref="LatticeStateApiAuthorizationContext"/> it observed so a test can
/// assert the operation and target tree were decoded and forwarded correctly.
/// </summary>
internal sealed class RecordingStateApiAuthorizer(bool allow) : ILatticeStateApiAuthorizer
{
    private LatticeStateApiAuthorizationContext _last;
    private int _count;

    public LatticeStateApiAuthorizationContext Last => _last;

    /// <summary>Number of times the authorizer was consulted. A test probe.</summary>
    public int Count => Volatile.Read(ref _count);

    public Task<bool> IsAuthorizedAsync(
        LatticeStateApiAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _count);
        _last = authorizationContext;
        return Task.FromResult(allow);
    }
}

/// <summary>
/// Test authorizer that permits only calls scoped to a specific tree (and the
/// cluster-wide catalog operations), denying every other tree. Exercises the
/// per-tree decisioning the enriched authorization context enables.
/// </summary>
internal sealed class TreeScopedStateApiAuthorizer(string allowedTreeId) : ILatticeStateApiAuthorizer
{
    public Task<bool> IsAuthorizedAsync(
        LatticeStateApiAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
    {
        var allowed = authorizationContext.TargetTreeId is null
            || string.Equals(authorizationContext.TargetTreeId, allowedTreeId, StringComparison.Ordinal);
        return Task.FromResult(allowed);
    }
}
