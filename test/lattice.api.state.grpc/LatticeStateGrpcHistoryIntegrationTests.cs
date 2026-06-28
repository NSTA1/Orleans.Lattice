using System.Text;
using Grpc.Core;
using Grpc.Net.Client;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Integration coverage for the <c>GetEntryHistory</c> unary RPC. Drives the
/// per-key change-history endpoint over an in-process <c>TestServer</c> backed
/// by a real <see cref="ILatticeStateQuery"/> facade and a durable history view,
/// asserting wire parity with the facade (revision order, kind, retention
/// descriptor, top-level bound), not-found status-code mapping, the OR-Set
/// member-change decode crossing the wire, the additive-contract decode of an
/// older-shaped request, and that the authorizer sees the
/// <see cref="LatticeStateApiOperation.GetEntryHistory"/> operation and target
/// tree.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class LatticeStateGrpcHistoryIntegrationTests
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
    public async Task get_entry_history_returns_revisions_over_grpc()
    {
        var treeId = $"grpc-hist-{Guid.NewGuid():N}";
        var view = $"{treeId}-view";
        var source = await _fixture.RegisterTreeAsync(treeId, shardCount: 1);
        await _fixture.CreateHistoryViewAsync(treeId, view);

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await _fixture.DrainToZeroAsync(view);

        var response = await CallAsync(_host.Channel, _host.Methods.GetEntryHistory,
            new EntryHistoryRequest { TreeId = treeId, Key = "k", Limit = 100 });

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(response.Bound, Is.EqualTo(EntryHistoryBound.BoundedByAge));
            Assert.That(response.Revisions, Has.Count.EqualTo(2));
            Assert.That(response.Revisions.Select(r => r.Hlc).ToList(), Is.Ordered);
            Assert.That(response.Revisions[0].Retention.Mode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(response.Revisions[^1].ValuePreview, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task get_entry_history_reverse_orders_newest_first_over_grpc()
    {
        var treeId = $"grpc-hist-rev-{Guid.NewGuid():N}";
        var view = $"{treeId}-view";
        var source = await _fixture.RegisterTreeAsync(treeId, shardCount: 1);
        await _fixture.CreateHistoryViewAsync(treeId, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await source.SetAsync("k", new byte[] { 3 });
        await _fixture.DrainToZeroAsync(view);

        var response = await CallAsync(_host.Channel, _host.Methods.GetEntryHistory,
            new EntryHistoryRequest { TreeId = treeId, Key = "k", Limit = 100, Reverse = true });

        Assert.That(response.Revisions, Has.Count.EqualTo(3));
        Assert.That(response.Revisions.Select(r => r.Hlc).ToList(), Is.Ordered.Descending);
    }

    [Test]
    public void get_entry_history_maps_missing_tree_to_not_found_status_code()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await CallAsync(_host.Channel, _host.Methods.GetEntryHistory,
                new EntryHistoryRequest { TreeId = $"missing-{Guid.NewGuid():N}", Key = "k" }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task get_entry_history_decodes_orset_member_changes_over_grpc()
    {
        var treeId = $"orset-grpc-hist-{Guid.NewGuid():N}";
        var view = $"{treeId}-view";
        var source = await _fixture.RegisterTreeAsync(treeId, shardCount: 1);
        await _fixture.CreateHistoryViewAsync(treeId, view);

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.OrSet("k").AddAsync(Encoding.UTF8.GetBytes("alpha"), "replica-a");
        await _fixture.DrainToZeroAsync(view);

        var response = await CallAsync(_host.Channel, _host.Methods.GetEntryHistory,
            new EntryHistoryRequest { TreeId = treeId, Key = "k", Limit = 100 });

        var added = response.Revisions
            .SelectMany(r => r.MemberChanges)
            .Where(m => m.Kind == CrdtMemberChangeKind.Added)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(response.Revisions.Any(r => r.Mode == LatticeMergeMode.OrSet), Is.True);
            Assert.That(added, Is.Not.Empty);
            Assert.That(added.Any(m => m.Element.SequenceEqual(Encoding.UTF8.GetBytes("alpha"))), Is.True);
        });
    }

    [Test]
    public async Task old_client_shaped_history_request_with_omitted_optional_fields_still_decodes()
    {
        var treeId = $"grpc-hist-compat-{Guid.NewGuid():N}";
        var view = $"{treeId}-view";
        var source = await _fixture.RegisterTreeAsync(treeId, shardCount: 1);
        await _fixture.CreateHistoryViewAsync(treeId, view);

        await source.SetAsync("k", new byte[] { 7 });
        await _fixture.DrainToZeroAsync(view);

        // An older client only knows TreeId / Key; bounds, paging, budget, and
        // reverse are all left at default. The additive [Id(n)] contract decodes.
        var response = await CallAsync(_host.Channel, _host.Methods.GetEntryHistory,
            new EntryHistoryRequest { TreeId = treeId, Key = "k" });

        Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(response.Revisions, Is.Not.Empty);
    }

    [Test]
    public async Task authorizer_receives_get_entry_history_operation_and_target_tree()
    {
        var treeId = $"grpc-authz-hist-{Guid.NewGuid():N}";
        var view = $"{treeId}-view";
        var source = await _fixture.RegisterTreeAsync(treeId, shardCount: 1);
        await _fixture.CreateHistoryViewAsync(treeId, view);
        await source.SetAsync("k", new byte[] { 1 });
        await _fixture.DrainToZeroAsync(view);

        var recorder = new RecordingStateApiAuthorizer(allow: true);
        await using var host = await _fixture.CreateGrpcHostAsync(recorder, requireAuthorization: true);

        await CallAsync(host.Channel, host.Methods.GetEntryHistory,
            new EntryHistoryRequest { TreeId = treeId, Key = "k" });

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Last.Operation, Is.EqualTo(LatticeStateApiOperation.GetEntryHistory));
            Assert.That(recorder.Last.TargetTreeId, Is.EqualTo(treeId));
        });
    }
}
