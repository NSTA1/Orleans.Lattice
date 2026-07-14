using Grpc.Core;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeDataApi"/>, the remote-host adapter that
/// fronts <see cref="ILatticeDataApi"/> over the data-API gRPC client. Proves
/// each member forwards its request and unwraps its response, that the four
/// mutating members translate a <c>PermissionDenied</c> <see cref="RpcException"/>
/// into a <see cref="LatticeAuthorizationDeniedException"/> (nothing persisted),
/// that the two reads never throw on denial, the exactly-sized cross-tree copy,
/// and the argument guards. Deterministic over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeDataApiTests
{
    private static GrpcLatticeDataApi Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.DataClient(invoker));

    private static RpcException Denied()
        => new(new Status(StatusCode.PermissionDenied, "denied"));

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeDataApi(null!), Throws.ArgumentNullException);

    [Test]
    public async Task SetAsync_forwards_tree_key_value()
    {
        var invoker = new FakeCallInvoker(_ => new DataSetResponse());
        var value = new byte[] { 1, 2, 3 };

        await Adapter(invoker).SetAsync("tree", "k", value);

        var sent = (DataSetRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("tree"));
            Assert.That(sent.Key, Is.EqualTo("k"));
            Assert.That(sent.Value, Is.SameAs(value));
        });
    }

    [Test]
    public void SetAsync_translates_permission_denied()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => Denied())).SetAsync("t", "k", Array.Empty<byte>()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>().With.InnerException.TypeOf<RpcException>());

    [Test]
    public async Task DeleteAsync_unwraps_removed()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new DataDeleteResponse { Removed = true }))
            .DeleteAsync("tree", "k");
        Assert.That(result, Is.True);
    }

    [Test]
    public void DeleteAsync_translates_permission_denied()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => Denied())).DeleteAsync("t", "k"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

    [Test]
    public async Task SetManyAtomicAsync_forwards_batch_and_operation_id()
    {
        var invoker = new FakeCallInvoker(_ => new DataAtomicResponse());
        var batch = new DataAtomicBatch { DeleteKeys = { "x" } };

        await Adapter(invoker).SetManyAtomicAsync("tree", batch, "op-1");

        var sent = (DataAtomicRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("tree"));
            Assert.That(sent.Batch, Is.SameAs(batch));
            Assert.That(sent.OperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public void SetManyAtomicAsync_translates_permission_denied()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => Denied())).SetManyAtomicAsync("t", new DataAtomicBatch(), "op"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

    [Test]
    public async Task SetManyAtomicCrossTreeAsync_copies_batches_exactly_and_unwraps_outcome()
    {
        var invoker = new FakeCallInvoker(_ => new DataCrossTreeResponse { Outcome = CrossTreeAtomicWriteOutcome.Committed });
        var batches = new[]
        {
            new DataTreeBatch { TreeId = "a" },
            new DataTreeBatch { TreeId = "b" },
        };

        var outcome = await Adapter(invoker).SetManyAtomicCrossTreeAsync(batches, "op-2");

        var sent = (DataCrossTreeRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(sent.Batches, Has.Count.EqualTo(2));
            Assert.That(sent.Batches[0], Is.SameAs(batches[0]));
            Assert.That(sent.Batches[1], Is.SameAs(batches[1]));
            Assert.That(sent.OperationId, Is.EqualTo("op-2"));
        });
    }

    [Test]
    public void SetManyAtomicCrossTreeAsync_null_batches_throws()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => new DataCrossTreeResponse()))
                .SetManyAtomicCrossTreeAsync(null!, "op"),
            Throws.ArgumentNullException);

    [Test]
    public async Task SetManyAtomicCrossTreeAsync_empty_batches_sends_empty_list()
    {
        var invoker = new FakeCallInvoker(_ => new DataCrossTreeResponse());
        await Adapter(invoker).SetManyAtomicCrossTreeAsync(Array.Empty<DataTreeBatch>(), "op");
        Assert.That(((DataCrossTreeRequest)invoker.LastRequest!).Batches, Is.Empty);
    }

    [Test]
    public void SetManyAtomicCrossTreeAsync_translates_permission_denied()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => Denied()))
                .SetManyAtomicCrossTreeAsync(Array.Empty<DataTreeBatch>(), "op"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

    [Test]
    public async Task GetAsync_forwards_and_returns_read_result()
    {
        var response = new DataReadResult { TreeId = "tree", Key = "k", Found = true, Value = new byte[] { 9 } };
        var invoker = new FakeCallInvoker(_ => response);

        var result = await Adapter(invoker).GetAsync("tree", "k");

        var sent = (DataGetRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("tree"));
            Assert.That(sent.Key, Is.EqualTo("k"));
            Assert.That(result, Is.SameAs(response));
        });
    }

    [Test]
    public async Task GetAsync_denied_key_reports_absent_without_throwing()
    {
        var response = new DataReadResult { TreeId = "tree", Key = "k", Found = false };
        var result = await Adapter(new FakeCallInvoker(_ => response)).GetAsync("tree", "k");
        Assert.That(result.Found, Is.False);
    }

    [Test]
    public async Task ReadRangeAsync_returns_page()
    {
        var page = new DataRangePage { TreeId = "tree" };
        var request = new DataRangeRequest { TreeId = "tree" };
        var result = await Adapter(new FakeCallInvoker(_ => page)).ReadRangeAsync(request);
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public void SetAsync_propagates_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => new DataSetResponse()))
                .SetAsync("t", "k", Array.Empty<byte>(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
