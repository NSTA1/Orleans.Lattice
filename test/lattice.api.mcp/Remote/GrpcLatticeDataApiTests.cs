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

    [Test]
    public async Task SetManyAsync_forwards_tree_and_upserts()
    {
        var invoker = new FakeCallInvoker(_ => new DataSetManyResponse());
        var upserts = new[]
        {
            new DataEntry { Key = "a", Value = new byte[] { 1 } },
            new DataEntry { Key = "b", Value = new byte[] { 2 } },
        };

        await Adapter(invoker).SetManyAsync("tree", upserts);

        var sent = (DataSetManyRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("tree"));
            Assert.That(sent.Upserts.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public void SetManyAsync_translates_permission_denied()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => Denied())).SetManyAsync("t", Array.Empty<DataEntry>()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

    [Test]
    public async Task CounterIncrementAsync_forwards_a_typed_write_request()
    {
        var invoker = new FakeCallInvoker(_ => new CrdtWriteResponse());

        await Adapter(invoker).CounterIncrementAsync("tree", "c", "r1", 5);

        var sent = (CrdtWriteRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("tree"));
            Assert.That(sent.Key, Is.EqualTo("c"));
            Assert.That(sent.Op, Is.EqualTo(CrdtWriteOp.CounterIncrement));
            Assert.That(sent.ReplicaId, Is.EqualTo("r1"));
            Assert.That(sent.Amount, Is.EqualTo(5));
        });
    }

    [Test]
    public async Task SequenceInsertAtAsync_forwards_index_and_element()
    {
        var invoker = new FakeCallInvoker(_ => new CrdtWriteResponse());

        await Adapter(invoker).SequenceInsertAtAsync("tree", "q", 2, "r1", new byte[] { 7 });

        var sent = (CrdtWriteRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Op, Is.EqualTo(CrdtWriteOp.SequenceInsertAt));
            Assert.That(sent.Index, Is.EqualTo(2));
            Assert.That(sent.Element, Is.EqualTo(new byte[] { 7 }));
        });
    }

    [Test]
    public void SetAddAsync_translates_permission_denied()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => Denied())).SetAddAsync("t", "k", new byte[] { 1 }, "r1"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

    [Test]
    public async Task CounterGetAsync_maps_the_counter_value()
    {
        var invoker = new FakeCallInvoker(_ => new CrdtReadResponse { CounterValue = 42 });

        var value = await Adapter(invoker).CounterGetAsync("tree", "c");

        var sent = (CrdtReadRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Kind, Is.EqualTo(CrdtKind.PnCounter));
            Assert.That(value, Is.EqualTo(42));
        });
    }

    [Test]
    public async Task SetGetAsync_maps_the_element_bytes()
    {
        var invoker = new FakeCallInvoker(_ => new CrdtReadResponse { Elements = { new byte[] { 1 }, new byte[] { 2 } } });

        var elements = await Adapter(invoker).SetGetAsync("tree", "s");

        Assert.That(elements.Select(e => e[0]), Is.EqualTo(new byte[] { 1, 2 }));
    }

    [Test]
    public async Task VersionVectorGetAsync_maps_the_vector_entries()
    {
        var invoker = new FakeCallInvoker(_ => new CrdtReadResponse
        {
            Vector = { new CrdtVectorEntry { ReplicaId = "r1", Clock = "9:1" } },
        });

        var vector = await Adapter(invoker).VersionVectorGetAsync("tree", "v");

        Assert.That(vector["r1"], Is.EqualTo("9:1"));
    }

    [Test]
    public async Task MapGetAsync_maps_the_fields()
    {
        var invoker = new FakeCallInvoker(_ => new CrdtReadResponse
        {
            Map = { new CrdtMapField { Field = "title", Values = { new byte[] { 3 } } } },
        });

        var map = await Adapter(invoker).MapGetAsync("tree", "doc");

        Assert.Multiple(() =>
        {
            Assert.That(map.Keys, Is.EquivalentTo(new[] { "title" }));
            Assert.That(map["title"][0], Is.EqualTo(new byte[] { 3 }));
        });
    }
}
