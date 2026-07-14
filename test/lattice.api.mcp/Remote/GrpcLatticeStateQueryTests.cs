using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeStateQuery"/>, the remote-host adapter
/// that fronts <see cref="ILatticeStateQuery"/> over the state-API gRPC client.
/// Every wire-backed member is proven to forward its request and map its
/// response; the three members with no gRPC binding are proven to fail loud with
/// <see cref="NotSupportedException"/>; cancellation and the null-client guard
/// are covered. All deterministic over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeStateQueryTests
{
    private static GrpcLatticeStateQuery Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.StateClient(invoker));

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeStateQuery(null!), Throws.ArgumentNullException);

    [Test]
    public async Task ListTreesAsync_forwards_request_and_returns_page()
    {
        var page = new TreeCatalogPage { NextPageToken = "cursor" };
        var invoker = new FakeCallInvoker(_ => page);
        var request = new CatalogRequest { PageSize = 7 };

        var result = await Adapter(invoker).ListTreesAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(invoker.LastRequest, Is.SameAs(request));
        });
    }

    [Test]
    public async Task ListViewsAsync_returns_page()
    {
        var page = new ViewCatalogPage { NextPageToken = "v" };
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListViewsAsync(new CatalogRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task ListTagIndexesAsync_returns_page()
    {
        var page = new TagIndexCatalogPage();
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListTagIndexesAsync(new CatalogRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task ListTagValuesAsync_returns_page()
    {
        var page = new TagValueCatalogPage();
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListTagValuesAsync(new CatalogRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task ListCoveredTreesAsync_returns_page()
    {
        var page = new CoveredTreeCatalogPage();
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListCoveredTreesAsync(new CatalogRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task ListIndexTagsAsync_returns_page()
    {
        var page = new TagValueCatalogPage();
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListIndexTagsAsync(new CatalogRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task ScanTagMembersAsync_returns_page()
    {
        var page = new TagMemberScanPage();
        var request = new TagMemberScanRequest { IndexName = "ix", Tag = "t" };
        var result = await Adapter(new FakeCallInvoker(_ => page)).ScanTagMembersAsync(request);
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task ListDeadLettersAsync_returns_page()
    {
        var page = new DeadLetterQueuePage();
        var request = new DeadLetterQueueRequest { TreeId = "t" };
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListDeadLettersAsync(request);
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task GetClusterInfoAsync_returns_wire_cluster_info()
    {
        var info = new ClusterInfo { ClusterId = "c-1", ServiceId = "s-1" };
        var result = await Adapter(new FakeCallInvoker(_ => info)).GetClusterInfoAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.ClusterId, Is.EqualTo("c-1"));
            Assert.That(result.ServiceId, Is.EqualTo("s-1"));
        });
    }

    [Test]
    public async Task GetTreeStructureAsync_maps_every_field()
    {
        var response = new StructureResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree",
            Roots = Array.Empty<NodeStateSummary>(),
            Truncated = true,
        };
        var request = new StructureRequest { TreeId = "tree" };

        var result = await Adapter(new FakeCallInvoker(_ => response)).GetTreeStructureAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.TreeId, Is.EqualTo("tree"));
            Assert.That(result.Roots, Is.SameAs(response.Roots));
            Assert.That(result.Truncated, Is.True);
        });
    }

    [Test]
    public async Task ScanEntriesAsync_maps_fields_and_continuation()
    {
        var response = new EntryScanResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree",
            Entries = Array.Empty<EntryRecord>(),
            ContinuationToken = "next",
        };
        var request = new EntryScanRequest { TreeId = "tree" };

        var result = await Adapter(new FakeCallInvoker(_ => response)).ScanEntriesAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("tree"));
            Assert.That(result.Entries, Is.SameAs(response.Entries));
            Assert.That(result.ContinuationToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task GetEntryAsync_wraps_tree_and_key_then_maps()
    {
        var response = new EntryGetResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree",
            Key = "k",
            Entry = new EntryRecord { Key = "k" },
        };
        var invoker = new FakeCallInvoker(_ => response);

        var result = await Adapter(invoker).GetEntryAsync("tree", "k");

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.TypeOf<EntryGetRequest>());
            var sent = (EntryGetRequest)invoker.LastRequest!;
            Assert.That(sent.TreeId, Is.EqualTo("tree"));
            Assert.That(sent.Key, Is.EqualTo("k"));
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Entry, Is.SameAs(response.Entry));
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_maps_every_field()
    {
        var response = new EntryHistoryResponse
        {
            Status = StateQueryStatus.Found,
            TreeId = "tree",
            Key = "k",
            Revisions = Array.Empty<EntryRevisionRecord>(),
            ContinuationToken = "c",
            Bound = EntryHistoryBound.Truncated,
        };
        var request = new EntryHistoryRequest { TreeId = "tree", Key = "k" };

        var result = await Adapter(new FakeCallInvoker(_ => response)).GetEntryHistoryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("tree"));
            Assert.That(result.Key, Is.EqualTo("k"));
            Assert.That(result.Revisions, Is.SameAs(response.Revisions));
            Assert.That(result.ContinuationToken, Is.EqualTo("c"));
            Assert.That(result.Bound, Is.EqualTo(EntryHistoryBound.Truncated));
        });
    }

    [Test]
    public async Task CancelScanAsync_wraps_tree_and_token()
    {
        var invoker = new FakeCallInvoker(_ => new EntryScanCancelResponse());

        await Adapter(invoker).CancelScanAsync("tree", "tok");

        var sent = (EntryScanCancelRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("tree"));
            Assert.That(sent.ContinuationToken, Is.EqualTo("tok"));
        });
    }

    [Test]
    public async Task GetDeadLetterCountAsync_unwraps_count()
    {
        var response = new DeadLetterCountResponse { TreeId = "tree", Count = 42 };
        var result = await Adapter(new FakeCallInvoker(_ => response)).GetDeadLetterCountAsync("tree");
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public void GetTreeSummaryAsync_has_no_binding_and_throws()
        => Assert.That(
            () => Adapter(new FakeCallInvoker(_ => throw new InvalidOperationException())).GetTreeSummaryAsync("t"),
            Throws.TypeOf<NotSupportedException>());

    [Test]
    public void GetShardSummariesAsync_has_no_binding_and_throws()
        => Assert.That(
            () => Adapter(new FakeCallInvoker(_ => throw new InvalidOperationException())).GetShardSummariesAsync("t"),
            Throws.TypeOf<NotSupportedException>());

    [Test]
    public void GetPhysicalShardCountAsync_has_no_binding_and_throws()
        => Assert.That(
            () => Adapter(new FakeCallInvoker(_ => throw new InvalidOperationException())).GetPhysicalShardCountAsync("t"),
            Throws.TypeOf<NotSupportedException>());

    [Test]
    public void ListTreesAsync_propagates_cancellation()
    {
        var page = new TreeCatalogPage();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => page)).ListTreesAsync(new CatalogRequest(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
