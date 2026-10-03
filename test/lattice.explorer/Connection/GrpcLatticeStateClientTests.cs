using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// The Explorer's production state-API client, driven end to end against a real
/// loopback state-API gRPC server.
/// </summary>
/// <remarks>
/// <para>
/// Every member of this type is a one-line delegation onto the generated client,
/// which is exactly why it needs a transport-level test rather than a
/// substitute: the only thing that can be wrong is which RPC a delegation is
/// wired to, and a substitute cannot see that - it would assert the mapping the
/// test itself supplied. So each call here is answered by a distinctly-marked
/// value returned from a distinct facade method, and the assertion is that the
/// marker came back: a delegation pointed at the wrong RPC fails, because it
/// returns another method's marker or no marker at all.
/// </para>
/// <para>
/// <see cref="GrpcLatticeStateClient.Create"/> builds its own channel through
/// <see cref="LatticeGrpcChannelFactory"/> and so cannot have a test handler
/// swapped into it, which is why the server is a real listening endpoint
/// (<see cref="StateApiH2cServer"/>). That also makes this the only shape that
/// exercises <c>Create</c> and <c>Dispose</c> over a channel that really
/// connected.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class GrpcLatticeStateClientTests
{
    private const string Tree = "orders";

    private ILatticeStateQuery _query = null!;
    private ILatticeStateObserver _observer = null!;
    private ILatticeStateMetricsObserver _metrics = null!;
    private StateApiH2cServer _server = null!;
    private ServiceProvider _serializer = null!;
    private GrpcLatticeStateClient _client = null!;

    [OneTimeSetUp]
    public async Task StartAsync()
    {
        _query = Substitute.For<ILatticeStateQuery>();
        _observer = Substitute.For<ILatticeStateObserver>();
        _metrics = Substitute.For<ILatticeStateMetricsObserver>();
        Script();

        _server = await StateApiH2cServer.StartAsync(_query, _observer, _metrics);

        var serializer = new ServiceCollection();
        serializer.AddSerializer();
        _serializer = serializer.BuildServiceProvider();

        _client = GrpcLatticeStateClient.Create(
            new LatticeConnectionSettings { Address = _server.Address, AllowUnencryptedHttp2 = true },
            _serializer);
    }

    [OneTimeTearDown]
    public async Task StopAsync()
    {
        _client.Dispose();
        _serializer.Dispose();
        await _server.DisposeAsync();
    }

    [Test]
    public void The_client_rejects_missing_arguments()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(() => GrpcLatticeStateClient.Create(null!, provider), Throws.ArgumentNullException);
            Assert.That(
                () => GrpcLatticeStateClient.Create(new LatticeConnectionSettings { Address = _server.Address }, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Each_catalogue_listing_reaches_its_own_facade_method()
    {
        var trees = await _client.ListTreesAsync(new CatalogRequest());
        var views = await _client.ListViewsAsync(new CatalogRequest());
        var tagIndexes = await _client.ListTagIndexesAsync(new CatalogRequest());
        var tagValues = await _client.ListTagValuesAsync(new CatalogRequest { IndexName = "by-status" });
        var coveredTrees = await _client.ListCoveredTreesAsync(new CatalogRequest { IndexName = "by-status" });
        var indexTags = await _client.ListIndexTagsAsync(new CatalogRequest { IndexName = "by-status" });

        Assert.Multiple(() =>
        {
            Assert.That(trees.NextPageToken, Is.EqualTo("trees"));
            Assert.That(views.NextPageToken, Is.EqualTo("views"));
            Assert.That(tagIndexes.NextPageToken, Is.EqualTo("tag-indexes"));

            // These two share a request and a response type, so only the marker
            // tells them apart - which is the whole point of the test.
            Assert.That(tagValues.NextPageToken, Is.EqualTo("tag-values"));
            Assert.That(indexTags.NextPageToken, Is.EqualTo("index-tags"));

            Assert.That(coveredTrees.NextPageToken, Is.EqualTo("covered-trees"));
        });
    }

    [Test]
    public async Task A_tag_member_scan_reaches_the_tag_member_facade_method()
    {
        var page = await _client.ScanTagMembersAsync(new TagMemberScanRequest { IndexName = "by-status", Tag = "open" });

        Assert.That(page.NextPageToken, Is.EqualTo("tag-members"));
    }

    [Test]
    public async Task A_structure_read_carries_the_typed_status_rather_than_a_transport_fault()
    {
        var response = await _client.GetTreeStructureAsync(new StructureRequest { TreeId = Tree });

        Assert.Multiple(() =>
        {
            Assert.That(response.TreeId, Is.EqualTo("structure"));
            Assert.That(response.Truncated, Is.True);
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
        });
    }

    [Test]
    public async Task An_entry_scan_carries_its_continuation_token_back()
    {
        var response = await _client.ScanEntriesAsync(new EntryScanRequest { TreeId = Tree });

        Assert.Multiple(() =>
        {
            Assert.That(response.TreeId, Is.EqualTo("scan"));
            Assert.That(response.ContinuationToken, Is.EqualTo("scan-cursor"));
        });
    }

    [Test]
    public async Task A_point_read_carries_a_missing_key_back_as_a_status()
    {
        var response = await _client.GetEntryAsync(new EntryGetRequest { TreeId = Tree, Key = "order/1" });

        Assert.Multiple(() =>
        {
            Assert.That(response.TreeId, Is.EqualTo("get"));
            Assert.That(response.Key, Is.EqualTo("get-key"));
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.KeyNotFound), "a routine miss is structured content, not a fault");
        });
    }

    [Test]
    public async Task A_history_read_reaches_the_history_facade_method()
    {
        var response = await _client.GetEntryHistoryAsync(new EntryHistoryRequest { TreeId = Tree, Key = "order/1" });

        Assert.Multiple(() =>
        {
            Assert.That(response.TreeId, Is.EqualTo("history"));
            Assert.That(response.Key, Is.EqualTo("history-key"));
            Assert.That(response.ContinuationToken, Is.EqualTo("history-cursor"));
        });
    }

    [Test]
    public async Task A_scan_cancellation_releases_the_named_cursor()
    {
        var response = await _client.CancelScanAsync(new EntryScanCancelRequest { TreeId = Tree, ContinuationToken = "cursor-7" });

        Assert.That(response, Is.Not.Null);
        await _query.Received(1).CancelScanAsync(Tree, "cursor-7", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_metrics_snapshot_reaches_the_metrics_observer()
    {
        var snapshot = await _client.GetMetricsSnapshotAsync(new TreeMetricsRequest());

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.IsInitial, Is.True);
            Assert.That(snapshot.RemovedTreeIds, Is.EqualTo(new[] { "sample" }), "a pulled snapshot comes from SampleAsync, not from the stream");
        });
    }

    [Test]
    public async Task Cluster_information_reaches_the_cluster_facade_method()
    {
        var info = await _client.GetClusterInfoAsync(new ClusterInfoRequest());

        Assert.Multiple(() =>
        {
            Assert.That(info.ClusterId, Is.EqualTo("cluster-marker"));
            Assert.That(info.ServiceId, Is.EqualTo("service-marker"));
        });
    }

    [Test]
    public async Task The_dead_letter_reads_reach_their_own_facade_methods()
    {
        var count = await _client.GetDeadLetterCountAsync(new DeadLetterCountRequest { TreeId = Tree });
        var page = await _client.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = Tree });

        Assert.Multiple(() =>
        {
            Assert.That(count.TreeId, Is.EqualTo(Tree));
            Assert.That(count.Count, Is.EqualTo(7));
            Assert.That(page.NextPageToken, Is.EqualTo("dead-letters"));
        });
    }

    [Test]
    public async Task A_change_subscription_streams_every_notification_the_cluster_sends()
    {
        var keys = new List<string>();

        await foreach (var notification in _client.ObserveChangesAsync(new StateObserveRequest { TreeId = Tree }))
        {
            keys.Add(notification.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "order/1", "order/2" }));
    }

    [Test]
    public async Task A_metrics_subscription_streams_the_initial_snapshot_then_the_deltas()
    {
        var initial = new List<bool>();

        await foreach (var snapshot in _client.ObserveMetricsAsync(new TreeMetricsRequest()))
        {
            initial.Add(snapshot.IsInitial);
        }

        Assert.That(initial, Is.EqualTo(new[] { true, false }), "the stream opens with a full snapshot and continues with deltas");
    }

    [Test]
    public void A_disposed_client_tears_its_channel_down()
    {
        var settings = new LatticeConnectionSettings { Address = _server.Address, AllowUnencryptedHttp2 = true };
        var client = GrpcLatticeStateClient.Create(settings, _serializer);

        client.Dispose();

        Assert.That(
            async () => await client.ListTreesAsync(new CatalogRequest()),
            Throws.InstanceOf<ObjectDisposedException>().Or.InstanceOf<RpcException>(),
            "the channel is gone, so no further call can be made on it");
    }

    private void Script()
    {
        _query.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeCatalogPage { NextPageToken = "trees" }));
        _query.ListViewsAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ViewCatalogPage { NextPageToken = "views" }));
        _query.ListTagIndexesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TagIndexCatalogPage { NextPageToken = "tag-indexes" }));
        _query.ListTagValuesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TagValueCatalogPage { NextPageToken = "tag-values" }));
        _query.ListCoveredTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new CoveredTreeCatalogPage { NextPageToken = "covered-trees" }));
        _query.ListIndexTagsAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TagValueCatalogPage { NextPageToken = "index-tags" }));
        _query.ScanTagMembersAsync(Arg.Any<TagMemberScanRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TagMemberScanPage { NextPageToken = "tag-members" }));
        _query.GetTreeStructureAsync(Arg.Any<StructureRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeStructureResult { TreeId = "structure", Truncated = true }));
        _query.ScanEntriesAsync(Arg.Any<EntryScanRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new EntryScanResult { TreeId = "scan", ContinuationToken = "scan-cursor" }));
        _query.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new EntryDetailResult
            {
                TreeId = "get",
                Key = "get-key",
                Status = StateQueryStatus.KeyNotFound,
            }));
        _query.GetEntryHistoryAsync(Arg.Any<EntryHistoryRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new EntryHistoryResult
            {
                TreeId = "history",
                Key = "history-key",
                ContinuationToken = "history-cursor",
            }));
        _query.CancelScanAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        _query.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ClusterInfo { ClusterId = "cluster-marker", ServiceId = "service-marker" }));
        _query.GetDeadLetterCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(7));
        _query.ListDeadLettersAsync(Arg.Any<DeadLetterQueueRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DeadLetterQueuePage { NextPageToken = "dead-letters" }));

        _metrics.SampleAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeMetricsSnapshot { IsInitial = true, RemovedTreeIds = ["sample"] }));
        _metrics.ObserveAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Stream(
                new TreeMetricsSnapshot { IsInitial = true },
                new TreeMetricsSnapshot { IsInitial = false }));
        _observer.ObserveAsync(Arg.Any<StateObserveRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Stream(
                new StateChangeNotification { TreeId = Tree, Key = "order/1", Position = "1" },
                new StateChangeNotification { TreeId = Tree, Key = "order/2", Position = "2" }));
    }

    private static async IAsyncEnumerable<T> Stream<T>(params T[] items)
    {
        foreach (var item in items)
        {
            yield return item;
            await Task.Yield();
        }
    }
}
