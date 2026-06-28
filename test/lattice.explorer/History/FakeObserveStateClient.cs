using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.History;

/// <summary>
/// An <see cref="ILatticeStateClient"/> double whose change feed is driven by a
/// test: either a fixed sequence of notifications, or an unbounded channel a test
/// writes to and later completes, so cancellation and incremental-push behaviour
/// can be asserted deterministically. Records the last observe request.
/// </summary>
internal sealed class FakeObserveStateClient : ILatticeStateClient
{
    private readonly Channel<StateChangeNotification>? _channel;
    private readonly IReadOnlyList<StateChangeNotification>? _fixed;

    private FakeObserveStateClient(
        Channel<StateChangeNotification>? channel,
        IReadOnlyList<StateChangeNotification>? fixedItems)
    {
        _channel = channel;
        _fixed = fixedItems;
    }

    /// <summary>The most recent request passed to <see cref="ObserveChangesAsync"/>.</summary>
    public StateObserveRequest? LastObserve { get; private set; }

    /// <summary>Whether the observe enumeration observed a cancellation.</summary>
    public bool ObserveCancelled { get; private set; }

    /// <summary>Creates a client that yields the given notifications then completes.</summary>
    public static FakeObserveStateClient WithSequence(params StateChangeNotification[] items) =>
        new(channel: null, fixedItems: items);

    /// <summary>Creates a channel-backed client; write notifications via <see cref="Push"/> and end via <see cref="Complete"/>.</summary>
    public static FakeObserveStateClient Channelled() =>
        new(Channel.CreateUnbounded<StateChangeNotification>(), fixedItems: null);

    /// <summary>Pushes a notification into the channel-backed stream.</summary>
    public void Push(StateChangeNotification notification) => _channel!.Writer.TryWrite(notification);

    /// <summary>Completes the channel-backed stream.</summary>
    public void Complete() => _channel!.Writer.TryComplete();

    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(
        StateObserveRequest request,
        CancellationToken cancellationToken = default)
    {
        LastObserve = request;
        return _channel is not null
            ? FromChannel(cancellationToken)
            : FromSequence(cancellationToken);
    }

    private async IAsyncEnumerable<StateChangeNotification> FromSequence(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var item in _fixed!)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }

        await Task.CompletedTask;
    }

    private async IAsyncEnumerable<StateChangeNotification> FromChannel(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (true)
        {
            bool hasMore;
            try
            {
                hasMore = await _channel!.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                ObserveCancelled = true;
                throw;
            }

            if (!hasMore)
            {
                yield break;
            }

            while (_channel!.Reader.TryRead(out var item))
            {
                yield return item;
            }
        }
    }

    // Remaining surface is unused by the live-follow tests.
    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TreeCatalogPage());
    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new ViewCatalogPage());
    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TagIndexCatalogPage());
    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TagValueCatalogPage());
    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new StructureResponse { TreeId = "t" });
    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryScanResponse { TreeId = "t" });
    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryGetResponse { TreeId = request.TreeId, Key = request.Key, Status = StateQueryStatus.KeyNotFound });
    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryHistoryResponse { TreeId = request.TreeId, Key = request.Key });
    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryScanCancelResponse());
    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TreeMetricsSnapshot());
    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new ClusterInfo());
    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => EmptyAsync();

#pragma warning disable CS1998
    private static async IAsyncEnumerable<TreeMetricsSnapshot> EmptyAsync() { yield break; }
#pragma warning restore CS1998
}
