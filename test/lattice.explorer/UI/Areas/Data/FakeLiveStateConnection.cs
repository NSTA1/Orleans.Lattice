using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The in-memory state API as the shared connection presents it: every read goes to
/// the wrapped <see cref="FakeStateClient"/>, and a test moves the connection's
/// health with <see cref="Raise"/>, as a degrade and a recovery would.
/// </summary>
/// <param name="inner">The in-memory state API.</param>
internal sealed class FakeLiveStateConnection(FakeStateClient inner) : ILatticeStateConnection
{
    /// <summary>A connected status.</summary>
    public static readonly LatticeConnectionStatus Connected = new(LatticeConnectionState.Connected, "https://fake", "Connected.");

    /// <summary>A degraded status: the endpoint stopped answering.</summary>
    public static readonly LatticeConnectionStatus Faulted = new(LatticeConnectionState.Faulted, "https://fake", "The endpoint did not answer.");

    /// <inheritdoc />
    public LatticeConnectionStatus Status { get; private set; } = Connected;

    /// <inheritdoc />
    public event Action<LatticeConnectionStatus>? StatusChanged;

    /// <summary>Moves the connection to <paramref name="status"/> and raises <see cref="StatusChanged"/>.</summary>
    /// <param name="status">The new status.</param>
    public void Raise(LatticeConnectionStatus status)
    {
        Status = status;
        StatusChanged?.Invoke(status);
    }

    /// <inheritdoc />
    public Task ConfigureAsync(LatticeConnectionSettings settings, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task<bool> ReconnectAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

    /// <inheritdoc />
    public Task<bool> ProbeAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <inheritdoc />
    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => inner.ListTreesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default) => inner.ListViewsAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => inner.ListTagIndexesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => inner.ListTagValuesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => inner.ListCoveredTreesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default) => inner.ListIndexTagsAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default) => inner.ScanTagMembersAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default) => inner.GetTreeStructureAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default) => inner.ScanEntriesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default) => inner.GetEntryAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default) => inner.GetEntryHistoryAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default) => inner.CancelScanAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default) => inner.GetMetricsSnapshotAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default) => inner.GetClusterInfoAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<DeadLetterCountResponse> GetDeadLetterCountAsync(DeadLetterCountRequest request, CancellationToken cancellationToken = default) => inner.GetDeadLetterCountAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default) => inner.ListDeadLettersAsync(request, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default) => inner.ObserveChangesAsync(request, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default) => inner.ObserveMetricsAsync(request, cancellationToken);
}
