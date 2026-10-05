using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Optional extension to <see cref="IRemoteSnapshotTransport"/> for bindings
/// that can carry a close-generation trailer after the entry stream.
/// </summary>
public interface IRemoteSnapshotItemTransport : IRemoteSnapshotTransport
{
    /// <summary>Streams snapshot entries and an optional close-generation trailer.</summary>
    IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default);
}
