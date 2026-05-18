using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IBootstrapSnapshotSource"/> implementation used
/// when no <see cref="IRemoteSnapshotTransport"/> is registered on the
/// silo. Forwards both overloads of
/// <see cref="ISnapshotProvider.ExportAsync(string, HybridLogicalClock, CancellationToken)"/>
/// to the silo's local <see cref="ISnapshotProvider"/>, so the
/// bootstrap state machine drains from the local tree (the
/// single-cluster recovery path) without needing a cross-cluster
/// transport.
/// </summary>
internal sealed class LocalBootstrapSnapshotSource(ISnapshotProvider local) : IBootstrapSnapshotSource
{
    private readonly ISnapshotProvider _local = local ?? throw new ArgumentNullException(nameof(local));

    /// <inheritdoc />
    public Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default) =>
        _local.ExportAsync(treeName, asOfHlc, cancellationToken);

    /// <inheritdoc />
    public Task<SnapshotStream> ExportAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default) =>
        _local.ExportAsync(treeName, sourceClusterId, asOfHlc, cancellationToken);
}
