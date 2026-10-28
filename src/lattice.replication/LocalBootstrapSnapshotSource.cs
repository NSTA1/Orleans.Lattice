using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IBootstrapSnapshotSource"/> implementation used
/// when no <see cref="IRemoteSnapshotTransport"/> is registered on the
/// silo. Forwards the source-less overload, and the source-named
/// overload when the named source is this silo's own
/// <see cref="LatticeReplicationOptions.ClusterId"/>, to the silo's
/// local <see cref="ISnapshotProvider"/>, so the bootstrap state
/// machine drains from the local tree (the single-cluster recovery
/// path) without needing a cross-cluster transport.
/// <para>
/// A source-named export that names any other cluster - or any source
/// at all while the local cluster id is unset - fails closed with
/// <see cref="LatticeBootstrapSourceMismatchException"/> and never
/// reaches the local provider. The local tree's export is not the
/// named peer's contents: accepting it would let the coordinator
/// record the local lineage as the peer's drained lineage and consume
/// that peer's pending re-seed request without ever importing from it
/// (issue #5109).
/// </para>
/// </summary>
internal sealed class LocalBootstrapSnapshotSource(
    ISnapshotProvider local,
    IOptionsMonitor<LatticeReplicationOptions> options) : IBootstrapSnapshotSource
{
    private readonly ISnapshotProvider _local = local ?? throw new ArgumentNullException(nameof(local));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options = options ?? throw new ArgumentNullException(nameof(options));

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
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(sourceClusterId);

        var localClusterId = _options.Get(treeName).ClusterId ?? string.Empty;
        if (localClusterId.Length == 0
            || !string.Equals(localClusterId, sourceClusterId, StringComparison.Ordinal))
        {
            return Task.FromException<SnapshotStream>(
                new LatticeBootstrapSourceMismatchException(treeName, sourceClusterId, localClusterId));
        }

        return _local.ExportAsync(treeName, sourceClusterId, asOfHlc, cancellationToken);
    }
}
