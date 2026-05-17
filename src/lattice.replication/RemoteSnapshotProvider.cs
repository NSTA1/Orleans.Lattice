using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side <see cref="ISnapshotProvider"/> adapter that drives a
/// cross-cluster snapshot drain through an
/// <see cref="IRemoteSnapshotTransport"/> binding. Hosts register this
/// implementation in place of the default <c>LatticeSnapshotProvider</c>
/// when the local cluster is the snapshot receiver (the consumer side
/// of cross-cluster bootstrap) and the sender lives on a peer cluster
/// reachable only through a wire-shaped transport.
/// <para>
/// The adapter implements the three-arg overload of
/// <see cref="ISnapshotProvider.ExportAsync(string, string, HybridLogicalClock, CancellationToken)"/>:
/// it calls <see cref="IRemoteSnapshotTransport.GetMetadataAsync"/>
/// once to capture the sender-side cut-point, then drains
/// <see cref="IRemoteSnapshotTransport.RequestSnapshotAsync"/> and
/// yields each entry through the existing
/// <see cref="SnapshotStream"/> shape so the receiver-side bootstrap
/// state machine sees no behavioural change from the local-tree default
/// other than the entries actually arriving from a remote peer. The
/// two-arg overload is intentionally unsupported: the adapter cannot
/// address a sender peer without the sender cluster id, and the
/// bootstrap coordinator always has the value in hand on
/// <c>BootstrapCoordinatorState.SourceClusterId</c>, so calling the
/// two-arg overload signals an integration bug.
/// </para>
/// <para>
/// The adapter is transport-agnostic and stateless. Concurrent
/// invocation across distinct <c>(treeName, sourceClusterId)</c> pairs
/// is safe; concurrent invocation against the same pair is bounded by
/// the underlying transport's concurrency model and by the receiver-
/// side bootstrap coordinator which serialises drains per pair.
/// </para>
/// </summary>
public sealed class RemoteSnapshotProvider : ISnapshotProvider
{
    private readonly IRemoteSnapshotTransport _transport;
    private readonly ILogger<RemoteSnapshotProvider> _logger;

    /// <summary>
    /// Constructs a new <see cref="RemoteSnapshotProvider"/> bound to
    /// the supplied cross-cluster transport.
    /// </summary>
    /// <param name="transport">
    /// The wire-shaped transport that delivers snapshot metadata and
    /// entry streams from the sender cluster. Must be non-null.
    /// </param>
    /// <param name="logger">Typed logger. Must be non-null.</param>
    public RemoteSnapshotProvider(
        IRemoteSnapshotTransport transport,
        ILogger<RemoteSnapshotProvider> logger)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(logger);

        _transport = transport;
        _logger = logger;
    }

    /// <summary>
    /// Not supported on the receiver-side adapter. The bootstrap
    /// coordinator always invokes the three-arg overload with the
    /// sender cluster id from
    /// <c>BootstrapCoordinatorState.SourceClusterId</c>; any caller
    /// reaching this overload is bypassing the coordinator and has no
    /// way to recover the sender peer to address.
    /// </summary>
    /// <param name="treeName">Unused.</param>
    /// <param name="asOfHlc">Unused.</param>
    /// <param name="cancellationToken">Unused.</param>
    /// <exception cref="InvalidOperationException">
    /// Always thrown. Call the three-arg overload instead.
    /// </exception>
    public Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        throw new InvalidOperationException(
            $"{nameof(RemoteSnapshotProvider)} requires the sender cluster id; " +
            $"call the three-arg ExportAsync(treeName, sourceClusterId, asOfHlc, ct) " +
            "overload from the bootstrap coordinator.");
    }

    /// <inheritdoc />
    public async Task<SnapshotStream> ExportAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        // Capture the sender-side cut-point first. The transport
        // contract guarantees the metadata describes the same snapshot
        // a paired RequestSnapshotAsync call will stream, so pinning
        // AsOfHlc and the causal-stable frontier here means the receiver
        // can resume incremental replication from a non-empty frontier
        // regardless of how the entry stream interleaves on the wire.
        var metadata = await _transport
            .GetMetadataAsync(treeName, sourceClusterId, asOfHlc, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Captured remote snapshot metadata for tree '{TreeName}' from cluster '{SourceClusterId}' at AsOfHlc={AsOfHlc}.",
            treeName,
            sourceClusterId,
            metadata.AsOfHlc);

        var entries = DrainAsync(treeName, sourceClusterId, asOfHlc, cancellationToken);
        return new SnapshotStream(treeName, metadata.AsOfHlc, metadata.CausalStableFrontier, entries);
    }

    private async IAsyncEnumerable<SnapshotEntry> DrainAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock asOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var entry in _transport
            .RequestSnapshotAsync(treeName, sourceClusterId, asOfHlc, cancellationToken)
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            yield return entry;
        }

        _logger.LogDebug(
            "Drained remote snapshot for tree '{TreeName}' from cluster '{SourceClusterId}'.",
            treeName,
            sourceClusterId);
    }
}
