using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Sender-side <see cref="IRemoteSnapshotTransport"/> handler. Registered
/// on a producer silo and invoked by the concrete transport binding
/// (gRPC, in-process loopback, custom HTTP) to translate inbound
/// metadata/stream RPCs into calls against the sender's local
/// <see cref="ISnapshotProvider"/>. The handler is independent of the
/// binding - the same instance is shared across all concrete bindings
/// the host registers - and is therefore the canonical sender-side
/// implementation of the cross-cluster bootstrap transport contract.
/// <para>
/// Point-in-time consistency is provided by the underlying
/// <see cref="ISnapshotProvider"/>: <see cref="GetMetadataAsync"/>
/// invokes <see cref="ISnapshotProvider.ExportAsync"/> and returns the
/// resulting cut-point metadata; a paired
/// <see cref="RequestSnapshotAsync"/> call invokes
/// <see cref="ISnapshotProvider.ExportAsync"/> again with the same
/// receiver-supplied <c>fromAsOfHlc</c> filter and drains the entry
/// stream. The canonical
/// <c>LatticeSnapshotProvider</c> reads the producer's causal-stable
/// frontier once and enumerates entries against the live tree, so
/// writes committed after the metadata cut-point are excluded from the
/// matching stream call by the per-entry HLC filter applied at
/// emission time. Hosts plugging a non-default
/// <see cref="ISnapshotProvider"/> must preserve the same semantics:
/// the snapshot is a point-in-time view at the metadata's
/// <see cref="RemoteSnapshotMetadata.AsOfHlc"/>, not a moving target.
/// </para>
/// <para>
/// The handler is stateless and safe for concurrent invocation across
/// distinct <c>(treeName, sourceClusterId)</c> pairs; concurrent
/// invocation against the same pair is bounded by the underlying
/// provider's concurrency model and by the receiver-side coordinator
/// which serialises bootstrap drains per pair.
/// </para>
/// </summary>
public sealed class LatticeRemoteSnapshotService : IRemoteSnapshotTransport
{
    private readonly ISnapshotProvider _provider;
    private readonly ILogger<LatticeRemoteSnapshotService> _logger;

    /// <summary>
    /// Constructs a new <see cref="LatticeRemoteSnapshotService"/>
    /// bound to the supplied sender-side <see cref="ISnapshotProvider"/>.
    /// </summary>
    /// <param name="provider">
    /// The sender's local snapshot provider - typically the default
    /// <c>LatticeSnapshotProvider</c> registered by
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>,
    /// or a host-registered replacement.
    /// </param>
    /// <param name="logger">Typed logger.</param>
    public LatticeRemoteSnapshotService(
        ISnapshotProvider provider,
        ILogger<LatticeRemoteSnapshotService> logger)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(logger);

        _provider = provider;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<RemoteSnapshotMetadata> GetMetadataAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var stream = await _provider
            .ExportAsync(treeName, fromAsOfHlc, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Captured snapshot metadata for tree '{TreeName}' from cluster '{SourceClusterId}' at AsOfHlc={AsOfHlc}.",
            treeName,
            sourceClusterId,
            stream.AsOfHlc);

        return new RemoteSnapshotMetadata
        {
            TreeName = treeName,
            SourceClusterId = sourceClusterId,
            AsOfHlc = stream.AsOfHlc,
            CausalStableFrontier = stream.CausalStableFrontier,
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var stream = await _provider
            .ExportAsync(treeName, fromAsOfHlc, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Streaming snapshot for tree '{TreeName}' to cluster '{SourceClusterId}' at AsOfHlc={AsOfHlc}.",
            treeName,
            sourceClusterId,
            stream.AsOfHlc);

        await foreach (var entry in stream.Entries
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            yield return entry;
        }
    }
}
