using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;
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
/// <para>
/// Both entry points are gated by the sender-side enrollment check
/// described on <see cref="EnsureTreeEnrolledForExport"/>: the
/// peer-supplied <c>treeName</c> is re-resolved against this cluster's own
/// replication enrollment before any tree is read, so a peer holding the
/// mesh secret cannot dump a tree this cluster deliberately kept
/// cluster-local by not enrolling it.
/// </para>
/// </summary>
public sealed class LatticeRemoteSnapshotService : IRemoteSnapshotTransport
{
    private readonly ISnapshotProvider _provider;
    private readonly ILatticeReplicationContext? _replicationContext;
    private readonly ILogger<LatticeRemoteSnapshotService> _logger;

    /// <summary>
    /// Constructs a new <see cref="LatticeRemoteSnapshotService"/>
    /// bound to the supplied sender-side <see cref="ISnapshotProvider"/>
    /// with <b>no replication enrollment source</b>. The sender-side export
    /// gate cannot be evaluated without one, so - per the fail-closed
    /// principle - a service built through this overload refuses every
    /// export (see <see cref="EnsureTreeEnrolledForExport"/>). Prefer the
    /// <see cref="LatticeRemoteSnapshotService(ISnapshotProvider, ILatticeReplicationContext, ILogger{LatticeRemoteSnapshotService})"/>
    /// overload; the container-registered instance always resolves it,
    /// because <c>AddLatticeReplication</c> registers an
    /// <see cref="ILatticeReplicationContext"/>.
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
        _replicationContext = null;
        _logger = logger;
    }

    /// <summary>
    /// Constructs a new <see cref="LatticeRemoteSnapshotService"/> bound to
    /// the supplied sender-side <see cref="ISnapshotProvider"/> and the local
    /// replication enrollment context that scopes what it will export. This
    /// is the overload the DI container selects, because
    /// <c>AddLatticeReplication</c> registers an
    /// <see cref="ILatticeReplicationContext"/>.
    /// </summary>
    /// <param name="provider">
    /// The sender's local snapshot provider - typically the default
    /// <c>LatticeSnapshotProvider</c> registered by
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>,
    /// or a host-registered replacement.
    /// </param>
    /// <param name="replicationContext">
    /// The local replication enrollment context. Its
    /// <see cref="ILatticeReplicationContext.ResolveMergeMode"/> is the same
    /// per-tree resolver the shipper, change feed, and receiver-side
    /// <c>ReplicationApplier</c> consult, so the sender-side export gate
    /// agrees exactly with the receiver-side apply gate.
    /// </param>
    /// <param name="logger">Typed logger.</param>
    public LatticeRemoteSnapshotService(
        ISnapshotProvider provider,
        ILatticeReplicationContext replicationContext,
        ILogger<LatticeRemoteSnapshotService> logger)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(replicationContext);
        ArgumentNullException.ThrowIfNull(logger);

        _provider = provider;
        _replicationContext = replicationContext;
        _logger = logger;
    }

    /// <summary>
    /// Sender-side enrollment gate for the cross-cluster bootstrap export
    /// path. <paramref name="treeName"/> arrives from the requesting peer
    /// and is therefore an assertion to check, never a fact to act on, so it
    /// is re-resolved against this cluster's own replication enrollment
    /// before any tree is read - the mirror image of the receiver-side
    /// classification <c>ReplicationApplier.ClassifyInboundTree</c> already
    /// performs on inbound entries.
    /// <para>
    /// The mesh secret authenticates "some peer in the mesh", not "this
    /// peer, for this tree", so without this gate any peer clearing the
    /// transport interceptor could stream out an arbitrary tree - including
    /// the <c>sys-</c>-prefixed authorization and identity trees, and tenant
    /// trees, that a cluster deliberately keeps local by never enrolling
    /// them for replication.
    /// </para>
    /// <para>
    /// The gate fails closed on ambiguity: a service constructed without an
    /// <see cref="ILatticeReplicationContext"/> has no enrollment signal at
    /// all, so it cannot evaluate the gate and refuses rather than exports.
    /// Placing the check on this handler - rather than on any one transport
    /// binding - means the gRPC binding, the in-process loopback, and any
    /// custom binding are all covered by the single rejection.
    /// </para>
    /// </summary>
    /// <param name="treeName">The peer-supplied logical tree name to export.</param>
    /// <exception cref="UnauthorizedAccessException">
    /// The tree is not enrolled for replication on this cluster, or no
    /// enrollment source is available to decide.
    /// </exception>
    private void EnsureTreeEnrolledForExport(string treeName)
    {
        if (_replicationContext is null)
        {
            _logger.LogWarning(
                "Refusing snapshot export of tree '{TreeName}': this LatticeRemoteSnapshotService was "
                + "constructed without an ILatticeReplicationContext, so the sender-side enrollment gate "
                + "cannot be evaluated and fails closed.",
                treeName);

            throw new UnauthorizedAccessException(
                $"Snapshot export of tree '{treeName}' was refused: no replication enrollment source is "
                + "wired on this cluster, so the sender-side enrollment gate cannot be evaluated.");
        }

        if (_replicationContext.ResolveMergeMode(treeName) is null)
        {
            _logger.LogWarning(
                "Refusing snapshot export of tree '{TreeName}': the tree is not enrolled for replication "
                + "on this cluster.",
                treeName);

            throw new UnauthorizedAccessException(
                $"Snapshot export of tree '{treeName}' was refused: the tree is not enrolled for "
                + "replication on this cluster.");
        }
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
        EnsureTreeEnrolledForExport(treeName);

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
        EnsureTreeEnrolledForExport(treeName);

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
