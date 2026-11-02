using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
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
/// <see cref="ISnapshotProvider"/>: <see cref="GetMetadataAsync(string, string, HybridLogicalClock, CancellationToken)"/>
/// invokes <see cref="Orleans.Lattice.Replication.ISnapshotProvider.ExportAsync(string, Orleans.Lattice.HybridLogicalClock, System.Threading.CancellationToken)"/> and returns the
/// resulting cut-point metadata; a paired
/// <see cref="RequestSnapshotAsync(string, string, HybridLogicalClock, CancellationToken)"/> call invokes
/// <see cref="Orleans.Lattice.Replication.ISnapshotProvider.ExportAsync(string, Orleans.Lattice.HybridLogicalClock, System.Threading.CancellationToken)"/> again with the same
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
/// Tenant trees additionally require an authenticated requesting peer and the
/// tenant export gate's approval before any provider access. The legacy
/// transport signatures have no requester identity and cannot export tenant trees.
/// </para>
/// </summary>
public sealed class LatticeRemoteSnapshotService : IRemoteSnapshotItemTransport
{
    private readonly ISnapshotProvider _provider;
    private readonly ILatticeReplicationContext? _replicationContext;
    private readonly ILogger<LatticeRemoteSnapshotService> _logger;
    private readonly IReplicationTenantIsolationGate? _tenantIsolationGate;

    /// <summary>
    /// The cross-tree export preconditions (issue #4684), set when the service
    /// is resolved from a host that registered replication. A service built
    /// directly has none and serves exports as before.
    /// </summary>
    internal CrossTreeExportGate? ExportGate { get; init; }

    /// <summary>
    /// The container factory: the overload the container would select, with
    /// the cross-tree export gate attached.
    /// </summary>
    internal static LatticeRemoteSnapshotService Create(IServiceProvider services)
    {
        var provider = services.GetRequiredService<ISnapshotProvider>();
        var logger = services.GetRequiredService<ILogger<LatticeRemoteSnapshotService>>();
        var context = services.GetService<ILatticeReplicationContext>();
        return context is null
            ? new LatticeRemoteSnapshotService(provider, logger)
            : new LatticeRemoteSnapshotService(provider, context, logger,
                services.GetService<IReplicationTenantIsolationGate>())
            {
                ExportGate = services.GetService<CrossTreeExportGate>(),
            };
    }

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
    /// has no tenant disclosure gate and refuses tenant exports. The registered
    /// factory supplies the gate through the four-argument constructor.
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
        : this(provider, replicationContext, logger, null)
    {
    }

    /// <summary>
    /// Constructs a sender with a tenant snapshot disclosure gate. Tenant
    /// exports require an authenticated requesting peer and an active gate;
    /// legacy and platform trees retain their enrollment-only export behavior.
    /// </summary>
    /// <param name="provider">The local snapshot provider.</param>
    /// <param name="replicationContext">The local enrollment context.</param>
    /// <param name="logger">Typed logger.</param>
    /// <param name="tenantIsolationGate">The tenant export authorizer, or <c>null</c> to deny tenant exports.</param>
    public LatticeRemoteSnapshotService(
        ISnapshotProvider provider,
        ILatticeReplicationContext replicationContext,
        ILogger<LatticeRemoteSnapshotService> logger,
        IReplicationTenantIsolationGate? tenantIsolationGate)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(replicationContext);
        ArgumentNullException.ThrowIfNull(logger);

        _provider = provider;
        _replicationContext = replicationContext;
        _logger = logger;
        _tenantIsolationGate = tenantIsolationGate;
    }

    private async ValueTask EnsurePeerMayExportAsync(
        string treeName, string? authenticatedRequesterClusterId, CancellationToken cancellationToken)
    {
        var owner = LatticeTenantTrees.GetOwner(treeName);
        if (owner.IsPlatformOwned || owner.Tenant.IsDefault)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(authenticatedRequesterClusterId)
            || _tenantIsolationGate is not { IsActive: true }
            || !await _tenantIsolationGate.AuthorizeExportAsync(
                treeName, authenticatedRequesterClusterId, cancellationToken).ConfigureAwait(false))
        {
            throw new UnauthorizedAccessException("Tenant snapshot export is not authorized for the requesting peer.");
        }
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
    public Task<RemoteSnapshotMetadata> GetMetadataAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default) =>
        GetMetadataAsync(treeName, sourceClusterId, fromAsOfHlc, null, cancellationToken);

    /// <summary>Captures snapshot metadata after authorizing the authenticated requesting peer.</summary>
    /// <param name="treeName">The tree to export.</param>
    /// <param name="sourceClusterId">The source cluster pinned by the receiver, not requester identity.</param>
    /// <param name="fromAsOfHlc">The requested cut-point filter.</param>
    /// <param name="authenticatedRequesterClusterId">The identity established by transport authentication; <c>null</c> when unavailable.</param>
    /// <param name="cancellationToken">Cancels the export.</param>
    /// <returns>The snapshot metadata.</returns>
    public async Task<RemoteSnapshotMetadata> GetMetadataAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        string? authenticatedRequesterClusterId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureTreeEnrolledForExport(treeName);
        await EnsurePeerMayExportAsync(treeName, authenticatedRequesterClusterId, cancellationToken).ConfigureAwait(false);
        ExportGate?.EnsureMayExport(treeName);

        var stream = await OpenExportAsync(treeName, fromAsOfHlc, cancellationToken).ConfigureAwait(false);

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
            ExportEpoch = stream.ExportEpoch,
            CrossTreeHoldHonoured = ExportGate is not null,
            OpenGeneration = stream.OpenGeneration,
            SourceFrontier = stream.OpenFrontier,
        };
    }

    /// <inheritdoc />
    public IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default) =>
        RequestSnapshotAsync(treeName, sourceClusterId, fromAsOfHlc, null, cancellationToken);

    /// <summary>Streams entries after authorizing the authenticated requesting peer, before opening the provider.</summary>
    /// <param name="treeName">The tree to export.</param>
    /// <param name="sourceClusterId">The source cluster pinned by the receiver.</param>
    /// <param name="fromAsOfHlc">The requested cut-point filter.</param>
    /// <param name="authenticatedRequesterClusterId">The transport-authenticated requesting peer, or <c>null</c> when unavailable.</param>
    /// <param name="cancellationToken">Cancels the export.</param>
    /// <returns>The authorized entry stream.</returns>
    public IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        string? authenticatedRequesterClusterId,
        CancellationToken cancellationToken) =>
        DeferOnRedirectAsync(
            treeName,
            RequestSnapshotCoreAsync(treeName, sourceClusterId, fromAsOfHlc, authenticatedRequesterClusterId, cancellationToken),
            cancellationToken);

    private async IAsyncEnumerable<SnapshotEntry> RequestSnapshotCoreAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        string? authenticatedRequesterClusterId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureTreeEnrolledForExport(treeName);
        await EnsurePeerMayExportAsync(treeName, authenticatedRequesterClusterId, cancellationToken).ConfigureAwait(false);
        ExportGate?.EnsureMayExport(treeName);

        var stream = await OpenExportAsync(treeName, fromAsOfHlc, cancellationToken).ConfigureAwait(false);

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

    /// <inheritdoc />
    public IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default) =>
        RequestSnapshotItemsAsync(treeName, sourceClusterId, fromAsOfHlc, null, cancellationToken);

    /// <summary>Streams entries and completion fences after authorizing the authenticated requesting peer.</summary>
    /// <param name="treeName">The tree to export.</param>
    /// <param name="sourceClusterId">The source cluster pinned by the receiver.</param>
    /// <param name="fromAsOfHlc">The requested cut-point filter.</param>
    /// <param name="authenticatedRequesterClusterId">The transport-authenticated requesting peer, or <c>null</c> when unavailable.</param>
    /// <param name="cancellationToken">Cancels the export.</param>
    /// <returns>The authorized snapshot stream.</returns>
    public IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        string? authenticatedRequesterClusterId,
        CancellationToken cancellationToken) =>
        DeferOnRedirectAsync(
            treeName,
            RequestSnapshotItemsCoreAsync(treeName, sourceClusterId, fromAsOfHlc, authenticatedRequesterClusterId, cancellationToken),
            cancellationToken);

    /// <summary>
    /// Re-raises a source redirect met while an export runs (issue #5537) as a
    /// transient <see cref="LatticeSnapshotExportDeferredException"/>. An export
    /// reads the one physical copy the tree resolved to when it opened, so once a
    /// resize or restore moves the alias and fences that copy, every later read
    /// throws <see cref="StaleTreeRoutingException"/>. Continuing on the new copy
    /// would mix two copies in one export, so the export ends and the receiver's
    /// bootstrap retries it; the fresh export resolves the current copy.
    /// </summary>
    private async IAsyncEnumerable<T> DeferOnRedirectAsync<T>(
        string treeName,
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var enumerator = source.GetAsyncEnumerator(cancellationToken);
        try
        {
            while (true)
            {
                bool hasNext;
                try
                {
                    hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (Exception ex) when (FindRedirect(ex) is { } redirect)
                {
                    throw Deferred(treeName, redirect, ex);
                }

                if (!hasNext)
                {
                    yield break;
                }

                yield return enumerator.Current;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task<SnapshotStream> OpenExportAsync(
        string treeName, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken)
    {
        try
        {
            return await _provider.ExportAsync(treeName, fromAsOfHlc, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (FindRedirect(ex) is { } redirect)
        {
            throw Deferred(treeName, redirect, ex);
        }
    }

    private LatticeSnapshotExportDeferredException Deferred(
        string treeName, StaleTreeRoutingException redirect, Exception observed)
    {
        _logger.LogWarning(observed,
            "Snapshot export of tree '{TreeName}' ended because physical copy '{StalePhysicalTreeId}' was redirected "
            + "to '{DestinationPhysicalTreeId}' during the export; the receiver retries with a fresh export.",
            treeName, redirect.StalePhysicalTreeId, redirect.DestinationPhysicalTreeId);
        return new LatticeSnapshotExportDeferredException(
            $"Tree '{treeName}' was redirected to another physical copy during the snapshot export; "
            + "retry so the export reads the current copy.");
    }

    internal static StaleTreeRoutingException? FindRedirect(Exception? exception)
    {
        for (var depth = 0; exception is not null && depth < 16; depth++)
        {
            switch (exception)
            {
                case StaleTreeRoutingException redirect:
                    return redirect;
                case AggregateException aggregate:
                    foreach (var inner in aggregate.Flatten().InnerExceptions)
                    {
                        if (FindRedirect(inner) is { } found)
                        {
                            return found;
                        }
                    }

                    return null;
            }

            exception = exception.InnerException;
        }

        return null;
    }

    private async IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsCoreAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        string? authenticatedRequesterClusterId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureTreeEnrolledForExport(treeName);
        await EnsurePeerMayExportAsync(treeName, authenticatedRequesterClusterId, cancellationToken).ConfigureAwait(false);
        ExportGate?.EnsureMayExport(treeName);

        var stream = await OpenExportAsync(treeName, fromAsOfHlc, cancellationToken).ConfigureAwait(false);

        HashSet<string>? participantTrees = stream.CrossTreeParticipantsComplete ? new(StringComparer.Ordinal) : null;
        await foreach (var entry in stream.Entries
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            if (participantTrees is not null && entry.CrossTreeOperationId is not null)
            {
                if (entry.CrossTreeParticipants.IsDefaultOrEmpty)
                {
                    // Missing participant metadata must not turn an
                    // unprovable export into a narrower safety boundary.
                    participantTrees = null;
                }
                else
                {
                    participantTrees.UnionWith(entry.CrossTreeParticipants);
                }
            }

            yield return new RemoteSnapshotStreamItem { Entry = entry };
        }

        // The canonical provider's completed entry stream includes participant
        // metadata on prepared rows and every decision row, including decisions
        // discovered by its completion pass. Capturing sibling tails now fences
        // every cross-tree operation represented by this snapshot. An operation
        // started after enumeration closes is outside the snapshot; its
        // incremental terminal still enters ApplyTxTerminalCoreAsync with its
        // participant set and is held by the receiver's cross-tree barrier.
        // Captured after the last entry, so every sibling record the receiver
        // must have before it serves this tree is below the boundary (#4684).
        var siblings = ExportGate is null || _replicationContext is null
            ? null
            : await ExportGate.CaptureSiblingBoundariesAsync(
                treeName, _replicationContext, participantTrees, cancellationToken).ConfigureAwait(false);
        // The exported tree's own tails, likewise after the last entry: every
        // prepare of a saga whose decision the export carried is below them, so
        // the receiver may forget the imported decisions once past them (#4524).
        var exportBoundary = ExportGate is null
            ? null
            : await ExportGate.CaptureExportBoundaryAsync(treeName, cancellationToken).ConfigureAwait(false);
        if (stream.CloseGeneration is not null || siblings is not null || exportBoundary is not null)
        {
            yield return new RemoteSnapshotStreamItem
            {
                CloseGeneration = stream.CloseGeneration,
                SourceFrontier = stream.SourceFrontier,
                SiblingBoundaries = siblings,
                ExportBoundary = exportBoundary,
            };
        }
    }
}
