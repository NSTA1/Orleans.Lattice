using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The durable admin audit trail (#3638): the <see cref="ILatticeAdminAuditTrail"/>
/// registered by <c>AddLatticeAuth</c>. Writes to the reserved
/// <see cref="AuthConstants.AdminAuditTree"/> tree under system origin, so the write
/// bypasses the gate it audits and cannot audit itself.
/// </summary>
/// <remarks>
/// <para>
/// <b>Actor.</b> The actor is the subject the caller context resolves to, the same
/// resolution the access gate authorizes, never an id taken from the request.
/// </para>
/// <para>
/// <b>Exactly-once finalize.</b> An intent is written at <c>pending/{operationId}</c>.
/// Its final record goes to a key derived from the intent alone, written
/// create-only (<see cref="ILattice.SetIfVersionAsync"/> against
/// <see cref="HybridLogicalClock.Zero"/>), then the intent is deleted. Whichever of the
/// caller and the reconciler finalizes first wins; the other's create is refused and
/// it only removes the intent.
/// </para>
/// </remarks>
internal sealed class DurableLatticeAdminAuditTrail : ILatticeAdminAuditTrail
{
    private readonly IGrainFactory _grainFactory;
    private readonly IOptionsMonitor<LatticeAuthOptions> _options;
    private readonly ILatticeReplicationContext _replication;
    private readonly ILatticeMembershipContext? _membership;
    private readonly ILogger<DurableLatticeAdminAuditTrail> _logger;
    private readonly TimeProvider _time;

    /// <summary>Creates the trail.</summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="options">The authorization options.</param>
    /// <param name="replication">The replication context, for the authoring region.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="membership">The caller context the actor is resolved from.</param>
    /// <param name="timeProvider">The clock; defaults to <see cref="TimeProvider.System"/>.</param>
    public DurableLatticeAdminAuditTrail(
        IGrainFactory grainFactory,
        IOptionsMonitor<LatticeAuthOptions> options,
        ILatticeReplicationContext replication,
        ILogger<DurableLatticeAdminAuditTrail> logger,
        ILatticeMembershipContext? membership = null,
        TimeProvider? timeProvider = null)
    {
        _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _replication = replication ?? throw new ArgumentNullException(nameof(replication));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _membership = membership;
        _time = timeProvider ?? TimeProvider.System;

        // Zero-primed so a flat zero means "no failed write" rather than "never measured".
        LatticeAuthMetrics.AdminAuditWriteFailures.Add(0, LatticeTenantLabel.Platform);
    }

    /// <inheritdoc />
    public bool IsEnabled => _options.CurrentValue.EnableAdminAuditTrail;

    /// <inheritdoc />
    public async ValueTask RecordDeniedAsync(LatticeAdminAuditRequest request, string reasonCode, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(reasonCode);
        try
        {
            var now = _time.GetUtcNow();
            var record = await BuildAsync(request, cancellationToken).ConfigureAwait(false) with
            {
                Outcome = LatticeAdminAuditOutcome.Denied,
                ReasonCode = reasonCode,
                CompletedUtc = now,
            };

            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                await AdminAuditStore.Tree(_grainFactory)
                    .SetAsync(AdminAuditKeys.Final(record), AdminAuditStore.Serialize(record), cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // The refusal is returned regardless; a lost denial record is counted.
            LatticeAuthMetrics.AdminAuditWriteFailures.Add(1, LatticeTenantLabel.Platform);
            _logger.LogWarning(ex, "The admin audit trail could not record a denied {Action} call.", request.Action);
        }
    }

    /// <inheritdoc />
    public async ValueTask<LatticeAdminAuditRecord> BeginAsync(LatticeAdminAuditRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        try
        {
            var record = await BuildAsync(request, cancellationToken).ConfigureAwait(false);
            if (request.Anchor is { } anchor)
            {
                var observed = await AdminAuditAnchorReader.ReadAsync(_grainFactory, anchor, cancellationToken).ConfigureAwait(false);
                record = record with
                {
                    AnchorTreeId = anchor.TreeId,
                    AnchorKey = anchor.Key,
                    AnchorBaseVersion = observed.Version,
                    AnchorBaseHash = observed.Hash,
                };
            }

            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                await AdminAuditStore.Tree(_grainFactory)
                    .SetAsync(AdminAuditKeys.Pending(record.OperationId), AdminAuditStore.Serialize(record), cancellationToken)
                    .ConfigureAwait(false);
            }

            return record;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LatticeAuthMetrics.AdminAuditWriteFailures.Add(1, LatticeTenantLabel.Platform);
            _logger.LogWarning(ex, "The admin audit trail could not record the intent of a {Action} call; the call was refused.", request.Action);
            throw new LatticeAdminAuditUnavailableException(
                "The admin audit trail could not record this call, so the call was refused before any change was made.",
                ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask CompleteAsync(LatticeAdminAuditRecord intent, LatticeAdminAuditCompletion completion)
    {
        ArgumentNullException.ThrowIfNull(intent);
        if (completion.Outcome == LatticeAdminAuditOutcome.Pending)
        {
            // The facade cannot tell whether the commit landed: the reconciler decides.
            return;
        }

        try
        {
            var final = intent with
            {
                Outcome = completion.Outcome,
                ReasonCode = completion.ReasonCode,
                CompletedUtc = _time.GetUtcNow(),
            };

            if (completion.Outcome == LatticeAdminAuditOutcome.Committed
                && intent.AnchorTreeId is { } treeId
                && intent.AnchorKey is { } key)
            {
                final = await PointAtRevisionAsync(final, new LatticeAdminAuditAnchor(treeId, key)).ConfigureAwait(false);
            }

            await AdminAuditStore.WriteFinalAsync(_grainFactory, final, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LatticeAuthMetrics.AdminAuditWriteFailures.Add(1, LatticeTenantLabel.Platform);
            _logger.LogWarning(
                ex,
                "The admin audit trail could not finalize operation {OperationId}; the reconciler will resolve it.",
                intent.OperationId);
        }
    }

    /// <summary>
    /// Points a committed record at the anchor revision the call left: the anchor's
    /// current version (when it moved past the base) and value hash. When the anchor's
    /// version has not moved at all (or it was absent before and is absent after) the
    /// call changed nothing, and the record says so
    /// (<see cref="LatticeAdminAuditReasonCodes.FailedNoChange"/>) instead of claiming a
    /// commit. A failed read leaves the record committed without the pointer.
    /// </summary>
    private async Task<LatticeAdminAuditRecord> PointAtRevisionAsync(LatticeAdminAuditRecord final, LatticeAdminAuditAnchor anchor)
    {
        try
        {
            var observed = await AdminAuditAnchorReader.ReadAsync(_grainFactory, anchor, CancellationToken.None).ConfigureAwait(false);

            // The anchor's version has not moved since before the commit, so the call
            // wrote nothing to it: a facade that returned normally made no change.
            // On a versioned tree an anchor absent before and absent after was not
            // written either: a removal that matched nothing.
            var unmoved = final.AnchorBaseVersion is { } baseVersion
                ? observed.Version is { } unchanged && unchanged.Equals(baseVersion)
                : observed.Version is null;
            if (observed.Versioned && unmoved)
            {
                return final with
                {
                    Outcome = LatticeAdminAuditCompletion.NoChange.Outcome,
                    ReasonCode = LatticeAdminAuditCompletion.NoChange.ReasonCode,
                };
            }

            return final with
            {
                AnchorValueHash = observed.Hash,
                CommittedRevisionHlc = observed.Version is { } version && !Equals(version, final.AnchorBaseVersion)
                    ? version
                    : null,
            };
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "The admin audit trail could not read the anchor of operation {OperationId}.", final.OperationId);
            return final;
        }
    }

    private async ValueTask<LatticeAdminAuditRecord> BuildAsync(LatticeAdminAuditRequest request, CancellationToken cancellationToken)
    {
        var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(_membership, cancellationToken).ConfigureAwait(false);
        var bootstrap = _options.CurrentValue.BootstrapAdministrators;
        return new LatticeAdminAuditRecord
        {
            OperationId = Guid.NewGuid().ToString("N"),
            Outcome = LatticeAdminAuditOutcome.Pending,
            Action = request.Action,
            ActorSubjectId = subject.SubjectId,
            ActorTenant = AdminAuditBounds.Bound(request.ActorTenant),
            ActorKind = bootstrap is not null && bootstrap.Contains(subject.SubjectId)
                ? LatticeAdminAuditActorKind.BootstrapAdministrator
                : LatticeAdminAuditActorKind.Subject,
            TargetScope = request.TargetScope,
            TargetKind = request.TargetKind,
            TargetId = AdminAuditBounds.Bound(request.TargetId),
            Detail = AdminAuditBounds.Bound(request.Detail),
            OriginRegion = _replication.LocalReplicaId ?? string.Empty,
            TimestampUtc = _time.GetUtcNow(),
        };
    }
}
