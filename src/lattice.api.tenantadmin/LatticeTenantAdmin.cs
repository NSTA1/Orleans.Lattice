using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of the transport-agnostic
/// <see cref="ILatticeTenantAdmin"/> tenant-administration control facade. It is
/// the single narrowest seam at which every tenant lifecycle operation is
/// authorized (fail-closed) and applied to the tenancy engine's
/// <see cref="ITenantRegistry"/>; every transport binding (gRPC, MCP) is a thin
/// adapter over this one surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed authorization.</b> Every mutating operation authorizes the
/// caller through <see cref="TenantAdminAccessAuthorizer"/> (the cluster-wide
/// <see cref="LatticeOperation.Admin"/> gate) <em>before</em> it reads or writes
/// the registry, so an unauthenticated or unauthorized caller is refused without
/// even learning whether a tenant exists.
/// </para>
/// <para>
/// <b>Last-writer-wins stamping.</b> The registry's status register keeps its
/// incumbent stamp internal, so the facade cannot read-then-supersede; instead it
/// stamps every write with a strictly increasing clock from
/// <see cref="ITenantAdminClock"/> and the cluster's writer id, which guarantees
/// each successive control-plane write supersedes the last.
/// </para>
/// </remarks>
internal sealed class LatticeTenantAdmin : ILatticeTenantAdmin
{
    private readonly ITenantRegistry _registry;
    private readonly TenantAdminAccessAuthorizer _authorizer;
    private readonly ITenantAdminClock _clock;
    private readonly ITenantTreeCascade _cascade;
    private readonly string? _writerId;

    /// <summary>
    /// Initializes a new <see cref="LatticeTenantAdmin"/>.
    /// </summary>
    /// <param name="registry">The tenancy engine's lifecycle store. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed tenant-admin authorization seam. Must not be <c>null</c>.</param>
    /// <param name="clock">The monotonic clock supplying last-writer-wins stamps. Must not be <c>null</c>.</param>
    /// <param name="cascade">The tenant-tree cascade seam used by delete. Must not be <c>null</c>.</param>
    /// <param name="clusterOptions">The cluster options supplying the writer id stamped on registry writes. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeTenantAdmin(
        ITenantRegistry registry,
        TenantAdminAccessAuthorizer authorizer,
        ITenantAdminClock clock,
        ITenantTreeCascade cascade,
        IOptions<ClusterOptions> clusterOptions)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(cascade);
        ArgumentNullException.ThrowIfNull(clusterOptions);

        _registry = registry;
        _authorizer = authorizer;
        _clock = clock;
        _cascade = cascade;
        _writerId = clusterOptions.Value.ClusterId;
    }

    /// <inheritdoc />
    public async Task<TenantCreationResult> CreateTenantAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        await _authorizer.AuthorizeTenantAdminAsync(cancellationToken).ConfigureAwait(false);

        if (await _registry.ExistsAsync(tenant, cancellationToken).ConfigureAwait(false))
        {
            throw new TenantAlreadyExistsException(tenant.Value);
        }

        var record = TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            _clock.Next(),
            _writerId);
        await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        return new TenantCreationResult
        {
            TenantId = tenant.Value,
            Status = TenantLifecycleStatus.Active,
        };
    }

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> SuspendTenantAsync(
        string tenantId, CancellationToken cancellationToken = default) =>
        TransitionAsync(tenantId, TenantStatus.Suspended, "suspend", cancellationToken);

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> ResumeTenantAsync(
        string tenantId, CancellationToken cancellationToken = default) =>
        TransitionAsync(tenantId, TenantStatus.Active, "resume", cancellationToken);

    /// <inheritdoc />
    public async Task<TenantDeletionResult> DeleteTenantAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        await _authorizer.AuthorizeTenantAdminAsync(cancellationToken).ConfigureAwait(false);

        // The reserved default tenant names the cluster's own legacy state and can
        // never be deleted. The reserved id is a constant, so rejecting it before
        // any store read leaks nothing about registry contents.
        if (tenant.IsDefault)
        {
            throw new ReservedTenantOperationException(tenant.Value, "delete");
        }

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);

        // Fail-closed cascade ordering. Suspend the tenant first, before any tree
        // is enumerated or removed, so the tenancy engine refuses every new
        // tenant-scoped admission for the duration of the delete. Without this a
        // create racing this delete could admit a fresh tree after the cascade
        // has enumerated the tenant's trees but before the registry record is
        // removed, orphaning that tree under a definition that no longer exists.
        // The status write lands through the registry's last-writer-wins merge, so
        // it composes with concurrent registry writes and a re-run of an already
        // interrupted delete is a stamp-advancing no-op.
        record.SetStatus(TenantStatus.Suspended, _clock.Next(), _writerId);
        await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        // Cascade the delete to the tenant's trees before removing the definition,
        // so an interrupted delete leaves the registry record (a retriable state)
        // rather than orphaning trees whose owning tenant is already gone.
        var cascaded = await _cascade
            .DeleteTenantTreesAsync(tenant, cancellationToken)
            .ConfigureAwait(false);
        await _registry.DeleteAsync(tenant, cancellationToken).ConfigureAwait(false);

        return new TenantDeletionResult
        {
            TenantId = tenant.Value,
            CascadedTreeCount = cascaded,
        };
    }

    /// <inheritdoc />
    public async Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        await _authorizer.AuthorizeTenantAdminAsync(cancellationToken).ConfigureAwait(false);

        // The reserved default tenant names the cluster's own legacy state and is
        // permanently unbounded; it can never be given quotas. The reserved id is a
        // constant, so rejecting it before any store read leaks nothing about
        // registry contents.
        if (tenant.IsDefault)
        {
            throw new ReservedTenantOperationException(tenant.Value, "set-quotas");
        }

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);

        // SetQuotas validates the burst percent (fail-closed on a negative value)
        // and stamps the write through the registry's last-writer-wins merge, so a
        // re-run of an interrupted author is a stamp-advancing idempotent write.
        var applied = TenantQuotasMapping.ToQuotas(quotas);
        record.SetQuotas(applied, _clock.Next(), _writerId);
        await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        return new TenantQuotasUpdateResult
        {
            TenantId = tenant.Value,
            Quotas = TenantQuotasMapping.ToDescriptor(record.Quotas),
        };
    }

    private async Task<TenantStatusChangeResult> TransitionAsync(
        string tenantId, TenantStatus target, string operation, CancellationToken cancellationToken)
    {
        var tenant = ParseTenant(tenantId);
        await _authorizer.AuthorizeTenantAdminAsync(cancellationToken).ConfigureAwait(false);

        // The reserved default tenant can never be suspended (and, symmetrically,
        // a resume is meaningless on a tenant that can never be suspended).
        if (tenant.IsDefault && target == TenantStatus.Suspended)
        {
            throw new ReservedTenantOperationException(tenant.Value, operation);
        }

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);

        var previous = record.Status;
        if (previous == target)
        {
            // Idempotent no-op: the tenant is already in the requested status.
            return new TenantStatusChangeResult
            {
                TenantId = tenant.Value,
                PreviousStatus = Map(previous),
                NewStatus = Map(target),
                Changed = false,
            };
        }

        record.SetStatus(target, _clock.Next(), _writerId);
        await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        return new TenantStatusChangeResult
        {
            TenantId = tenant.Value,
            PreviousStatus = Map(previous),
            NewStatus = Map(target),
            Changed = true,
        };
    }

    private static TenantId ParseTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (!TenantId.TryParse(tenantId, out var tenant))
        {
            throw new ArgumentException(
                $"'{tenantId}' is not a valid tenant id.", nameof(tenantId));
        }

        return tenant;
    }

    private static TenantLifecycleStatus Map(TenantStatus status) => status switch
    {
        TenantStatus.Active => TenantLifecycleStatus.Active,
        TenantStatus.Suspended => TenantLifecycleStatus.Suspended,
        _ => TenantLifecycleStatus.Active,
    };
}
