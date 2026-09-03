using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Default <see cref="ILatticeReplicationControl"/> implementation. Registered
/// as a silo singleton by <c>AddLatticeReplicationApi</c>; it authorizes every
/// operation through the shared <see cref="ReplicationAccessAuthorizer"/>
/// fail-closed <i>before</i> delegating to the engine's
/// <see cref="ILatticeReplicationConfigAuthority"/> authoring seam, and maps the
/// engine's result records onto the transport-agnostic abstraction DTOs.
/// </summary>
/// <remarks>
/// Authorization is the single narrowest seam: enable / disable authorize the
/// target tree for the <see cref="LatticeOperation.Replication"/> capability, and
/// the engine is consulted only after the caller has been authorized.
/// <see cref="GetReplicationConfigAsync"/> is permission-scoped - it enumerates
/// the engine's whole-estate status map but includes only the trees the caller is
/// authorized to manage, so it never reveals a tree outside the caller's grant.
/// The engine's precondition / mode-change exceptions surface unchanged so a
/// transport can map them to the appropriate status.
/// </remarks>
internal sealed class LatticeReplicationControl : ILatticeReplicationControl
{
    private readonly ILatticeReplicationConfigAuthority _authority;
    private readonly ReplicationAccessAuthorizer _authorizer;
    private readonly ITenantContextResolver _tenantResolver;

    /// <summary>Initializes a new <see cref="LatticeReplicationControl"/>.</summary>
    /// <param name="authority">The engine config-authoring seam. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed replication authorization seam. Must not be <c>null</c>.</param>
    /// <param name="tenantResolver">
    /// The active-tenant context resolver used to scope a caller-supplied,
    /// tenant-local tree name before it is authorized and acted on. Must not be
    /// <c>null</c>. With no tenancy add-on registered the core no-op resolver
    /// returns the caller's name unchanged, so behaviour is unaffected.
    /// </param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeReplicationControl(
        ILatticeReplicationConfigAuthority authority,
        ReplicationAccessAuthorizer authorizer,
        ITenantContextResolver tenantResolver)
    {
        ArgumentNullException.ThrowIfNull(authority);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(tenantResolver);
        _authority = authority;
        _authorizer = authorizer;
        _tenantResolver = tenantResolver;
    }

    /// <inheritdoc />
    public async Task<ReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // Scope the caller-supplied, tenant-local name to the caller's tenant before
        // anything uses it, so the authorization check and the operation below act on
        // the SAME effective tree. A no-op when tenancy is off (the core null resolver
        // returns the bare name unchanged).
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeAsync(treeId, cancellationToken).ConfigureAwait(false);
        var result = await _authority
            .EnableReplicationAsync(treeId, mode, bootstrapSourceClusterId, cancellationToken)
            .ConfigureAwait(false);
        return new ReplicationEnableResult(
            result.TreeId,
            result.Mode,
            result.AlreadyEnabled,
            result.BootstrapRequested);
    }

    /// <inheritdoc />
    public async Task<ReplicationDisableResult> DisableReplicationAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // Scope the caller-supplied, tenant-local name to the caller's tenant before
        // anything uses it, so the authorization check and the operation below act on
        // the SAME effective tree. A no-op when tenancy is off (the core null resolver
        // returns the bare name unchanged).
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeAsync(treeId, cancellationToken).ConfigureAwait(false);
        var result = await _authority
            .DisableReplicationAsync(treeId, cancellationToken)
            .ConfigureAwait(false);
        return new ReplicationDisableResult(result.TreeId, result.AlreadyDisabled);
    }

    /// <inheritdoc />
    public async Task<ReplicationConfigReport> GetReplicationConfigAsync(
        CancellationToken cancellationToken = default)
    {
        var statuses = await _authority
            .GetAllTreeStatusesAsync(cancellationToken)
            .ConfigureAwait(false);
        if (statuses.Count == 0)
        {
            return ReplicationConfigReport.Empty;
        }

        var entries = new List<ReplicationTreeConfigEntry>(statuses.Count);
        foreach (var status in statuses.Values)
        {
            if (!await _authorizer.IsAuthorizedAsync(status.TreeId, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            entries.Add(new ReplicationTreeConfigEntry(
                status.TreeId,
                status.Enabled,
                status.Mode,
                status.Ambiguous)
            {
                Source = ToApi(status.Source),
            });
        }

        return new ReplicationConfigReport(entries);
    }

    /// <summary>
    /// Maps the engine's enrollment-source discriminator onto the
    /// transport-agnostic abstraction enum. The two are declared separately so
    /// the abstraction package carries no dependency on the engine, and the
    /// mapping is exhaustive with a fail-safe default of
    /// <see cref="ReplicationEnrollmentSource.Runtime"/> - the value the report
    /// has always implied.
    /// </summary>
    /// <param name="source">The engine-side enrollment source.</param>
    /// <returns>The abstraction-side enrollment source.</returns>
    private static ReplicationEnrollmentSource ToApi(LatticeReplicationEnrollmentSource source)
        => source switch
        {
            LatticeReplicationEnrollmentSource.Static => ReplicationEnrollmentSource.Static,
            LatticeReplicationEnrollmentSource.RuntimeAndStatic => ReplicationEnrollmentSource.RuntimeAndStatic,
            _ => ReplicationEnrollmentSource.Runtime,
        };
}
