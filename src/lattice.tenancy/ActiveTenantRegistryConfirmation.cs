using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Confirms a single asserted active tenant against the tenant registry while the
/// compiled tenant-policy snapshot is not authoritative, using the same
/// <see cref="LatticeTenantPolicyEngine.ValidateActiveTenant(CompiledTenantPolicy, string, IReadOnlyCollection{string}, TenantId)"/>
/// rule the snapshot itself applies in the steady state.
/// </summary>
/// <remarks>
/// <see cref="TenantGateEnforcer"/> established this pattern (issue #4053 / PR
/// #4064) for the enforcement path, including cross-tenant grants and residency.
/// This helper is the single-tenant subset of that pattern - no crossing, no
/// grant, no residency - for the two read paths (<see cref="TenantObservabilityView"/>
/// and <see cref="TenantContextResolver"/>) that only ever need to know whether
/// the subject may act as the one asserted tenant (issue #4065). A registry
/// record is admitted only under the tenant id it was read for, exactly as
/// <c>TenantGateEnforcer.CompileConfirmed</c> does, so a misdirected or stale
/// read can never be mistaken for the asserted tenant's own record.
/// </remarks>
internal static class ActiveTenantRegistryConfirmation
{
    /// <summary>
    /// Confirms that <paramref name="subjectId"/> (with <paramref name="groupIds"/>)
    /// may act as <paramref name="activeTenant"/>, by reading the tenant's current
    /// registry record and re-running the active-tenant rule against it. Returns
    /// <paramref name="activeTenant"/> when confirmed, or <c>null</c> when the
    /// registry denies it, the record cannot be found, or the registry read
    /// fails - fail-closed in every case.
    /// </summary>
    public static async ValueTask<TenantId?> ConfirmAsync(
        ITenantRegistry registry,
        bool delegatedAccessEnabled,
        string subjectId,
        IReadOnlyCollection<string> groupIds,
        TenantId activeTenant,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        TenantRecord? record;
        try
        {
            record = await registry.GetAsync(activeTenant, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Could not confirm subject '{SubjectId}' acting as tenant '{ActiveTenant}' against the tenant registry while the compiled tenant-policy snapshot was not authoritative; the request was denied.",
                subjectId,
                activeTenant.Value);
            return null;
        }

        // Admitted only under the tenant id it was read for: a misdirected or
        // stale read can never be mistaken for the asserted tenant's own record.
        var confirmed = record is not null && record.Id.Equals(activeTenant)
            ? CompiledTenantPolicy.Compile([record], delegatedAccessEnabled)
            : CompiledTenantPolicy.Empty;

        var validation = LatticeTenantPolicyEngine.ValidateActiveTenant(confirmed, subjectId, groupIds, activeTenant);
        return validation.Allowed ? activeTenant : null;
    }
}
