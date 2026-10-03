namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The one place tenancy's own consumers (the tenant gate, the tenant context
/// resolver and the tenant observability view) ask an <see cref="ITenantPolicyEngine"/>
/// whether a resolved subject may act as a tenant, so all three pass the subject's
/// groups the same way.
/// </summary>
internal static class TenantPolicyEngineSubjectExtensions
{
    /// <summary>
    /// Validates that the subject may act as <paramref name="activeTenant"/>. A
    /// subject that carries no groups takes the exact-id overload, which for the
    /// default engine is the same rule with an empty group set and is exactly the
    /// call every consumer made before groups existed; a subject with groups takes the
    /// group-aware overload, which itself reduces to the exact-id admin check while
    /// delegated tenant access administration is disabled.
    /// </summary>
    /// <param name="engine">The engine to ask.</param>
    /// <param name="subjectId">The subject id. Must not be <c>null</c>.</param>
    /// <param name="groupIds">The subject's resolved transitive group ids, or <c>null</c> for none.</param>
    /// <param name="activeTenant">The asserted active tenant.</param>
    /// <returns>The engine's decision.</returns>
    public static TenantAccessDecision ValidateActiveTenantAs(
        this ITenantPolicyEngine engine,
        string subjectId,
        IReadOnlyCollection<string>? groupIds,
        TenantId activeTenant) =>
        groupIds is null || groupIds.Count == 0
            ? engine.ValidateActiveTenant(subjectId, activeTenant)
            : engine.ValidateActiveTenant(subjectId, groupIds, activeTenant);
}
