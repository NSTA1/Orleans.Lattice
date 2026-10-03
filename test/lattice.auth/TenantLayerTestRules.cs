namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Shared builders for the tenant-layer test fixtures: rule constructors for both
/// layers and a direct evaluator over a compiled snapshot.
/// </summary>
internal static class TenantLayerTestRules
{
    /// <summary>The tenant every fixture uses.</summary>
    public static readonly TenantId Contoso = TenantId.Parse("contoso");

    /// <summary>A second tenant, for cross-tenant cases.</summary>
    public static readonly TenantId Fabrikam = TenantId.Parse("fabrikam");

    /// <summary>A tenant-layer tree of <see cref="Contoso"/>.</summary>
    public const string TenantTree = "t/contoso/orders";

    /// <summary>The tenant group of <see cref="Contoso"/> the fixtures use.</summary>
    public const string ContosoReaders = "t/contoso/readers";

    public static LatticeAuthorizationRule Operator(
        string id, LatticeSubjectSelector subject, LatticeScope scope, LatticeOperation ops, LatticeEffect effect) =>
        new(id, subject, scope, ops, effect);

    public static LatticeAuthorizationRule Tenant(
        TenantId tenant, string localId, LatticeSubjectSelector subject, LatticeScope scope, LatticeOperation ops, LatticeEffect effect) =>
        new(LatticeTenantRuleIds.For(tenant, localId), subject, scope, ops, effect);

    public static LatticeAuthorizationRule Tenant(
        string localId, LatticeSubjectSelector subject, LatticeScope scope, LatticeOperation ops, LatticeEffect effect) =>
        Tenant(Contoso, localId, subject, scope, ops, effect);

    public static LatticeSubject Subject(string id, params string[] groups) =>
        new(id, groups.Length == 0 ? null : groups);

    public static LatticeAccessDecision Evaluate(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        out PolicyMatch match,
        LatticeAuthOptions? options = null,
        bool active = true)
    {
        var policy = CompiledPolicy.Compile(rules, includeTenantLayer: active);
        return PolicyEvaluator.Evaluate(
            policy, options ?? new LatticeAuthOptions(), subject, treeId, operation, key, null, null, active, out match);
    }

    public static LatticeAccessDecision Evaluate(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        LatticeAuthOptions? options = null,
        bool active = true) =>
        Evaluate(rules, subject, treeId, operation, key, out _, options, active);
}
