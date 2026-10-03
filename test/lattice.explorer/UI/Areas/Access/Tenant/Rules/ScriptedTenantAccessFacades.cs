using Orleans.Lattice;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Fakes;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The tenant facade seam over the contract fakes with one twist: a tenant policy
/// call can be scripted to fail by name, every time it is made, so a test reaches
/// a page's error state or a write's refusal without disturbing the posture probe
/// that opens the page.
/// </summary>
/// <param name="facades">The fakes the calls are passed to.</param>
internal sealed class ScriptedTenantAccessFacades(FakeTenantAccessFacades facades) : ITenantAccessFacades, ILatticeTenantPolicyAdmin
{
    /// <summary>The failure each named policy operation throws, if any.</summary>
    public Dictionary<string, Exception> Faults { get; } = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public ILatticeTenantDirectoryAdmin? Directory => facades.Directory;

    /// <inheritdoc />
    public ILatticeTenantPolicyAdmin? Policy => facades.Policy is null ? null : this;

    private FakeTenantPolicyAdmin Fake => facades.PolicyFake;

    /// <inheritdoc />
    public Task<TenantRuleView> PutRuleAsync(string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default) =>
        Fault(nameof(PutRuleAsync)) ?? Fake.PutRuleAsync(tenantId, rule, cancellationToken);

    /// <inheritdoc />
    public Task<TenantRuleView?> GetRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default) =>
        Fault<TenantRuleView?>(nameof(GetRuleAsync)) ?? Fake.GetRuleAsync(tenantId, ruleId, cancellationToken);

    /// <inheritdoc />
    public Task<bool> RemoveRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default) =>
        Fault<bool>(nameof(RemoveRuleAsync)) ?? Fake.RemoveRuleAsync(tenantId, ruleId, cancellationToken);

    /// <inheritdoc />
    public Task<TenantRulePage> ListRulesAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default) =>
        Fault<TenantRulePage>(nameof(ListRulesAsync)) ?? Fake.ListRulesAsync(tenantId, page, cancellationToken);

    /// <inheritdoc />
    public Task<TenantExplanation> ExplainAsync(
        string tenantId,
        string subjectId,
        string treeName,
        string? key,
        LatticeOperation operation,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default) =>
        Fault<TenantExplanation>(nameof(ExplainAsync)) ?? Fake.ExplainAsync(tenantId, subjectId, treeName, key, operation, subjectKind, cancellationToken);

    /// <inheritdoc />
    public Task<TenantEffectivePermissions> EffectivePermissionsAsync(
        string tenantId,
        string subjectId,
        string? treeName = null,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default) =>
        Fault<TenantEffectivePermissions>(nameof(EffectivePermissionsAsync)) ?? Fake.EffectivePermissionsAsync(tenantId, subjectId, treeName, subjectKind, cancellationToken);

    /// <inheritdoc />
    public Task<TenantAccessPosture> GetPostureAsync(string tenantId, CancellationToken cancellationToken = default) =>
        Fault<TenantAccessPosture>(nameof(GetPostureAsync)) ?? Fake.GetPostureAsync(tenantId, cancellationToken);

    private Task<TenantRuleView>? Fault(string operation) => Fault<TenantRuleView>(operation);

    private Task<T>? Fault<T>(string operation) =>
        Faults.TryGetValue(operation, out var fault) ? Task.FromException<T>(fault) : null;
}
