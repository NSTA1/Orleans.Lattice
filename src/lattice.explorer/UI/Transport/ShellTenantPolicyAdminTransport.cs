using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantPolicyAdmin"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeTenantAdminApiGrpcClient"/>'s tenant rule,
/// explain, effective-permission and posture RPCs. Every call carries the circuit's
/// sign-in and asserts its active tenant through <see cref="ShellTransportChannel"/>,
/// and faults map through <see cref="ShellTenantAccessFaults"/>, so a page sees the
/// facade's typed refusals.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantPolicyAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantAdminApiGrpcClient>(channel, LatticeTenantAdminApiGrpcClient.Create), ILatticeTenantPolicyAdmin
{
    /// <inheritdoc />
    public Task<TenantRuleView> PutRuleAsync(string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(rule);
        return CallAsync(
            (TenantId: tenantId, Rule: rule),
            static (client, state, ct) => client.PutRuleAsync(state.TenantId, state.Rule, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantRuleView?> GetRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        return CallAsync(
            (TenantId: tenantId, RuleId: ruleId),
            static (client, state, ct) => client.GetRuleAsync(state.TenantId, state.RuleId, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> RemoveRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        return CallAsync(
            (TenantId: tenantId, RuleId: ruleId),
            static (client, state, ct) => client.RemoveRuleAsync(state.TenantId, state.RuleId, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantRulePage> ListRulesAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(page);
        return CallAsync(
            (TenantId: tenantId, Page: page),
            static (client, state, ct) => client.ListRulesAsync(state.TenantId, state.Page, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantExplanation> ExplainAsync(
        string tenantId,
        string subjectId,
        string treeName,
        string? key,
        LatticeOperation operation,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        return CallAsync(
            (TenantId: tenantId, SubjectId: subjectId, TreeName: treeName, Key: key, Operation: operation, Kind: subjectKind),
            static (client, state, ct) => client.ExplainAsync(state.TenantId, state.SubjectId, state.TreeName, state.Key, state.Operation, state.Kind, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantEffectivePermissions> EffectivePermissionsAsync(
        string tenantId,
        string subjectId,
        string? treeName = null,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return CallAsync(
            (TenantId: tenantId, SubjectId: subjectId, TreeName: treeName, Kind: subjectKind),
            static (client, state, ct) => client.EffectivePermissionsAsync(state.TenantId, state.SubjectId, state.TreeName, state.Kind, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantAccessPosture> GetPostureAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.GetPostureAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantAccessFaults.Map(exception, subject, cancellationToken);
}
