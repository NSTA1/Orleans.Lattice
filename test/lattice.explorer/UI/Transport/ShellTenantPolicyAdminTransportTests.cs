using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantPolicyAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantPolicyAdminTransportTests : ShellTransportAdapterContractTests<ILatticeTenantPolicyAdmin>
{
    private const string Service = "/orleans.lattice.api.tenantadmin/";

    private static readonly TenantRuleDraft Draft = new()
    {
        RuleId = "operators-read-orders",
        SubjectId = "operators",
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = TenantRuleScopeKind.Tree,
        TreeName = "orders",
        Operations = LatticeOperation.Read,
        Effect = LatticeEffect.Allow,
    };

    internal override IEnumerable<ShellTransportCall<ILatticeTenantPolicyAdmin>> Calls() =>
    [
        new("PutRuleAsync", Service + "PutTenantRule", (f, ct) => f.PutRuleAsync("globex", Draft, ct)),
        new("GetRuleAsync", Service + "GetTenantRule", (f, ct) => f.GetRuleAsync("globex", "operators-read-orders", ct)),
        new("RemoveRuleAsync", Service + "RemoveTenantRule", (f, ct) => f.RemoveRuleAsync("globex", "operators-read-orders", ct)),
        new("ListRulesAsync", Service + "ListTenantRules", (f, ct) => f.ListRulesAsync("globex", new TenantAccessPageRequest(), ct)),
        new("ExplainAsync", Service + "ExplainTenantAccess", (f, ct) => f.ExplainAsync("globex", "alice", "orders", null, LatticeOperation.Read, TenantSubjectKind.User, ct)),
        new("EffectivePermissionsAsync", Service + "GetTenantEffectivePermissions", (f, ct) => f.EffectivePermissionsAsync("globex", "operators", "orders", TenantSubjectKind.TenantGroup, ct)),
        new("GetPostureAsync", Service + "GetTenantAccessPosture", (f, ct) => f.GetPostureAsync("globex", ct)),
    ];

    [Test]
    public void The_switched_off_feature_maps_to_its_typed_refusal_naming_the_tenant()
    {
        using var circuit = new ShellTransportCircuit();
        var policy = circuit.Resolve<ILatticeTenantPolicyAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, new TenantAccessAdministrationDisabledException("globex").Message);

        var ex = Assert.ThrowsAsync<TenantAccessAdministrationDisabledException>(() => policy.PutRuleAsync("globex", Draft));

        Assert.That(ex!.TenantId, Is.EqualTo("globex"));
    }

    [Test]
    public void A_reached_rule_cap_maps_to_a_quota_refusal()
    {
        using var circuit = new ShellTransportCircuit();
        var policy = circuit.Resolve<ILatticeTenantPolicyAdmin>();
        circuit.Peer.AnswerWith(
            StatusCode.ResourceExhausted,
            "Tenant globex is at its cap of 1000 rules.",
            new Dictionary<string, string>
            {
                ["lattice-quota-dimension"] = "tenant-rules",
                ["lattice-quota-current"] = "1000",
                ["lattice-quota-limit"] = "1000",
            });

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(() => policy.PutRuleAsync("globex", Draft));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo("tenant-rules"));
            Assert.That(ex.Limit, Is.EqualTo(1000));
            Assert.That(ex.TenantId, Is.EqualTo("globex"));
        });
    }

    [Test]
    public void A_refused_posture_read_maps_to_the_facade_denial()
    {
        using var circuit = new ShellTransportCircuit();
        var policy = circuit.Resolve<ILatticeTenantPolicyAdmin>();
        circuit.Peer.AnswerWith(StatusCode.PermissionDenied, "not an administrator of tenant globex");

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => policy.GetPostureAsync("globex"));

        Assert.That(ex!.Message, Is.EqualTo("not an administrator of tenant globex"));
    }

    [Test]
    public void A_cluster_that_does_not_serve_the_facade_maps_to_not_supported()
    {
        using var circuit = new ShellTransportCircuit();
        var policy = circuit.Resolve<ILatticeTenantPolicyAdmin>();
        circuit.Peer.AnswerWith(StatusCode.Unimplemented, "not served");

        Assert.That(() => policy.GetPostureAsync("globex"), Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void A_cancelled_call_carries_the_callers_token()
    {
        using var circuit = new ShellTransportCircuit();
        var policy = circuit.Resolve<ILatticeTenantPolicyAdmin>();
        using var cancellation = new CancellationTokenSource();
        circuit.Peer.AnswerWith(StatusCode.Cancelled, "cancelled");

        var ex = Assert.CatchAsync<OperationCanceledException>(() => policy.ListRulesAsync("globex", new TenantAccessPageRequest(), cancellation.Token));

        Assert.That(ex!.CancellationToken, Is.EqualTo(cancellation.Token));
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var policy = circuit.Resolve<ILatticeTenantPolicyAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => policy.PutRuleAsync(string.Empty, Draft), Throws.ArgumentException);
            Assert.That(() => policy.PutRuleAsync("globex", null!), Throws.ArgumentNullException);
            Assert.That(() => policy.GetRuleAsync("globex", string.Empty), Throws.ArgumentException);
            Assert.That(() => policy.RemoveRuleAsync(string.Empty, "rule"), Throws.ArgumentException);
            Assert.That(() => policy.ListRulesAsync("globex", null!), Throws.ArgumentNullException);
            Assert.That(() => policy.ExplainAsync("globex", string.Empty, "orders", null, LatticeOperation.Read), Throws.ArgumentException);
            Assert.That(() => policy.ExplainAsync("globex", "alice", string.Empty, null, LatticeOperation.Read), Throws.ArgumentException);
            Assert.That(() => policy.EffectivePermissionsAsync("globex", string.Empty), Throws.ArgumentException);
            Assert.That(() => policy.GetPostureAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
