using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

public sealed partial class LatticeTenantScopedTreeAdminTests
{
    [TestCase("Set", LatticeOperation.SchemaAdmin)]
    [TestCase("Clear", LatticeOperation.SchemaAdmin)]
    [TestCase("Get", LatticeOperation.Read)]
    public void SchemaPolicy_denied_caller_never_reaches_schema_admin(string verb, LatticeOperation operation)
    {
        var facade = CreateFacade(out _, out var schemaAdmin, new TenantAdminTestSupport.FixedGate(allow: false));
        using var scope = ActiveTenant();

        var exception = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => InvokeSchemaPolicyAsync(facade, verb));

        Assert.That(exception!.Operation, Is.EqualTo(operation));
        Assert.That(schemaAdmin.ReceivedCalls(), Is.Empty);
    }

    [TestCase("Set", LatticeOperation.SchemaAdmin)]
    [TestCase("Clear", LatticeOperation.SchemaAdmin)]
    [TestCase("Get", LatticeOperation.Read)]
    public void SchemaPolicy_asserted_victim_tenant_does_not_authorize_caller(string verb, LatticeOperation operation)
    {
        var gate = new SchemaScopeGate(operation);
        var schemaAdmin = Substitute.For<ILatticeSchemaAdmin>();
        var facade = new LatticeTenantScopedTreeAdmin(
            Substitute.For<ILatticeTreeAdmin>(), schemaAdmin, gate,
            new TenantAdminTestSupport.FixedMembershipContext(new LatticeSubject("acme-admin")));
        using var scope = ActiveTenant("victim");

        Assert.That(() => InvokeSchemaPolicyAsync(facade, verb),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(gate.LastRequest!.Value.Subject.SubjectId, Is.EqualTo("acme-admin"));
        Assert.That(gate.LastRequest.Value.TreeId, Is.EqualTo("t/victim/orders"));
        Assert.That(gate.LastRequest.Value.Operation, Is.EqualTo(operation));
        Assert.That(schemaAdmin.ReceivedCalls(), Is.Empty);
    }

    [TestCase("Set", LatticeOperation.SchemaAdmin)]
    [TestCase("Clear", LatticeOperation.SchemaAdmin)]
    [TestCase("Get", LatticeOperation.Read)]
    public async Task SchemaPolicy_authorized_caller_delegates_after_whole_tree_check(
        string verb, LatticeOperation operation)
    {
        var gate = new SchemaScopeGate(operation);
        var schemaAdmin = Substitute.For<ILatticeSchemaAdmin>();
        var facade = new LatticeTenantScopedTreeAdmin(
            Substitute.For<ILatticeTreeAdmin>(), schemaAdmin, gate,
            new TenantAdminTestSupport.FixedMembershipContext(new LatticeSubject("acme-admin")));
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());
        using var cancellation = new CancellationTokenSource();
        var token = cancellation.Token;
        schemaAdmin.SetPolicyAsync("t/acme/orders", policy, token)
            .Returns(_ =>
            {
                Assert.That(gate.LastRequest, Is.Not.Null, "Authorization must precede delegation.");
                return Task.CompletedTask;
            });
        schemaAdmin.ClearPolicyAsync("t/acme/orders", token)
            .Returns(_ =>
            {
                Assert.That(gate.LastRequest, Is.Not.Null, "Authorization must precede delegation.");
                return Task.FromResult(true);
            });
        schemaAdmin.GetPolicyAsync("t/acme/orders", token)
            .Returns(_ =>
            {
                Assert.That(gate.LastRequest, Is.Not.Null, "Authorization must precede delegation.");
                return Task.FromResult<LatticeSchemaPolicy?>(policy);
            });
        using var scope = ActiveTenant();

        var result = await InvokeSchemaPolicyAsync(facade, verb, policy, token);

        Assert.That(gate.Calls, Is.EqualTo(1));
        Assert.That(gate.LastToken, Is.EqualTo(token));
        var request = gate.LastRequest!.Value;
        Assert.That(request.TreeId, Is.EqualTo("t/acme/orders"));
        Assert.That(request.Operation, Is.EqualTo(operation));
        Assert.That(request.Subject.SubjectId, Is.EqualTo("acme-admin"));
        Assert.That(request.Key, Is.Null);
        Assert.That(request.RangeStart, Is.Null);
        Assert.That(request.RangeEnd, Is.Null);
        Assert.That(schemaAdmin.ReceivedCalls().Count(), Is.EqualTo(1));
        switch (verb)
        {
            case "Set":
                await schemaAdmin.Received(1).SetPolicyAsync("t/acme/orders", policy, token);
                break;
            case "Clear":
                Assert.That(result, Is.EqualTo(true));
                await schemaAdmin.Received(1).ClearPolicyAsync("t/acme/orders", token);
                break;
            case "Get":
                Assert.That(result, Is.SameAs(policy));
                await schemaAdmin.Received(1).GetPolicyAsync("t/acme/orders", token);
                break;
        }
    }

    [TestCase("Set")]
    [TestCase("Clear")]
    [TestCase("Get")]
    public void SchemaPolicy_filtered_allow_is_refused_before_delegation(string verb)
    {
        var facade = CreateFacade(out _, out var schemaAdmin, new TenantAdminTestSupport.FilteredGate());
        using var scope = ActiveTenant();

        Assert.That(() => InvokeSchemaPolicyAsync(facade, verb),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(schemaAdmin.ReceivedCalls(), Is.Empty);
    }

    [TestCase("Set", LatticeOperation.Read)]
    [TestCase("Clear", LatticeOperation.Read)]
    [TestCase("Get", LatticeOperation.SchemaAdmin)]
    public void SchemaPolicy_wrong_capability_does_not_authorize(string verb, LatticeOperation granted)
    {
        var schemaAdmin = Substitute.For<ILatticeSchemaAdmin>();
        var facade = new LatticeTenantScopedTreeAdmin(
            Substitute.For<ILatticeTreeAdmin>(), schemaAdmin, new SchemaScopeGate(granted),
            new TenantAdminTestSupport.FixedMembershipContext(new LatticeSubject("acme-admin")));
        using var scope = ActiveTenant();

        Assert.That(() => InvokeSchemaPolicyAsync(facade, verb),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(schemaAdmin.ReceivedCalls(), Is.Empty);
    }

    [TestCase("Set")]
    [TestCase("Clear")]
    [TestCase("Get")]
    public async Task SchemaPolicy_system_origin_preserves_gate_bypass(string verb)
    {
        var gate = new TenantAdminTestSupport.FixedGate(allow: false);
        var facade = CreateFacade(out _, out var schemaAdmin, gate);
        using var scope = ActiveTenant();
        using var system = LatticeAccessGateContext.EnterSystemOrigin();

        await InvokeSchemaPolicyAsync(facade, verb);

        Assert.That(gate.Calls, Is.Zero);
        Assert.That(schemaAdmin.ReceivedCalls().Count(), Is.EqualTo(1));
    }

    [TestCase("Set")]
    [TestCase("Clear")]
    [TestCase("Get")]
    public async Task SchemaPolicy_no_op_host_preserves_delegation(string verb)
    {
        var facade = CreateFacade(out _, out var schemaAdmin, new NullLatticeAccessGate());
        using var scope = ActiveTenant();

        await InvokeSchemaPolicyAsync(facade, verb);

        Assert.That(schemaAdmin.ReceivedCalls().Count(), Is.EqualTo(1));
    }

    private static async Task<object?> InvokeSchemaPolicyAsync(
        ILatticeTenantScopedTreeAdmin facade, string verb,
        LatticeSchemaPolicy? policy = null, CancellationToken cancellationToken = default)
    {
        switch (verb)
        {
            case "Set":
                await facade.SetSchemaPolicyAsync("orders",
                    policy ?? new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>()), cancellationToken);
                return null;
            case "Clear":
                return await facade.ClearSchemaPolicyAsync("orders", cancellationToken);
            case "Get":
                return await facade.GetSchemaPolicyAsync("orders", cancellationToken);
            default:
                throw new ArgumentOutOfRangeException(nameof(verb));
        }
    }

    private sealed class SchemaScopeGate(LatticeOperation granted) : ILatticeAccessGate
    {
        public LatticeAccessRequest? LastRequest { get; private set; }
        public CancellationToken LastToken { get; private set; }
        public int Calls { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Calls++;
            LastRequest = request;
            LastToken = cancellationToken;
            return new(request.Subject.SubjectId == "acme-admin"
                && request.TreeId == "t/acme/orders"
                && request.Operation == granted
                    ? LatticeAccessDecision.Allow()
                    : LatticeAccessDecision.Deny("Caller lacks the capability on this tenant tree."));
        }
    }
}
