using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Policy.TenantPolicyTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// Unit tests for <see cref="LatticeTenantPolicyAdmin"/>: the shared prologue every
/// operation runs (tenant parsing, tenant-tier authorization, the feature flag, and
/// the reserved default tenant, in that order) and the constructor guards.
/// Rules, introspection and posture are covered in the partial files.
/// </summary>
[TestFixture]
public sealed partial class LatticeTenantPolicyAdminTests
{
    /// <summary>Every facade operation, invoked for <paramref name="tenantId"/> with valid other arguments.</summary>
    private static readonly (string Name, Func<ILatticeTenantPolicyAdmin, string, Task> Call)[] Operations =
    [
        ("PutRule", (f, t) => f.PutRuleAsync(t, Draft())),
        ("GetRule", (f, t) => f.GetRuleAsync(t, "r1")),
        ("RemoveRule", (f, t) => f.RemoveRuleAsync(t, "r1")),
        ("ListRules", (f, t) => f.ListRulesAsync(t, new TenantAccessPageRequest())),
        ("Explain", (f, t) => f.ExplainAsync(t, Member, "orders", null, LatticeOperation.Read)),
        ("EffectivePermissions", (f, t) => f.EffectivePermissionsAsync(t, Member)),
        ("GetPosture", (f, t) => f.GetPostureAsync(t)),
    ];

    private static IEnumerable<TestCaseData> AllOperations() =>
        Operations.Select(o => new TestCaseData(o.Name).SetName($"{{m}}({o.Name})"));

    private static IEnumerable<TestCaseData> GatedOperations() =>
        Operations.Where(o => o.Name != "GetPosture").Select(o => new TestCaseData(o.Name).SetName($"{{m}}({o.Name})"));

    private static Func<ILatticeTenantPolicyAdmin, string, Task> Call(string name) =>
        Operations.Single(o => o.Name == name).Call;

    [TestCaseSource(nameof(AllOperations))]
    public void Operation_on_the_default_tenant_is_refused_as_reserved_after_authorization(string operation)
    {
        var harness = new Harness { Caller = new LatticeSubject(Operator) };
        harness.SeedTenant(TenantId.DefaultId, default);
        var facade = harness.Create();

        Assert.That(
            () => Call(operation)(facade, TenantId.DefaultId),
            Throws.TypeOf<ReservedTenantOperationException>());
        Assert.Multiple(() =>
        {
            Assert.That(harness.Store.FullScans, Is.Zero, "nothing is read for the reserved tenant");
            Assert.That(harness.Store.Writes, Is.Zero);
        });
    }

    [Test]
    public void Operation_on_the_default_tenant_by_a_non_operator_is_denied_first()
    {
        var harness = new Harness();
        harness.SeedTenant(TenantId.DefaultId, default);

        Assert.That(
            () => harness.Create().GetPostureAsync(TenantId.DefaultId),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [TestCaseSource(nameof(AllOperations))]
    public void Operation_with_an_invalid_tenant_id_is_refused_as_an_argument_error(string operation)
    {
        var facade = new Harness().Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => Call(operation)(facade, "Not A Tenant"), Throws.ArgumentException);
            Assert.That(() => Call(operation)(facade, string.Empty), Throws.ArgumentException);
        });
    }

    [TestCaseSource(nameof(AllOperations))]
    public void Operation_by_a_caller_who_is_neither_operator_nor_tenant_admin_is_denied_before_anything_is_read(string operation)
    {
        var harness = new Harness { Caller = new LatticeSubject(Stranger) };
        var facade = harness.Create();

        Assert.That(() => Call(operation)(facade, Tenant), Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.Multiple(() =>
        {
            Assert.That(harness.Store.FullScans, Is.Zero);
            Assert.That(harness.Store.Writes, Is.Zero);
            Assert.That(harness.Decisions.Evaluations, Is.Empty);
        });
    }

    [TestCaseSource(nameof(AllOperations))]
    public void Operation_by_an_admin_of_another_tenant_is_denied(string operation)
    {
        var harness = new Harness { Caller = new LatticeSubject("olivia") };
        var facade = harness.Create();

        Assert.That(() => Call(operation)(facade, Tenant), Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [TestCaseSource(nameof(GatedOperations))]
    public void Operation_while_the_feature_is_off_is_refused_as_disabled_after_authorization(string operation)
    {
        var harness = new Harness { Enabled = false };
        var facade = harness.Create();

        var ex = Assert.ThrowsAsync<TenantAccessAdministrationDisabledException>(() => Call(operation)(facade, Tenant));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo(Tenant));
            Assert.That(harness.Store.FullScans, Is.Zero, "nothing is read while the feature is off");
            Assert.That(harness.Store.Writes, Is.Zero);
        });
    }

    [TestCaseSource(nameof(GatedOperations))]
    public void Operation_while_the_feature_is_off_still_denies_an_unauthorized_caller_first(string operation)
    {
        var harness = new Harness { Enabled = false, Caller = new LatticeSubject(Stranger) };
        var facade = harness.Create();

        Assert.That(() => Call(operation)(facade, Tenant), Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "an unauthorized caller must not learn that the feature is off");
    }

    [TestCaseSource(nameof(AllOperations))]
    public void Operation_by_a_platform_operator_is_authorized(string operation)
    {
        var harness = new Harness { Caller = new LatticeSubject(Operator) };
        harness.AdmitMember(Member);
        var facade = harness.Create();

        Assert.That(() => Call(operation)(facade, Tenant), Throws.Nothing);
    }

    [Test]
    public void Operation_on_a_tenant_that_does_not_exist_is_denied_to_a_non_operator()
    {
        var facade = new Harness().Create();

        Assert.That(() => facade.GetPostureAsync("nobody"), Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void Constructor_rejects_every_null_required_dependency()
    {
        var harness = new Harness();
        var gate = new TenantAdminTestSupport.FixedGate(true);
        var authorizer = new TenantRegionResidencyAuthorizer(gate, harness.Registry);

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTenantPolicyAdmin(null!, harness.Store, harness.Directory, harness.Decisions, gate, () => true), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantPolicyAdmin(authorizer, null!, harness.Directory, harness.Decisions, gate, () => true), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantPolicyAdmin(authorizer, harness.Store, null!, harness.Decisions, gate, () => true), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantPolicyAdmin(authorizer, harness.Store, harness.Directory, null!, gate, () => true), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantPolicyAdmin(authorizer, harness.Store, harness.Directory, harness.Decisions, null!, () => true), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantPolicyAdmin(authorizer, harness.Store, harness.Directory, harness.Decisions, gate, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Constructor_accepts_no_membership_context_and_then_treats_every_caller_as_anonymous()
    {
        var harness = new Harness();
        var gate = new TenantAdminTestSupport.FixedGate(false);
        var facade = new LatticeTenantPolicyAdmin(
            new TenantRegionResidencyAuthorizer(gate, harness.Registry),
            harness.Store,
            harness.Directory,
            harness.Decisions,
            gate,
            () => true);

        Assert.That(() => facade.GetPostureAsync(Tenant), Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }
}
