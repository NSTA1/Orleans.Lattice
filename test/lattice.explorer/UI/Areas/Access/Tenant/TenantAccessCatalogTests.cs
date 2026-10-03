using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// The tenant access catalogue: the posture probe read as a standing, memoised
/// for the caller and tenant; nothing asked for a tenant that cannot have
/// delegated administration; a fault never remembered; and the first page of a
/// tenant's groups and rules for the picker and completion.
/// </summary>
[TestFixture]
public sealed class TenantAccessCatalogTests
{
    private FakeTenantAccessFacades _facades = null!;
    private FakeActiveTenantProvider _tenant = null!;
    private TenantAccessCatalog _catalog = null!;

    [SetUp]
    public void SetUp()
    {
        _facades = new FakeTenantAccessFacades();
        _tenant = new FakeActiveTenantProvider("acme");
        _catalog = new TenantAccessCatalog(_facades, ShellCaller.Unobserved(tenant: _tenant));
    }

    [Test]
    [TestCase(false, true, true, 1)]
    [TestCase(true, true, false, 3)]
    [TestCase(true, false, true, 3)]
    [TestCase(true, false, false, 2)]
    public void A_posture_is_read_as_a_standing(bool enabled, bool admin, bool operatorCaller, int standing)
    {
        var expected = (TenantAccessStanding)standing;
        var state = TenantAccessState.From("acme", new TenantAccessPosture
        {
            TenantId = "acme",
            Enabled = enabled,
            CallerIsTenantAdmin = admin,
            CallerIsPlatformOperator = operatorCaller,
        });

        Assert.Multiple(() =>
        {
            Assert.That(state.Standing, Is.EqualTo(expected));
            Assert.That(state.IsDelegated, Is.EqualTo(expected == TenantAccessStanding.Delegated));
            Assert.That(state.Posture, Is.Not.Null);
            Assert.That(() => TenantAccessState.From("acme", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_delegated_tenant_is_read_once_for_the_caller()
    {
        _facades.AsTenantAdmin();

        var first = await _catalog.GetStateAsync("acme", CancellationToken.None);
        var second = await _catalog.GetStateAsync("acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.IsDelegated, Is.True);
            Assert.That(second, Is.SameAs(first));
            Assert.That(_facades.Gate.Calls, Is.EqualTo(new[] { "GetPostureAsync" }));
        });
    }

    [Test]
    public async Task Another_caller_is_asked_again()
    {
        _facades.AsTenantAdmin();
        await _catalog.GetStateAsync("acme", CancellationToken.None);

        _tenant.Set("globex");
        _facades.AsMember();
        var state = await _catalog.GetStateAsync("acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.Standing, Is.EqualTo(TenantAccessStanding.NotPermitted));
            Assert.That(_facades.Gate.Calls, Has.Count.EqualTo(2));
        });
    }

    [Test]
    [TestCase("default")]
    [TestCase("Not A Tenant")]
    public async Task A_tenant_that_cannot_have_delegated_administration_is_never_asked(string tenant)
    {
        _facades.AsTenantAdmin();

        var state = await _catalog.GetStateAsync(tenant, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.Standing, Is.EqualTo(TenantAccessStanding.Unavailable));
            Assert.That(_facades.Gate.Calls, Is.Empty);
            Assert.That(TenantAccessCatalog.Administers(tenant), Is.False);
            Assert.That(TenantAccessCatalog.Administers(null), Is.False);
            Assert.That(TenantAccessCatalog.Administers("acme"), Is.True);
        });
    }

    [Test]
    public async Task Without_a_tenant_policy_facade_the_standing_is_unavailable()
    {
        _facades.AsTenantAdmin();
        _facades.ServesPolicy = false;

        var state = await _catalog.GetStateAsync("acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.Standing, Is.EqualTo(TenantAccessStanding.Unavailable));
            Assert.That(_catalog.IsServed, Is.False);
        });
    }

    [Test]
    public async Task A_refused_posture_is_not_permitted_and_a_disabled_one_is_off()
    {
        _facades.AsTenantAdmin();
        _facades.Gate.Denied = true;
        var denied = await _catalog.GetStateAsync("acme", CancellationToken.None);

        _catalog.Invalidate();
        _facades.Gate.Denied = false;
        _facades.Gate.NextFailure = new TenantAccessAdministrationDisabledException("acme");
        var disabled = await _catalog.GetStateAsync("acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(denied.Standing, Is.EqualTo(TenantAccessStanding.NotPermitted));
            Assert.That(disabled.Standing, Is.EqualTo(TenantAccessStanding.Off));
        });
    }

    [Test]
    public async Task A_fault_reads_as_unavailable_and_is_asked_again()
    {
        _facades.AsTenantAdmin();
        _facades.Gate.NextFailure = new TimeoutException();

        var faulted = await _catalog.GetStateAsync("acme", CancellationToken.None);
        var recovered = await _catalog.GetStateAsync("acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(faulted.Standing, Is.EqualTo(TenantAccessStanding.Unavailable));
            Assert.That(recovered.IsDelegated, Is.True);
        });
    }

    [Test]
    public void A_cancelled_read_propagates()
    {
        _facades.AsTenantAdmin();
        _facades.Gate.NextFailure = new OperationCanceledException();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await _catalog.GetStateAsync("acme", CancellationToken.None));
    }

    [Test]
    public async Task The_scope_state_is_null_on_a_cluster_wide_page()
    {
        _facades.AsTenantAdmin();

        Assert.Multiple(async () =>
        {
            Assert.That(await _catalog.GetStateForScopeAsync(null, CancellationToken.None), Is.Null);
            Assert.That((await _catalog.GetStateForScopeAsync("acme", CancellationToken.None))!.IsDelegated, Is.True);
        });
    }

    [Test]
    public async Task A_tenants_groups_and_rules_are_read_once_until_invalidated()
    {
        _facades.AsTenantAdmin().WithGroup("acme", "ops").WithGroup("globex", "finance");

        var groups = await _catalog.GetGroupsAsync("acme", CancellationToken.None);
        await _catalog.GetGroupsAsync("acme", CancellationToken.None);
        var rules = await _catalog.GetRulesAsync("acme", CancellationToken.None);
        await _catalog.GetRulesAsync("acme", CancellationToken.None);
        _catalog.Invalidate();
        await _catalog.GetGroupsAsync("acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(groups.Select(group => group.Name), Is.EqualTo(new[] { "ops" }));
            Assert.That(rules, Is.Empty);
            Assert.That(_facades.Gate.Calls, Is.EqualTo(new[] { "ListGroupsAsync", "ListRulesAsync", "ListGroupsAsync" }));
        });
    }

    [Test]
    public void Groups_and_rules_need_their_facades()
    {
        _facades.ServesDirectory = false;
        _facades.ServesPolicy = false;

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<NotSupportedException>(async () => await _catalog.GetGroupsAsync("acme", CancellationToken.None));
            Assert.ThrowsAsync<NotSupportedException>(async () => await _catalog.GetRulesAsync("acme", CancellationToken.None));
            Assert.That(() => new TenantAccessCatalog(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_failure_is_classified_in_the_tenants_words()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantAccessFailure.From(new OperationCanceledException(), "acme"), Is.Null);
            Assert.That(TenantAccessFailure.From(new LatticeAuthorizationDeniedException("denied"), "acme"),
                Is.EqualTo(new AccessFailure(AccessFailureKind.Denied, TenantAccessFailure.DeniedMessage("acme"))));
            Assert.That(TenantAccessFailure.From(new TenantAccessAdministrationDisabledException("acme"), "acme")!.Message, Is.EqualTo(TenantAccessFailure.OffMessage));
            Assert.That(TenantAccessFailure.From(new ReservedTenantOperationException("default", "ListGroupsAsync"), "default")!.Kind, Is.EqualTo(AccessFailureKind.Invalid));
            Assert.That(TenantAccessFailure.From(new LatticeQuotaExceededException("At the cap.", string.Empty, "MaxGroups", 500, 500, "acme"), "acme")!.Message, Is.EqualTo("At the cap."));
            Assert.That(TenantAccessFailure.From(
                new TenantAccessConfinementException("acme", TenantAccessConfinementRule.ForeignTenantGroup, "Another tenant's group.", "memberId"), "acme")!.Kind,
                Is.EqualTo(AccessFailureKind.Invalid));
            Assert.That(TenantAccessFailure.From(new TimeoutException(), "acme")!.Kind, Is.EqualTo(AccessFailureKind.Unavailable));
            Assert.That(() => TenantAccessFailure.From(null!, "acme"), Throws.ArgumentNullException);
            Assert.That(() => TenantAccessFailure.From(new TimeoutException(), null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_gate_reads_once_per_scope_and_names_only_a_delegated_tenant()
    {
        _facades.AsTenantAdmin();
        var gate = new AccessTenantGate();

        var cluster = await gate.ResolveAsync(_catalog, null);
        var acme = await gate.ResolveAsync(_catalog, "acme");
        var again = await gate.ResolveAsync(_catalog, "acme");

        Assert.Multiple(() =>
        {
            Assert.That(cluster, Is.False);
            Assert.That(acme, Is.True);
            Assert.That(again, Is.True);
            Assert.That(gate.DelegatedTenant, Is.EqualTo("acme"));
            Assert.That(gate.Resolving, Is.False);
            Assert.That(_facades.Gate.Calls, Is.EqualTo(new[] { "GetPostureAsync" }));
            Assert.ThrowsAsync<ArgumentNullException>(async () => await gate.ResolveAsync(null!, "acme"));
        });
    }

    [Test]
    [TestCase(1, "off")]
    [TestCase(2, "not-permitted")]
    [TestCase(3, "delegated")]
    [TestCase(0, "unavailable")]
    public void The_gate_names_each_standing(int standing, string expected)
    {
        Assert.That(AccessTenantGate.StandingAttribute((TenantAccessStanding)standing), Is.EqualTo(expected));
    }

    [Test]
    public void The_gate_says_why_a_tenant_page_is_not_administered_here()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessTenantGate.Unavailable(new TenantAccessState("acme", TenantAccessStanding.Off), "member set"),
                Is.EqualTo("Delegated tenant access administration is off, so tenant acme's member set is not administered here."));
            Assert.That(AccessTenantGate.Unavailable(new TenantAccessState("acme", TenantAccessStanding.NotPermitted), "member set"),
                Does.StartWith("Tenant acme's member set is administered by its administrators"));
            Assert.That(AccessTenantGate.Unavailable(TenantAccessState.Unavailable("acme"), "member set"),
                Is.EqualTo("Tenant acme's member set is not administered here."));
            Assert.That(() => AccessTenantGate.Unavailable(null!, "x"), Throws.ArgumentNullException);
            Assert.That(() => AccessTenantGate.Unavailable(TenantAccessState.Unavailable("acme"), string.Empty), Throws.ArgumentException);
        });
    }
}
