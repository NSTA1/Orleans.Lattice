using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The plugin's access gate: whether the tenant self-service area exists for the
/// current caller, resolved onto the shell's four-state access model.
/// <para>
/// Every branch fails closed. A deployment without the tenancy add-on reports
/// the surface unavailable and makes no call at all (epic decision D9); an
/// authenticated caller who does not administer the active tenant is denied; and
/// a not-found answer - which the cluster returns for a tenant the caller may
/// not see - withholds the grant rather than reporting an absence, so the gate
/// cannot be used to probe for tenants.
/// </para>
/// <para>
/// The gate reports facts and <see cref="ExplorerPluginAccessContract"/> picks
/// the state, so these tests run signed in unless they say otherwise - an
/// anonymous caller is invited to sign in rather than told they lack authority
/// over a tenant they never had (issue #1854).
/// </para>
/// </summary>
[TestFixture]
public sealed class MyTenantAccessGateTests
{
    private sealed class HeadSuppliedGate : IExplorerTenantOperatorGate
    {
        public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
            new(true);
    }

    private static IExplorerAuthSession SignedIn(bool authenticated = true)
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(authenticated);
        return session;
    }

    private static IExplorerPluginHostContext Context(bool tenancyActive = true, string? tenantId = null)
    {
        var state = new FakeExplorerPluginHostState
        {
            Tenant = new ExplorerPluginTenantScope(
                tenancyActive,
                tenantId ?? (tenancyActive ? MyTenantSample.TenantId : null),
                ExplorerPluginTenantVisibility.ActiveTenant),
        };

        return new ExplorerPluginHostContext(
            MyTenantPluginKeys.PluginId,
            state,
            new FakeExplorerPluginPreferences(),
            new ExplorerPluginDomainResolver(
                new ExplorerPluginCatalog([]),
                new ServiceCollection().BuildServiceProvider()));
    }

    private static async Task<(ExplorerPluginAccess Access, FakeTenancyDomain Domain, ExplorerPluginAccessStore Store)>
        ProbeAsync(
            Action<FakeTenancyDomain>? configure = null,
            bool tenancyActive = true,
            IExplorerTenantOperatorGate? operatorGate = null,
            bool authenticated = true)
    {
        var domain = new FakeTenancyDomain();
        configure?.Invoke(domain);

        var store = new ExplorerPluginAccessStore();
        var gate = new MyTenantAccessGate(domain, store, operatorGate, SignedIn(authenticated));
        var access = await gate.ProbeAsync(Context(tenancyActive));

        return (access, domain, store);
    }

    [Test]
    public async Task A_deployment_without_the_tenancy_add_on_reports_the_surface_unavailable()
    {
        var (access, domain, _) = await ProbeAsync(d => d.IsTenancyEnabled = false);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.IsVisible, Is.False, "the shell renders no entry at all");
            Assert.That(access.Reason, Is.Not.Null);
            Assert.That(
                domain.Service.AdminSubjectListCalls,
                Is.Zero,
                "no call is made when the add-on is absent");
        });
    }

    [Test]
    public async Task An_anonymous_caller_is_asked_to_sign_in_rather_than_told_they_lack_tenant_authority()
    {
        // Signed out there is no account to be refused for, so a withheld
        // tenant-admin grant is a recoverable sign-in prompt rather than a
        // statement about authority the visitor never claimed.
        var (access, _, _) = await ProbeAsync(
            d => d.Service.Admins =
                TenantOperationResult<ExplorerTenantAdmins>.Failure(TenantOperationStatus.Denied, "no"),
            authenticated: false);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task A_host_reporting_an_inactive_tenant_scope_also_degrades_to_unavailable()
    {
        var domain = new FakeTenancyDomain();
        var gate = new MyTenantAccessGate(domain, new ExplorerPluginAccessStore(), session: SignedIn());

        var access = await gate.ProbeAsync(Context(tenancyActive: false));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(domain.Service.AdminSubjectListCalls, Is.Zero);
        });
    }

    [Test]
    public async Task A_caller_who_administers_the_active_tenant_is_admitted()
    {
        var (access, domain, _) = await ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(domain.Service.AdminSubjectListCalls, Is.EqualTo(1));
            Assert.That(
                domain.Service.TenantIdsTouched,
                Is.EqualTo(new[] { MyTenantSample.TenantId }),
                "the probe asks about the active tenant and no other");
        });
    }

    [Test]
    public async Task An_authenticated_caller_who_does_not_administer_the_tenant_is_denied()
    {
        var (access, _, _) = await ProbeAsync(d => d.Service.Admins =
            TenantOperationResult<ExplorerTenantAdmins>.Failure(TenantOperationStatus.Denied, "no"));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.IsVisible, Is.True, "a denial greys out rather than hides");
        });
    }

    [Test]
    public async Task A_not_found_answer_is_a_denial_so_the_gate_cannot_probe_for_tenants()
    {
        var (access, _, _) = await ProbeAsync(d => d.Service.Admins =
            TenantOperationResult<ExplorerTenantAdmins>.Failure(TenantOperationStatus.NotFound, "gone"));

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task An_unauthenticated_connection_asks_for_a_sign_in_rather_than_greying_out()
    {
        var (access, _, _) = await ProbeAsync(d => d.Service.Admins =
            TenantOperationResult<ExplorerTenantAdmins>.Failure(
                TenantOperationStatus.AuthenticationRequired,
                "sign in"));

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
    }

    [Test]
    public async Task A_cluster_that_does_not_serve_the_surface_reports_unavailable()
    {
        var (access, _, _) = await ProbeAsync(d => d.Service.Admins =
            TenantOperationResult<ExplorerTenantAdmins>.Failure(
                TenantOperationStatus.Unavailable,
                "not served"));

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
    }

    [Test]
    public async Task A_transport_failure_is_a_denial_rather_than_an_uninstall()
    {
        // A dropped connection must never hide the plugin: that would make a
        // reconnect look like the capability had been removed.
        var (access, _, _) = await ProbeAsync(d => d.Service.Admins =
            TenantOperationResult<ExplorerTenantAdmins>.Failure(TenantOperationStatus.Failed, "boom"));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task An_availability_refusal_passes_straight_through_and_narrows_nothing()
    {
        var (access, domain, _) = await ProbeAsync(d =>
            d.Availability = ExplorerPluginAccess.ReportUnavailable("no tenancy facade"));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.EqualTo("no tenancy facade"));
            Assert.That(
                domain.Service.AdminSubjectListCalls,
                Is.Zero,
                "the admin probe is not reached once availability has already refused");
        });
    }

    [Test]
    public async Task A_caller_with_no_active_tenant_is_denied_rather_than_probing_a_blank_tenant()
    {
        var domain = new FakeTenancyDomain { ActiveTenant = null };
        var gate = new MyTenantAccessGate(domain, new ExplorerPluginAccessStore(), session: SignedIn());

        var access = await gate.ProbeAsync(Context(tenancyActive: true, tenantId: string.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(domain.Service.AdminSubjectListCalls, Is.Zero);
        });
    }

    [Test]
    public async Task The_hosts_tenant_scope_is_used_when_the_domain_has_not_established_one()
    {
        var domain = new FakeTenancyDomain { ActiveTenant = null };
        var gate = new MyTenantAccessGate(domain, new ExplorerPluginAccessStore(), session: SignedIn());

        var access = await gate.ProbeAsync(Context(tenancyActive: true, tenantId: MyTenantSample.TenantId));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(domain.Service.TenantIdsTouched, Is.EqualTo(new[] { MyTenantSample.TenantId }));
        });
    }

    [Test]
    public async Task A_misordered_head_files_the_operator_gate_diagnostic()
    {
        // The placeholder is internal to the navigation core, so it is obtained
        // exactly as a misordered head would obtain it.
        var services = new ServiceCollection();
        services.AddExplorerTenantView();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var placeholder = scope.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>();

        var (_, _, store) = await ProbeAsync(operatorGate: placeholder);
        var diagnostic = store.Get(MyTenantPluginKeys.PluginId, MyTenantPluginKeys.OperatorGateScope);

        Assert.Multiple(() =>
        {
            Assert.That(diagnostic.IsAllowed, Is.False);
            Assert.That(diagnostic.Reason, Is.EqualTo(MyTenantOperatorGateDiagnostic.PlaceholderGateMessage));
        });
    }

    [Test]
    public async Task A_correctly_ordered_head_files_a_clean_diagnostic()
    {
        var (_, _, store) = await ProbeAsync(operatorGate: new HeadSuppliedGate());
        var diagnostic = store.Get(MyTenantPluginKeys.PluginId, MyTenantPluginKeys.OperatorGateScope);

        Assert.Multiple(() =>
        {
            Assert.That(diagnostic.IsAllowed, Is.True);
            Assert.That(diagnostic.Reason, Is.Null);
        });
    }

    [Test]
    public async Task The_diagnostic_is_filed_even_when_the_surface_is_unavailable()
    {
        // Filed before the D9 short-circuit, so a head can be told it is
        // misordered without first having to make the surface reachable.
        var (_, _, store) = await ProbeAsync(
            d => d.IsTenancyEnabled = false,
            operatorGate: new HeadSuppliedGate());

        Assert.That(
            store.Get(MyTenantPluginKeys.PluginId, MyTenantPluginKeys.OperatorGateScope).IsAllowed,
            Is.True);
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        var store = new ExplorerPluginAccessStore();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new MyTenantAccessGate(null!, store),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => new MyTenantAccessGate(new FakeTenancyDomain(), null!),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => new MyTenantAccessGate(new FakeTenancyDomain(), store)
                    .ProbeAsync(null!)
                    .AsTask()
                    .GetAwaiter()
                    .GetResult(),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }
}
