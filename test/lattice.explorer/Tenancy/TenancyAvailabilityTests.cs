using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Covers the seam's availability detection: the tenancy-absent paths resolve to
/// the four-state model's <see cref="ExplorerPluginAccessState.Unavailable"/> so
/// the tenancy plugins render nothing, and no fault escapes a probe as an
/// exception.
/// </summary>
/// <remarks>
/// The probe reports facts and <see cref="ExplorerPluginAccessContract"/> picks
/// the state, so a refusal it cannot classify reads as a denial for a signed-in
/// caller and as a sign-in prompt for an anonymous one. Most of these tests
/// therefore run signed in, and
/// <see cref="A_refused_anonymous_caller_is_asked_to_sign_in_rather_than_denied"/>
/// covers the other half - the defect that told a signed-out visitor tenancy was
/// not available for their account (issue #1854).
/// </remarks>
[TestFixture]
public class TenancyAvailabilityTests
{
    private FakeTenantAdminClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTenantAdminClient();

    private static IExplorerAuthSession SignedIn(bool authenticated = true)
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(authenticated);
        return session;
    }

    private TenancyAvailability Create(StubTenantSwitcher? switcher) => new(_client, switcher, SignedIn());

    private TenancyAvailability Create() => new(_client, new StubTenantSwitcher(), SignedIn());

    private TenancyAvailability CreateAnonymous() =>
        new(_client, new StubTenantSwitcher(), SignedIn(authenticated: false));

    [Test]
    public void Constructor_rejects_a_null_client() =>
        Assert.That(() => new TenancyAvailability(null!), Throws.ArgumentNullException);

    [Test]
    public async Task No_tenant_view_reports_unavailable_without_touching_the_cluster()
    {
        var access = await Create(switcher: null).ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.IsVisible, Is.False, "an unavailable surface renders no entry at all");
            Assert.That(_client.CurrentTenantCallCount, Is.Zero, "no call is worth making without a tenant view");
            Assert.That(access.Reason, Is.Not.Null);
        });
    }

    [Test]
    public async Task An_inactive_tenant_view_reports_unavailable()
    {
        var access = await Create(new StubTenantSwitcher(isActive: false)).ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(_client.CurrentTenantCallCount, Is.Zero);
        });
    }

    [Test]
    public async Task An_absent_tenancy_add_on_reports_unavailable_rather_than_an_error()
    {
        _client.Throws = new TenancyUnavailableException("the tenancy facade is not registered");

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.EqualTo("the tenancy facade is not registered"));
        });
    }

    [Test]
    public async Task An_untranslated_unimplemented_status_also_reports_unavailable()
    {
        _client.Throws = new RpcException(new Status(StatusCode.Unimplemented, "not served here"));

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.EqualTo("not served here"));
        });
    }

    [Test]
    public async Task An_unimplemented_status_with_no_detail_still_carries_a_reason()
    {
        _client.Throws = new RpcException(new Status(StatusCode.Unimplemented, string.Empty));

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.Not.Empty);
        });
    }

    [Test]
    public async Task A_reachable_surface_is_allowed()
    {
        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(_client.CurrentTenantCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_refused_caller_is_denied_and_still_visible()
    {
        _client.Throws = new LatticeAuthorizationDeniedException("not yours");

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.IsVisible, Is.True, "a denial is demoted rather than hidden");
            Assert.That(access.Reason, Is.EqualTo("not yours"));

            // A denial states a remedy rather than only that it is unavailable.
            Assert.That(access.Remedy.Permission, Is.EqualTo("Tenant read"));
            Assert.That(access.Remedy.Audience, Is.EqualTo("an operator"));
        });
    }

    [Test]
    public async Task A_refused_anonymous_caller_is_asked_to_sign_in_rather_than_denied()
    {
        // The measured defect (issue #1854): a signed-out visitor was told that
        // tenancy was "not available for your account" when the honest answer was
        // "sign in". The server refuses an anonymous caller and an authenticated
        // but unauthorized one identically, so only the credential state can tell
        // them apart.
        _client.Throws = new LatticeAuthorizationDeniedException("not yours");

        var access = await CreateAnonymous().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task A_caller_with_no_credential_is_asked_to_sign_in()
    {
        _client.Throws = new RpcException(new Status(StatusCode.Unauthenticated, "sign in"));

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task An_unreachable_server_is_denied_not_reported_as_an_absent_capability()
    {
        // StatusCode.Unavailable is a transient transport failure. Hiding the
        // surface on it would make a reconnect look like an uninstall.
        _client.Throws = new RpcException(new Status(StatusCode.Unavailable, "connection refused"));

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task An_unconfigured_endpoint_is_denied_rather_than_throwing()
    {
        _client.Throws = new InvalidOperationException("The explorer is not configured with an endpoint yet.");

        var access = await Create().ProbeAsync();

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public void The_gate_form_rejects_a_null_context() =>
        Assert.That(
            async () => await Create().ProbeAsync(null!),
            Throws.ArgumentNullException);

    [Test]
    public async Task The_gate_form_reports_unavailable_when_the_host_scope_is_inactive()
    {
        var host = PluginTestHost.Create();
        host.State.Tenant = ExplorerPluginTenantScope.Inactive;
        var context = host.Contexts.Create("tenancy");

        var access = await Create().ProbeAsync(context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(_client.CurrentTenantCallCount, Is.Zero);
        });
    }

    [Test]
    public async Task The_gate_form_probes_the_cluster_when_the_host_scope_is_active()
    {
        var host = PluginTestHost.Create();
        host.State.Tenant = new ExplorerPluginTenantScope(
            IsActive: true,
            ActiveTenantId: SampleTenant.TenantId,
            ExplorerPluginTenantVisibility.ActiveTenant);
        var context = host.Contexts.Create("tenancy");

        var access = await Create().ProbeAsync(context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(_client.CurrentTenantCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_gate_form_reports_unavailable_for_an_absent_add_on_on_an_active_host_scope()
    {
        _client.Throws = new TenancyUnavailableException();
        var host = PluginTestHost.Create();
        host.State.Tenant = new ExplorerPluginTenantScope(
            IsActive: true,
            ActiveTenantId: SampleTenant.TenantId,
            ExplorerPluginTenantVisibility.ActiveTenant);
        var context = host.Contexts.Create("tenancy");

        var access = await Create().ProbeAsync(context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
    }

    [Test]
    public void The_unavailable_exception_carries_a_message_on_every_shape() =>
        Assert.Multiple(() =>
        {
            Assert.That(new TenancyUnavailableException().Message, Is.Not.Empty);
            Assert.That(new TenancyUnavailableException("gone").Message, Is.EqualTo("gone"));
            Assert.That(
                new TenancyUnavailableException("gone", new InvalidOperationException()).InnerException,
                Is.InstanceOf<InvalidOperationException>());
        });
}
