using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsControlTenancyTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp()
    {
        _h = new AppsControlHarness();
        _h.Tenants.Tenant = AppsControlHarness.Acme;
    }

    [Test]
    public async Task Lifecycle_verbs_run_in_the_active_tenant()
    {
        _h.Pipeline.EnableAsync(AppsControlHarness.Acme, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Enable, AppRegistryLifecycleState.Enabled));

        var result = await _h.Control.EnableAsync(AppsControlHarness.Slug);

        Assert.That(result.State, Is.EqualTo(AppLifecycleState.Enabled));
        await _h.Pipeline.DidNotReceive().EnableAsync(TenantId.Default, Arg.Any<AppSlug>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Asynchronous_tenant_resolution_is_used_when_no_warm_path_exists()
    {
        _h.Tenants.Synchronous = false;
        _h.RegistryLists();

        await _h.Control.ListAsync();

        Assert.That(_h.Tenants.AsyncResolutions, Is.EqualTo(1));
        _ = _h.Registry.Received(1).ListForTenantAsync(AppsControlHarness.Acme, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InstallAsync_composes_at_entry_and_stores_tenant_local_scopes()
    {
        _h.SourceResolves();
        _h.RegistryHas(null);
        AppRegistryInstallRequest? captured = null;
        _h.Registry.InstallAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed, tenant: AppsControlHarness.Acme)));

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest(
            scopes:
            [
                new AppExceptionScope { App = "billing", Tree = "ledger" },
                new AppExceptionScope { AdoptedTreeId = "legacy-contacts", Kind = LatticeScopeKind.Key, KeyOrPrefix = "k1" },
            ]));

        Assert.That(captured!.Tenant, Is.EqualTo(AppsControlHarness.Acme));
        Assert.That(captured.Ceiling.ApprovedExceptionScopes, Is.EqualTo(new[]
        {
            LatticeScope.Tree("a/billing/ledger"),
            LatticeScope.Key("legacy-contacts", "k1"),
        }));
        await _h.Registry.Received(1).GetAsync(AppsControlHarness.Acme, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>());
    }

    [TestCase("t/other/x")]
    [TestCase("t/acme/x")]
    [TestCase("t/malformed")]
    [TestCase("sys-app-registry")]
    [TestCase("_lattice_policy")]
    [TestCase("a/billing/ledger")]
    [TestCase("*")]
    [TestCase(" ")]
    public void Adopted_ids_outside_the_tenant_namespace_are_rejected_before_authorization(string adopted)
    {
        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(
            AppsControlHarness.InstallRequest(scopes: new AppExceptionScope { AdoptedTreeId = adopted })));

        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public void Denying_tenant_resolution_fails_closed_before_authorization()
    {
        _h.Tenants.Tenant = default;

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(() => _h.Control.ListAsync());
        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(() => _h.Control.EnableAsync(AppsControlHarness.Slug));
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public async Task ListAsync_never_echoes_records_of_another_tenant()
    {
        _h.RegistryLists(
            AppsControlHarness.Record(AppRegistryLifecycleState.Installed, tenant: AppsControlHarness.Acme, slug: "mine"),
            AppsControlHarness.Record(AppRegistryLifecycleState.Installed, tenant: TenantId.Parse("other"), slug: "theirs"));

        var catalog = await _h.Control.ListAsync();

        Assert.That(catalog.Apps.Select(a => a.Slug), Is.EqualTo(new[] { "mine" }));
    }

    [Test]
    public async Task GetConsentAsync_echoes_app_local_names_under_a_tenant()
    {
        var ceiling = new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.Read,
            ApprovedExceptionScopes = [LatticeScope.Tree("a/billing/ledger"), LatticeScope.Tree("t/acme/legacy")],
        };
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed, tenant: AppsControlHarness.Acme, ceiling: ceiling));

        var report = (await _h.Control.GetConsentAsync(AppsControlHarness.Slug))!;

        Assert.That(report.Ceiling.ApprovedExceptionScopes, Is.EqualTo(new[]
        {
            new AppExceptionScope { App = "billing", Tree = "ledger" },
            new AppExceptionScope { AdoptedTreeId = "legacy" },
        }));
    }
}
