using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsControlInstallTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    [Test]
    public async Task InstallAsync_installs_from_source_with_source_provenance()
    {
        _h.SourceResolves();
        _h.RegistryHas(null);
        AppRegistryInstallRequest? captured = null;
        _h.Registry.InstallAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        var result = await _h.Control.InstallAsync(AppsControlHarness.InstallRequest(
            scopes: new AppExceptionScope { App = "billing", Tree = "ledger" }));

        Assert.That(result, Is.EqualTo(new AppLifecycleResult
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            State = AppLifecycleState.Installed,
            Changed = true,
        }));
        Assert.That(captured, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(captured!.Tenant, Is.EqualTo(TenantId.Default));
            Assert.That(captured.Identity.Slug, Is.EqualTo(AppsControlHarness.AppSlugValue));
            Assert.That(captured.Identity.Version, Is.EqualTo(AppsControlHarness.V(AppsControlHarness.Version)));
            Assert.That(captured.Identity.Provenance.Publisher, Is.EqualTo("contoso"));
            Assert.That(captured.Ceiling.AllowedOperations, Is.EqualTo(LatticeOperation.Read));
            Assert.That(captured.Ceiling.ApprovedExceptionScopes, Is.EqualTo(new[] { LatticeScope.Tree("a/billing/ledger") }));
            Assert.That(captured.RoleBindings, Is.EqualTo(new[] { AppRoleBinding.Create("reader", "g-readers") }));
        });
        await _h.Pipeline.DidNotReceiveWithAnyArgs().EnableAsync(default, default, default);
    }

    [Test]
    public async Task InstallAsync_different_version_of_enabled_app_upgrades_and_reconciles()
    {
        _h.SourceResolves(AppsControlHarness.Manifest(AppsControlHarness.OtherVersion));
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled, AppsControlHarness.OtherVersion)));
        _h.Pipeline.ReconcileAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, version: AppsControlHarness.OtherVersion));

        var result = await _h.Control.InstallAsync(AppsControlHarness.InstallRequest(AppsControlHarness.OtherVersion));

        Assert.That(result.Version, Is.EqualTo(AppsControlHarness.OtherVersion));
        Assert.That(result.State, Is.EqualTo(AppLifecycleState.Enabled));
        await _h.Registry.DidNotReceiveWithAnyArgs().InstallAsync(default!, default);
        await _h.Pipeline.Received(1).ReconcileAsync(TenantId.Default, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InstallAsync_different_version_of_installed_app_upgrades_without_reconcile()
    {
        _h.SourceResolves(AppsControlHarness.Manifest(AppsControlHarness.OtherVersion));
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Disabled));
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Disabled, AppsControlHarness.OtherVersion)));

        var result = await _h.Control.InstallAsync(AppsControlHarness.InstallRequest(AppsControlHarness.OtherVersion));

        Assert.That(result.State, Is.EqualTo(AppLifecycleState.Disabled));
        await _h.Pipeline.DidNotReceiveWithAnyArgs().ReconcileAsync(default, default, default);
    }

    [Test]
    public void InstallAsync_upgrade_reapply_failure_throws_noting_the_recorded_upgrade()
    {
        _h.SourceResolves(AppsControlHarness.Manifest(AppsControlHarness.OtherVersion));
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled, AppsControlHarness.OtherVersion)));
        _h.Pipeline.ReconcileAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, AppActivationFailure.CeilingExceeded));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => _h.Control.InstallAsync(AppsControlHarness.InstallRequest(AppsControlHarness.OtherVersion)));
        Assert.That(ex!.Message, Does.StartWith("The upgrade was recorded").And.Contain("CeilingExceeded"));
    }

    [Test]
    public void InstallAsync_same_version_already_installed_throws_invalid_operation()
    {
        _h.SourceResolves();
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));
        _h.Registry.InstallAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Rejected(AppRegistryTransitionError.AlreadyInstalled));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.InstallAsync(AppsControlHarness.InstallRequest()));
        Assert.That(ex!.Message, Does.Contain("already installed"));
    }

    [Test]
    public async Task InstallAsync_after_uninstall_installs_afresh()
    {
        _h.SourceResolves();
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Uninstalled, AppsControlHarness.OtherVersion));
        _h.Registry.InstallAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        var result = await _h.Control.InstallAsync(AppsControlHarness.InstallRequest());

        Assert.That(result.State, Is.EqualTo(AppLifecycleState.Installed));
        await _h.Registry.DidNotReceiveWithAnyArgs().UpgradeAsync(default!, default);
    }

    [Test]
    public void InstallAsync_source_not_found_throws_key_not_found_without_mutation()
    {
        _h.SourceReturns(AppSourceResult.NotFound(AppsControlHarness.AppSlugValue));

        Assert.ThrowsAsync<KeyNotFoundException>(() => _h.Control.InstallAsync(AppsControlHarness.InstallRequest()));
        Assert.That(_h.Registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void InstallAsync_invalid_source_manifest_throws_invalid_operation()
    {
        _h.SourceReturns(AppSourceResult.InvalidManifest(
            AppsControlHarness.AppSlugValue, [new AppManifestError("bad", "$", "Broken manifest.")]));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.InstallAsync(AppsControlHarness.InstallRequest()));
        Assert.That(ex!.Message, Does.Contain("InvalidManifest").And.Contain("Broken manifest"));
        Assert.That(_h.Registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void InstallAsync_binding_to_undeclared_role_throws_argument_without_mutation()
    {
        _h.SourceResolves();
        var request = AppsControlHarness.InstallRequest() with
        {
            RoleBindings = [new AppRoleBindingDescriptor { RoleName = "admin", GroupId = "g" }],
        };

        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(request));
        Assert.That(_h.Registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void InstallAsync_rejects_invalid_requests_before_authorization()
    {
        var valid = AppsControlHarness.InstallRequest();
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => _h.Control.InstallAsync(null!));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(valid with { Slug = "NOPE" }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(valid with { Version = "1" }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(valid with { Ceiling = null! }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(valid with
            {
                RoleBindings = [new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "" }],
            }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(valid with
            {
                RoleBindings =
                [
                    new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "a" },
                    new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "b" },
                ],
            }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(
                AppsControlHarness.InstallRequest(scopes: new AppExceptionScope { AdoptedTreeId = "sys-secrets" })));
        });
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public async Task InstallAsync_default_role_bindings_install_with_no_bindings()
    {
        _h.SourceResolves();
        _h.RegistryHas(null);
        AppRegistryInstallRequest? captured = null;
        _h.Registry.InstallAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest() with { RoleBindings = default });

        Assert.That(captured!.RoleBindings, Is.Empty);
    }
}
