using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// <see cref="ILatticeAppRoleBindings.UpdateRoleBindingsAsync"/> on the in-process facade: it
/// authorizes before reading anything, pins the change to the installed version and the record
/// revision it read, accepts only declared roles bound to a group, keeps the consent, and
/// re-applies an enabled app so its compiled rules are replaced.
/// </summary>
[TestFixture]
public sealed class LatticeAppsControlRoleBindingsTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    private static AppRoleBindingsUpdate Update(string version = AppsControlHarness.Version, params (string Role, string Group)[] bindings) =>
        new()
        {
            Slug = AppsControlHarness.Slug,
            Version = version,
            RoleBindings = [.. (bindings.Length == 0 ? [("reader", "g-new-readers")] : bindings)
                .Select(binding => new AppRoleBindingDescriptor { RoleName = binding.Role, GroupId = binding.Group })],
        };

    [Test]
    public void Denied_caller_is_refused_before_any_engine_access()
    {
        _h.Gate.Decision = LatticeAccessDecision.Deny("no");

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));

        Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.AppInstall));
        Assert.That(_h.Gate.Requests.Single().TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
        _h.AssertEngineUntouched();
    }

    [Test]
    public void Key_filtered_allow_is_refused_before_any_engine_access()
    {
        _h.Gate.Decision = LatticeAccessDecision.Filtered(static _ => true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));
        _h.AssertEngineUntouched();
    }

    [Test]
    public void Invalid_requests_are_rejected_before_authorization()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => _h.Control.UpdateRoleBindingsAsync(null!));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateRoleBindingsAsync(Update() with { Slug = "x" }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateRoleBindingsAsync(Update() with { Version = "latest" }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateRoleBindingsAsync(Update(bindings: [("reader", "")])),
                "a binding must name a group: bindings are group-only");
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateRoleBindingsAsync(Update(bindings: [("", "g-readers")])));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateRoleBindingsAsync(
                Update(bindings: [("reader", "g-a"), ("reader", "g-b")])), "a role binds to exactly one group");
        });
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public void A_version_other_than_the_installed_one_fails_without_mutation()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateRoleBindingsAsync(Update(AppsControlHarness.OtherVersion)));

        Assert.That(ex!.Message, Does.Contain("pinned to the installed version"));
        _ = _h.Registry.DidNotReceiveWithAnyArgs().UpgradeAsync(default!, default);
        _ = _h.Pipeline.DidNotReceiveWithAnyArgs().ReconcileAsync(default, default, default);
    }

    [Test]
    public void An_app_that_is_not_installed_throws_key_not_found()
    {
        _h.RegistryHas(null);
        Assert.ThrowsAsync<KeyNotFoundException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));

        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Uninstalled));
        Assert.ThrowsAsync<KeyNotFoundException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));
        _ = _h.Registry.DidNotReceiveWithAnyArgs().UpgradeAsync(default!, default);
    }

    [Test]
    public void A_role_the_installed_manifest_does_not_declare_is_rejected_without_mutation()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.SourceResolves();

        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateRoleBindingsAsync(Update(bindings: [("auditor", "g-auditors")])));
        _ = _h.Registry.DidNotReceiveWithAnyArgs().UpgradeAsync(default!, default);
    }

    [Test]
    public void A_ceiling_not_consented_for_the_installed_version_must_be_re_consented_first()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled) with { CeilingVersion = AppsControlHarness.V("0.9.0") });
        _h.SourceResolves();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));

        Assert.That(ex!.Message, Does.Contain("re-consent"));
        _ = _h.Registry.DidNotReceiveWithAnyArgs().UpgradeAsync(default!, default);
    }

    [Test]
    public async Task A_disabled_app_has_its_bindings_replaced_its_consent_kept_and_is_never_enabled()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Disabled) with { Revision = 5 };
        _h.RegistryHas(current);
        _h.SourceResolves();
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(_ => AppsControlHarness.Succeeded(current with { RoleBindings = captured!.RoleBindings, Revision = 6 }));

        var report = await _h.Control.UpdateRoleBindingsAsync(Update(bindings: [("reader", "g-new-readers"), ("writer", "g-writers")]));

        Assert.Multiple(() =>
        {
            Assert.That(captured!.Identity.Version, Is.EqualTo(current.Version));
            Assert.That(captured.Identity.Provenance, Is.EqualTo(current.Provenance));
            Assert.That(captured.Ceiling, Is.SameAs(current.Ceiling), "the consented ceiling is kept, never re-supplied");
            Assert.That(captured.BridgeConsent, Is.Null, "the consented bridge grants are kept");
            Assert.That(captured.ExpectedVersion, Is.EqualTo(current.Version));
            Assert.That(captured.ExpectedRevision, Is.EqualTo(5), "pinned to the revision it read");
            Assert.That(captured.RoleBindings, Is.EqualTo(new[] { AppRoleBinding.Create("reader", "g-new-readers"), AppRoleBinding.Create("writer", "g-writers") }));
            Assert.That(report.Slug, Is.EqualTo(AppsControlHarness.Slug));
            Assert.That(report.Version, Is.EqualTo(AppsControlHarness.Version));
            Assert.That(report.State, Is.EqualTo(AppLifecycleState.Disabled));
            Assert.That(report.RoleBindings, Is.EqualTo(new[]
            {
                new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-new-readers" },
                new AppRoleBindingDescriptor { RoleName = "writer", GroupId = "g-writers" },
            }));
        });
        await _h.Registry.DidNotReceiveWithAnyArgs().EnableAsync(default, default, default);
        await _h.Pipeline.DidNotReceiveWithAnyArgs().EnableAsync(default, default, default);
        await _h.Pipeline.DidNotReceiveWithAnyArgs().ReconcileAsync(default, default, default);
    }

    [Test]
    public async Task An_empty_replacement_unbinds_every_role()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Installed);
        _h.RegistryHas(current);
        _h.SourceResolves();
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(_ => AppsControlHarness.Succeeded(current with { RoleBindings = captured!.RoleBindings }));

        var report = await _h.Control.UpdateRoleBindingsAsync(Update() with { RoleBindings = [] });

        Assert.That(captured!.RoleBindings, Is.Empty);
        Assert.That(report.RoleBindings, Is.Empty);
    }

    [Test]
    public async Task An_enabled_app_is_re_applied_so_its_compiled_rules_are_replaced()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Enabled);
        _h.RegistryHas(current);
        _h.SourceResolves();
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(current));
        _h.Pipeline.ReconcileAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled));

        var report = await _h.Control.UpdateRoleBindingsAsync(Update());

        Assert.That(report.State, Is.EqualTo(AppLifecycleState.Enabled));
        Received.InOrder(() =>
        {
            _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>());
            _h.Pipeline.ReconcileAsync(TenantId.Default, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public void A_failed_re_application_throws_noting_the_bindings_were_recorded()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Enabled);
        _h.RegistryHas(current);
        _h.SourceResolves();
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(current));
        _h.Pipeline.ReconcileAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, AppActivationFailure.RulePersistenceFailed));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));

        Assert.That(ex!.Message, Does.StartWith("The role bindings were recorded").And.Contain("RulePersistenceFailed"));
    }

    [Test]
    public async Task A_lost_race_is_a_failed_precondition_and_nothing_is_re_applied()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.SourceResolves();
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Rejected(AppRegistryTransitionError.ConcurrencyConflict, "Changed concurrently."));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));

        Assert.That(ex!.Message, Does.Contain("ConcurrencyConflict"));
        await _h.Pipeline.DidNotReceiveWithAnyArgs().ReconcileAsync(default, default, default);
    }

    [Test]
    public void A_source_that_no_longer_offers_the_installed_version_fails_without_mutation()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.SourceReturns(AppSourceResult.NotFound(AppsControlHarness.AppSlugValue));

        Assert.ThrowsAsync<KeyNotFoundException>(() => _h.Control.UpdateRoleBindingsAsync(Update()));
        _ = _h.Registry.DidNotReceiveWithAnyArgs().UpgradeAsync(default!, default);
    }

    [Test]
    public async Task UpdateConsentAsync_pins_the_revision_it_read_so_it_cannot_roll_back_a_rebinding()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Disabled) with { Revision = 9 };
        _h.RegistryHas(current);
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(current));

        await _h.Control.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
        });

        Assert.That(captured!.ExpectedRevision, Is.EqualTo(9));
    }

    [Test]
    public void AddLatticeAppsApi_registers_the_control_facade_as_the_role_bindings_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IAppRegistry>());
        services.AddSingleton(Substitute.For<IAppSource>());
        services.AddSingleton(Substitute.For<IAppActivationPipeline>());
        services.AddSingleton(Substitute.For<ILatticeAccessGate>());
        services.AddSingleton(Substitute.For<ITenantContextResolver>());

        services.AddLatticeAppsApi();
        services.AddLatticeAppsApi();

        Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeAppRoleBindings)), Is.EqualTo(1));
        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeAppRoleBindings>(), Is.SameAs(provider.GetRequiredService<ILatticeAppsControl>()));
    }

    [Test]
    public void AddLatticeAppsApi_serves_role_bindings_beside_a_host_supplied_control_facade()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IAppRegistry>());
        services.AddSingleton(Substitute.For<IAppSource>());
        services.AddSingleton(Substitute.For<IAppActivationPipeline>());
        services.AddSingleton(Substitute.For<ILatticeAccessGate>());
        services.AddSingleton(Substitute.For<ITenantContextResolver>());
        var custom = Substitute.For<ILatticeAppsControl>();
        services.AddSingleton(custom);

        services.AddLatticeAppsApi();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeAppsControl>(), Is.SameAs(custom));
        Assert.That(provider.GetRequiredService<ILatticeAppRoleBindings>(), Is.TypeOf<LatticeAppsControl>());
    }
}
