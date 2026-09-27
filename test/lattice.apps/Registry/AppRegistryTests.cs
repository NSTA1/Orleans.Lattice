using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppRegistry"/> over an in-memory store: every lifecycle
/// transition, rejected transitions, the per-(app, version) ceiling pin, the recorded
/// isolation context, provenance and timestamps.
/// </summary>
[TestFixture]
public sealed partial class AppRegistryTests
{
    private static readonly string DefaultKey = AppRegistryTreeNames.ComposeKey(TenantId.Default, AppRegistryTestData.Slug);

    [Test]
    public async Task InstallAsync_records_identity_isolation_ceiling_bindings_and_the_installed_state()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        var provenance = new AppProvenance { Source = "in-image", Publisher = "contoso", Reference = "notes.json" };
        var ceiling = new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.Read,
            ApprovedExceptionScopes = new[] { LatticeScope.Tree("legacy-notes") },
        };
        var request = AppRegistryTestData.Request(ceiling: ceiling) with
        {
            Identity = new AppIdentity { Slug = AppRegistryTestData.Slug, Version = AppRegistryTestData.V1, Provenance = provenance },
        };

        var result = await registry.InstallAsync(request);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Changed, Is.True);
        var record = result.Record!;
        Assert.That(record.State, Is.EqualTo(AppRegistryLifecycleState.Installed));
        Assert.That(record.Slug, Is.EqualTo(AppRegistryTestData.Slug));
        Assert.That(record.Version, Is.EqualTo(AppRegistryTestData.V1));
        Assert.That(record.Provenance, Is.EqualTo(provenance));
        Assert.That(record.Isolation, Is.EqualTo(new AppIsolationContext { Tenant = TenantId.Default, ClusterId = AppRegistryTestData.ClusterId }));
        Assert.That(record.Tenant, Is.EqualTo(TenantId.Default));
        Assert.That(record.Ceiling, Is.SameAs(ceiling));
        Assert.That(record.CeilingVersion, Is.EqualTo(AppRegistryTestData.V1));
        Assert.That(record.IsCeilingPinnedToVersion, Is.True);
        Assert.That(record.RoleBindings, Is.EqualTo(request.RoleBindings));
        Assert.That(record.RoleBindings, Is.Not.SameAs(request.RoleBindings), "bindings are copied, not aliased");
        Assert.That(record.Revision, Is.EqualTo(1));
        Assert.That(record.InstalledAtUtc, Is.EqualTo(AppRegistryTestData.Start));
        Assert.That(record.StateChangedAtUtc, Is.EqualTo(AppRegistryTestData.Start));
        Assert.That(record.ConsentedAtUtc, Is.EqualTo(AppRegistryTestData.Start));
        Assert.That(store.Peek(DefaultKey), Is.SameAs(record));
    }

    [Test]
    public async Task InstallAsync_keys_the_record_by_tenant_and_slug()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);

        await registry.InstallAsync(AppRegistryTestData.Request(tenant: AppRegistryTestData.Acme));

        Assert.That(store.Peek("acme/notes"), Is.Not.Null);
        Assert.That(store.Peek(DefaultKey), Is.Null);
        Assert.That(store.Peek("acme/notes")!.Isolation.Tenant, Is.EqualTo(AppRegistryTestData.Acme));
    }

    [Test]
    public async Task Full_lifecycle_install_enable_disable_enable_uninstall_advances_state_and_revision()
    {
        var store = new InMemoryAppRegistryStore();
        var time = new ManualTimeProvider(AppRegistryTestData.Start);
        var registry = AppRegistryTestData.CreateRegistry(store, time: time);
        var tenant = TenantId.Default;
        var slug = AppRegistryTestData.Slug;

        await registry.InstallAsync(AppRegistryTestData.Request());
        time.Advance(TimeSpan.FromMinutes(1));
        var enabled = await registry.EnableAsync(tenant, slug);
        time.Advance(TimeSpan.FromMinutes(1));
        var disabled = await registry.DisableAsync(tenant, slug);
        time.Advance(TimeSpan.FromMinutes(1));
        var reenabled = await registry.EnableAsync(tenant, slug);
        time.Advance(TimeSpan.FromMinutes(1));
        var uninstalled = await registry.UninstallAsync(tenant, slug);

        Assert.That(enabled.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
        Assert.That(disabled.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Disabled));
        Assert.That(reenabled.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
        Assert.That(uninstalled.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Uninstalled));
        Assert.That(
            new[] { enabled, disabled, reenabled, uninstalled }.Select(r => r.Record!.Revision),
            Is.EqualTo(new long[] { 2, 3, 4, 5 }));
        Assert.That(uninstalled.Record!.StateChangedAtUtc, Is.EqualTo(AppRegistryTestData.Start + TimeSpan.FromMinutes(4)));
        Assert.That(uninstalled.Record!.InstalledAtUtc, Is.EqualTo(AppRegistryTestData.Start), "state changes keep the install time");
        Assert.That(uninstalled.Record!.Ceiling, Is.EqualTo(enabled.Record!.Ceiling), "state changes carry the consent over");
        Assert.That(store.Peek(DefaultKey)!.State, Is.EqualTo(AppRegistryLifecycleState.Uninstalled), "uninstall retains the record");
    }

    [Test]
    public async Task InstallAsync_after_uninstall_reinstalls_with_a_fresh_consent_and_a_non_regressing_revision()
    {
        var store = new InMemoryAppRegistryStore();
        var time = new ManualTimeProvider(AppRegistryTestData.Start);
        var registry = AppRegistryTestData.CreateRegistry(store, time: time);
        await registry.InstallAsync(AppRegistryTestData.Request());
        await registry.UninstallAsync(TenantId.Default, AppRegistryTestData.Slug);
        time.Advance(TimeSpan.FromHours(1));

        var ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read);
        var result = await registry.InstallAsync(AppRegistryTestData.Request(version: AppRegistryTestData.V2, ceiling: ceiling));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Installed));
        Assert.That(result.Record.Revision, Is.EqualTo(3));
        Assert.That(result.Record.Version, Is.EqualTo(AppRegistryTestData.V2));
        Assert.That(result.Record.Ceiling, Is.SameAs(ceiling));
        Assert.That(result.Record.CeilingVersion, Is.EqualTo(AppRegistryTestData.V2));
        Assert.That(result.Record.InstalledAtUtc, Is.EqualTo(AppRegistryTestData.Start + TimeSpan.FromHours(1)));
    }

    [TestCase(AppRegistryLifecycleState.Installed)]
    [TestCase(AppRegistryLifecycleState.Enabled)]
    [TestCase(AppRegistryLifecycleState.Disabled)]
    public async Task InstallAsync_over_a_live_install_is_rejected_and_writes_nothing(AppRegistryLifecycleState state)
    {
        var store = new InMemoryAppRegistryStore();
        var existing = AppRegistryTestData.Record(state);
        store.Seed(DefaultKey, existing);
        var registry = AppRegistryTestData.CreateRegistry(store);

        var result = await registry.InstallAsync(AppRegistryTestData.Request(version: AppRegistryTestData.V2));

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.AlreadyInstalled));
        Assert.That(result.Changed, Is.False);
        Assert.That(result.Message, Is.Not.Empty);
        Assert.That(result.Record, Is.SameAs(existing));
        Assert.That(store.SetAttempts, Is.EqualTo(0));
    }

    [Test]
    public async Task EnableAsync_on_an_absent_app_is_rejected_as_not_installed()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);

        var result = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.NotInstalled));
        Assert.That(result.Record, Is.Null);
        Assert.That(store.SetAttempts, Is.EqualTo(0));
    }

    [Test]
    public async Task EnableAsync_on_an_uninstalled_app_is_rejected_as_not_installed()
    {
        var store = new InMemoryAppRegistryStore();
        store.Seed(DefaultKey, AppRegistryTestData.Record(AppRegistryLifecycleState.Uninstalled));
        var registry = AppRegistryTestData.CreateRegistry(store);

        var result = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.NotInstalled));
        Assert.That(store.SetAttempts, Is.EqualTo(0));
    }

    [Test]
    public async Task DisableAsync_on_an_installed_but_never_enabled_app_is_an_invalid_transition()
    {
        var store = new InMemoryAppRegistryStore();
        store.Seed(DefaultKey, AppRegistryTestData.Record(AppRegistryLifecycleState.Installed));
        var registry = AppRegistryTestData.CreateRegistry(store);

        var result = await registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.InvalidTransition));
        Assert.That(store.SetAttempts, Is.EqualTo(0));
    }

    [Test]
    public async Task UninstallAsync_on_an_absent_app_is_rejected_as_not_installed()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        var result = await registry.UninstallAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.NotInstalled));
    }

    [TestCase(AppRegistryLifecycleState.Enabled, "enable")]
    [TestCase(AppRegistryLifecycleState.Disabled, "disable")]
    [TestCase(AppRegistryLifecycleState.Uninstalled, "uninstall")]
    public async Task Repeating_a_transition_whose_target_already_holds_is_an_idempotent_no_op(AppRegistryLifecycleState state, string action)
    {
        var store = new InMemoryAppRegistryStore();
        var existing = AppRegistryTestData.Record(state);
        store.Seed(DefaultKey, existing);
        var registry = AppRegistryTestData.CreateRegistry(store);

        var result = action switch
        {
            "enable" => await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug),
            "disable" => await registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug),
            _ => await registry.UninstallAsync(TenantId.Default, AppRegistryTestData.Slug),
        };

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Changed, Is.False);
        Assert.That(result.Record, Is.SameAs(existing));
        Assert.That(store.SetAttempts, Is.EqualTo(0), "a no-op writes nothing");
    }

    [TestCase(AppRegistryLifecycleState.Installed)]
    [TestCase(AppRegistryLifecycleState.Enabled)]
    [TestCase(AppRegistryLifecycleState.Disabled)]
    public async Task UpgradeAsync_pins_the_new_ceiling_to_the_new_version_and_keeps_state(AppRegistryLifecycleState state)
    {
        var store = new InMemoryAppRegistryStore();
        var time = new ManualTimeProvider(AppRegistryTestData.Start);
        var registry = AppRegistryTestData.CreateRegistry(store, time: time);
        await registry.InstallAsync(AppRegistryTestData.Request());
        if (state != AppRegistryLifecycleState.Installed)
        {
            await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        }

        if (state == AppRegistryLifecycleState.Disabled)
        {
            await registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);
        }

        var before = store.Peek(DefaultKey)!;
        time.Advance(TimeSpan.FromDays(1));
        var newCeiling = AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.RangeRead);
        var newBindings = new[] { AppRoleBinding.Create("writer", "writers") };

        var result = await registry.UpgradeAsync(AppRegistryTestData.Request(version: AppRegistryTestData.V2, ceiling: newCeiling, bindings: newBindings));

        var record = result.Record!;
        Assert.That(result.Succeeded && result.Changed, Is.True);
        Assert.That(record.State, Is.EqualTo(state));
        Assert.That(record.Version, Is.EqualTo(AppRegistryTestData.V2));
        Assert.That(record.Ceiling, Is.SameAs(newCeiling), "the upgrade's ceiling replaces the old one, never inherits it");
        Assert.That(record.CeilingVersion, Is.EqualTo(AppRegistryTestData.V2));
        Assert.That(record.RoleBindings, Is.EqualTo(newBindings));
        Assert.That(record.Revision, Is.EqualTo(before.Revision + 1));
        Assert.That(record.InstalledAtUtc, Is.EqualTo(before.InstalledAtUtc));
        Assert.That(record.StateChangedAtUtc, Is.EqualTo(before.StateChangedAtUtc), "an upgrade does not change state");
        Assert.That(record.ConsentedAtUtc, Is.EqualTo(AppRegistryTestData.Start + TimeSpan.FromDays(1)));
        Assert.That(record.Isolation, Is.EqualTo(before.Isolation));
    }

    [Test]
    public async Task UpgradeAsync_with_the_same_version_re_consents_the_ceiling()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        await registry.InstallAsync(AppRegistryTestData.Request());
        var widened = AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete);

        var result = await registry.UpgradeAsync(AppRegistryTestData.Request(ceiling: widened));

        Assert.That(result.Record!.Version, Is.EqualTo(AppRegistryTestData.V1));
        Assert.That(result.Record.Ceiling, Is.SameAs(widened));
    }

    [TestCase(null)]
    [TestCase(AppRegistryLifecycleState.Uninstalled)]
    public async Task UpgradeAsync_without_a_live_install_is_rejected(AppRegistryLifecycleState? state)
    {
        var store = new InMemoryAppRegistryStore();
        if (state is { } s)
        {
            store.Seed(DefaultKey, AppRegistryTestData.Record(s));
        }

        var registry = AppRegistryTestData.CreateRegistry(store);

        var result = await registry.UpgradeAsync(AppRegistryTestData.Request(version: AppRegistryTestData.V2));

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.NotInstalled));
        Assert.That(store.SetAttempts, Is.EqualTo(0));
    }

    [Test]
    public async Task EnableAsync_refuses_a_stored_record_whose_ceiling_is_not_pinned_until_upgrade_re_consents()
    {
        var store = new InMemoryAppRegistryStore();
        store.Seed(DefaultKey, AppRegistryTestData.Record(
            AppRegistryLifecycleState.Installed, version: AppRegistryTestData.V2, ceilingVersion: AppRegistryTestData.V1));
        var registry = AppRegistryTestData.CreateRegistry(store);

        var refused = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        Assert.That(refused.Error, Is.EqualTo(AppRegistryTransitionError.CeilingNotPinned));
        Assert.That(store.SetAttempts, Is.EqualTo(0));

        await registry.UpgradeAsync(AppRegistryTestData.Request(version: AppRegistryTestData.V2));
        var enabled = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(enabled.Succeeded, Is.True);
        Assert.That(enabled.Record!.IsCeilingPinnedToVersion, Is.True);
    }
}
