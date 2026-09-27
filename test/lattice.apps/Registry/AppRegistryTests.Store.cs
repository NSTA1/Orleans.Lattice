namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Request validation, optimistic-concurrency retry, and the read surface of
/// <see cref="AppRegistry"/>.
/// </summary>
public sealed partial class AppRegistryTests
{
    // ---- Validation -------------------------------------------------------

    [Test]
    public void InstallAsync_null_request_throws()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        Assert.That(() => registry.InstallAsync(null!), Throws.ArgumentNullException);
        Assert.That(() => registry.UpgradeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void InstallAsync_rejects_an_uninitialised_tenant_slug_or_version()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        var valid = AppRegistryTestData.Request();

        Assert.That(() => registry.InstallAsync(valid with { Tenant = default }), Throws.ArgumentException);
        Assert.That(() => registry.InstallAsync(valid with { Identity = valid.Identity with { Slug = default } }), Throws.ArgumentException);
        Assert.That(() => registry.InstallAsync(valid with { Identity = valid.Identity with { Version = default } }), Throws.ArgumentException);
    }

    [Test]
    public void InstallAsync_rejects_missing_ceiling_bindings_or_provenance()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        var valid = AppRegistryTestData.Request();

        Assert.That(() => registry.InstallAsync(valid with { Ceiling = null! }), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => registry.InstallAsync(valid with { RoleBindings = null! }), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => registry.InstallAsync(valid with { Identity = valid.Identity with { Provenance = null! } }), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => registry.InstallAsync(valid with { Ceiling = new AppCapabilityCeiling { ApprovedExceptionScopes = null! } }), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void InstallAsync_rejects_duplicate_or_malformed_role_bindings()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        var duplicate = AppRegistryTestData.Request(bindings: new[]
        {
            AppRoleBinding.Create("reader", "readers"),
            AppRoleBinding.Create("reader", "others"),
        });
        var nullEntry = AppRegistryTestData.Request(bindings: new AppRoleBinding[] { null! });
        var emptyGroup = AppRegistryTestData.Request(bindings: new[] { new AppRoleBinding { RoleName = "reader", GroupId = "" } });

        Assert.That(() => registry.InstallAsync(duplicate), Throws.ArgumentException.With.Message.Contains("reader"));
        Assert.That(() => registry.InstallAsync(nullEntry), Throws.ArgumentException);
        Assert.That(() => registry.InstallAsync(emptyGroup), Throws.ArgumentException);
    }

    [Test]
    public void Validation_precedes_authorization()
    {
        var gate = RecordingAccessGate.AllowAll();
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore(), gate);

        Assert.That(() => registry.InstallAsync(AppRegistryTestData.Request() with { Tenant = default }), Throws.ArgumentException);
        Assert.That(() => registry.EnableAsync(default, AppRegistryTestData.Slug), Throws.ArgumentException);
        Assert.That(() => registry.DisableAsync(TenantId.Default, default), Throws.ArgumentException);
        Assert.That(gate.Requests, Is.Empty);
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var store = new InMemoryAppRegistryStore();
        var authorizer = new AppInstallAuthorizer(RecordingAccessGate.AllowAll());
        var options = Microsoft.Extensions.Options.Options.Create(new Orleans.Configuration.ClusterOptions());

        Assert.That(() => new AppRegistry(null!, authorizer, options), Throws.ArgumentNullException);
        Assert.That(() => new AppRegistry(store, null!, options), Throws.ArgumentNullException);
        Assert.That(() => new AppRegistry(store, authorizer, null!), Throws.ArgumentNullException);
    }

    // ---- Optimistic concurrency ------------------------------------------

    [Test]
    public async Task A_lost_conditional_write_is_re_decided_against_the_competing_writers_record()
    {
        var store = new InMemoryAppRegistryStore();
        store.Seed(DefaultKey, AppRegistryTestData.Record(AppRegistryLifecycleState.Installed));
        var registry = AppRegistryTestData.CreateRegistry(store);

        // A competing writer enables the app between this call's read and its write, once.
        var interposed = false;
        store.BeforeSet = key =>
        {
            if (!interposed)
            {
                interposed = true;
                store.Seed(key, AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled) with { Revision = 2 });
            }
        };

        var result = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Changed, Is.False, "the re-read sees the app already enabled, so the retry is a no-op");
        Assert.That(result.Record!.Revision, Is.EqualTo(2));
        Assert.That(store.SetAttempts, Is.EqualTo(1));
        Assert.That(store.AppliedWrites, Is.EqualTo(0));
    }

    [Test]
    public async Task A_lost_install_race_is_reported_as_already_installed()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        var interposed = false;
        store.BeforeSet = key =>
        {
            if (!interposed)
            {
                interposed = true;
                store.Seed(key, AppRegistryTestData.Record(AppRegistryLifecycleState.Installed));
            }
        };

        var result = await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.AlreadyInstalled));
    }

    [Test]
    public async Task Sustained_contention_exhausts_the_retry_budget_as_a_concurrency_conflict()
    {
        var store = new InMemoryAppRegistryStore();
        store.Seed(DefaultKey, AppRegistryTestData.Record(AppRegistryLifecycleState.Installed));
        var registry = AppRegistryTestData.CreateRegistry(store);
        store.BeforeSet = key => store.Seed(key, AppRegistryTestData.Record(AppRegistryLifecycleState.Installed));

        var result = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.ConcurrencyConflict));
        Assert.That(result.Message, Does.Contain(AppRegistry.MaxTransitionAttempts.ToString()));
        Assert.That(store.SetAttempts, Is.EqualTo(AppRegistry.MaxTransitionAttempts));
        Assert.That(store.AppliedWrites, Is.EqualTo(0));
    }

    // ---- Reads -----------------------------------------------------------

    [Test]
    public async Task GetAsync_returns_the_record_or_null()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(await registry.GetAsync(TenantId.Default, AppRegistryTestData.Slug), Is.SameAs(store.Peek(DefaultKey)));
        Assert.That(await registry.GetAsync(AppRegistryTestData.Acme, AppRegistryTestData.Slug), Is.Null);
        Assert.That(() => registry.GetAsync(default, AppRegistryTestData.Slug), Throws.ArgumentException);
    }

    [Test]
    public async Task ListForTenantAsync_is_a_prefix_scan_that_excludes_other_tenants()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        var acme2 = TenantId.Parse("acme2");
        await registry.InstallAsync(AppRegistryTestData.Request(tenant: AppRegistryTestData.Acme, slug: AppSlug.Parse("zeta")));
        await registry.InstallAsync(AppRegistryTestData.Request(tenant: AppRegistryTestData.Acme, slug: AppSlug.Parse("alpha")));
        await registry.InstallAsync(AppRegistryTestData.Request(tenant: acme2, slug: AppSlug.Parse("alpha")));
        await registry.InstallAsync(AppRegistryTestData.Request(slug: AppSlug.Parse("alpha")));

        var acme = await registry.ListForTenantAsync(AppRegistryTestData.Acme).ToListAsync();
        var all = await registry.ListAsync().ToListAsync();

        Assert.That(acme.Select(r => r.Slug.Value), Is.EqualTo(new[] { "alpha", "zeta" }));
        Assert.That(acme.All(r => r.Tenant == AppRegistryTestData.Acme), Is.True, "acme2 shares the text prefix but not the key segment");
        Assert.That(all, Has.Count.EqualTo(4));
        Assert.That(() => registry.ListForTenantAsync(default), Throws.ArgumentException, "validated eagerly");
    }
}
