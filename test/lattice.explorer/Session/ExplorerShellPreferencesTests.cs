using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Detail;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The preference contract itself: declared keys only, scoped per user and
/// cluster, resettable, and falling back with an explanation when a remembered
/// value no longer resolves.
/// </summary>
[TestFixture]
public sealed class ExplorerShellPreferencesTests
{
    private static readonly ExplorerPreferenceKey UnregisteredKey =
        new("feature.unregistered", "something nobody declared");

    private static readonly ExplorerPreferenceKey UserScopedKey =
        new("feature.theme", "your theme", ExplorerPreferenceScope.User);

    private static (ExplorerShellPreferences Preferences, FakeUiPreferenceStore Store, FakeExplorerPreferenceScopeProvider Scope) Create(
        FakeUiPreferenceStore? store = null,
        IExplorerPreferenceCatalog? catalog = null)
    {
        store ??= new FakeUiPreferenceStore();
        var scope = new FakeExplorerPreferenceScopeProvider();
        var preferences = new ExplorerShellPreferences(
            store,
            catalog ?? new ExplorerPreferenceCatalog(),
            scope);

        return (preferences, store, scope);
    }

    [Test]
    public void Constructor_NullStore_Throws()
    {
        Assert.That(
            () => new ExplorerShellPreferences(
                null!,
                new ExplorerPreferenceCatalog(),
                new FakeExplorerPreferenceScopeProvider()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_NullCatalog_Throws()
    {
        Assert.That(
            () => new ExplorerShellPreferences(
                new FakeUiPreferenceStore(),
                null!,
                new FakeExplorerPreferenceScopeProvider()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_NullScope_Throws()
    {
        Assert.That(
            () => new ExplorerShellPreferences(
                new FakeUiPreferenceStore(),
                new ExplorerPreferenceCatalog(),
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Keys_AreTheCatalogsKeys()
    {
        var (preferences, _, _) = Create();

        Assert.That(preferences.Keys, Is.EquivalentTo(ExplorerPreferenceKeys.All));
    }

    [Test]
    public async Task EnsureLoadedAsync_HydratesTheUnderlyingStore()
    {
        var (preferences, _, _) = Create();

        await preferences.EnsureLoadedAsync();

        Assert.That(preferences.IsLoaded, Is.True);
    }

    [Test]
    public async Task SetAsync_ThenGetOrDefault_ReturnsTheStoredValue()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();

        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, string.Empty),
            Is.EqualTo("tenants"));
    }

    [Test]
    public async Task SetAsync_WritesUnderAScopedName()
    {
        var (preferences, store, _) = Create();
        await preferences.EnsureLoadedAsync();

        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        Assert.That(store.Writes.Single(), Does.EndWith("." + ExplorerPreferenceKeys.ActiveArea.Name)
            .And.Not.EqualTo(ExplorerPreferenceKeys.ActiveArea.Name));
    }

    [Test]
    public async Task GetOrDefault_AfterTheScopeChanges_DoesNotSeeThePreviousIdentitysValue()
    {
        var (preferences, _, scope) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        scope.MoveTo("bob", "https://cluster-b");

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"),
            Is.EqualTo("none"));
    }

    [Test]
    public async Task GetOrDefault_AfterReturningToTheOriginalScope_SeesItsValueAgain()
    {
        var (preferences, _, scope) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        scope.MoveTo("bob", "https://cluster-b");
        scope.MoveTo("alice", "https://cluster-a");

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"),
            Is.EqualTo("tenants"));
    }

    [Test]
    public async Task UserScopedKey_SurvivesAClusterSwitch()
    {
        var catalog = new ExplorerPreferenceCatalog();
        catalog.Register(UserScopedKey);
        var (preferences, _, scope) = Create(catalog: catalog);
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(UserScopedKey, "light");

        scope.MoveTo("alice", "https://cluster-b");

        Assert.That(preferences.GetOrDefault(UserScopedKey, "dark"), Is.EqualTo("light"));
    }

    [Test]
    public async Task UserScopedKey_DoesNotSurviveAUserSwitch()
    {
        var catalog = new ExplorerPreferenceCatalog();
        catalog.Register(UserScopedKey);
        var (preferences, _, scope) = Create(catalog: catalog);
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(UserScopedKey, "light");

        scope.MoveTo("bob", "https://cluster-a");

        Assert.That(preferences.GetOrDefault(UserScopedKey, "dark"), Is.EqualTo("dark"));
    }

    [Test]
    public void ScopeChanged_RaisesChanged()
    {
        var (preferences, _, scope) = Create();
        var changes = 0;
        preferences.Changed += () => changes++;

        scope.MoveTo("bob", "https://cluster-b");

        Assert.That(changes, Is.EqualTo(1));
    }

    [Test]
    public void GetOrDefault_UnregisteredKey_Throws()
    {
        var (preferences, _, _) = Create();

        Assert.That(
            () => preferences.GetOrDefault(UnregisteredKey, string.Empty),
            Throws.ArgumentException.With.Message.Contains("not a registered"));
    }

    [Test]
    public void SetAsync_UnregisteredKey_Throws()
    {
        var (preferences, _, _) = Create();

        Assert.That(
            async () => await preferences.SetAsync(UnregisteredKey, "x"),
            Throws.ArgumentException);
    }

    [Test]
    public void GetOrDefault_NullKey_Throws()
    {
        var (preferences, _, _) = Create();

        Assert.That(
            () => preferences.GetOrDefault<string>(null!, string.Empty),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ClearAsync_ForgetsTheValue()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        await preferences.ClearAsync(ExplorerPreferenceKeys.ActiveArea);

        Assert.That(preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"), Is.EqualTo("none"));
    }

    [Test]
    public async Task ResetAsync_ForgetsEveryDeclaredKeyAndAnnounces()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");
        await preferences.SetAsync(ExplorerPreferenceKeys.Selection, "orders");
        await preferences.SetAsync(ExplorerPreferenceKeys.AllTenantsVisible, true);
        var changes = 0;
        preferences.Changed += () => changes++;

        await preferences.ResetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"), Is.EqualTo("none"));
            Assert.That(preferences.GetOrDefault(ExplorerPreferenceKeys.Selection, "none"), Is.EqualTo("none"));
            Assert.That(preferences.GetOrDefault(ExplorerPreferenceKeys.AllTenantsVisible, false), Is.False);
            Assert.That(changes, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ResetAsync_LeavesAnotherScopesValuesAlone()
    {
        var (preferences, _, scope) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        scope.MoveTo("bob", "https://cluster-b");
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "telemetry");
        await preferences.ResetAsync();
        scope.MoveTo("alice", "https://cluster-a");

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"),
            Is.EqualTo("tenants"));
    }

    [Test]
    public async Task ResetAsync_WithNothingStored_IsHarmless()
    {
        var (preferences, _, _) = Create();

        Assert.That(async () => await preferences.ResetAsync(), Throws.Nothing);
        await Task.CompletedTask;
    }

    [Test]
    public async Task Resolve_RememberedAndResolvable_RestoresIt()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        var resolution = preferences.Resolve(
            ExplorerPreferenceKeys.ActiveArea,
            "explore",
            state: true,
            static (_, allowed) => allowed);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.EqualTo("tenants"));
            Assert.That(resolution.IsRestored, Is.True);
            Assert.That(resolution.Reason, Is.EqualTo(ExplorerPreferenceFallbackReason.None));
            Assert.That(resolution.Explanation, Is.Null);
        });
    }

    [Test]
    public async Task Resolve_NothingRemembered_FallsBackQuietly()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();

        var resolution = preferences.Resolve(
            ExplorerPreferenceKeys.ActiveArea,
            "explore",
            state: true,
            static (_, allowed) => allowed);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.EqualTo("explore"));
            Assert.That(resolution.Reason, Is.EqualTo(ExplorerPreferenceFallbackReason.NotStored));
            Assert.That(resolution.Explanation, Is.Null);
            Assert.That(resolution.WasAbandoned, Is.False);
        });
    }

    [Test]
    public void Resolve_BeforeHydration_ReportsNotLoaded()
    {
        var (preferences, _, _) = Create(new FakeUiPreferenceStore { HydrateOnCall = 99 });

        var resolution = preferences.Resolve(
            ExplorerPreferenceKeys.ActiveArea,
            "explore",
            state: true,
            static (_, allowed) => allowed);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Reason, Is.EqualTo(ExplorerPreferenceFallbackReason.NotLoaded));
            Assert.That(resolution.Value, Is.EqualTo("explore"));
        });
    }

    [Test]
    public async Task Resolve_RememberedButUnresolvable_FallsBackAndExplains()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.Selection, "a-deleted-tree");

        var resolution = preferences.Resolve(
            ExplorerPreferenceKeys.Selection,
            string.Empty,
            state: false,
            static (_, exists) => exists);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.EqualTo(string.Empty));
            Assert.That(resolution.WasAbandoned, Is.True);
            Assert.That(
                resolution.Explanation,
                Does.Contain(ExplorerPreferenceKeys.Selection.Description));
        });
    }

    [Test]
    public async Task Resolve_ClosureOverload_BehavesTheSame()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants");

        var resolution = preferences.Resolve(
            ExplorerPreferenceKeys.ActiveArea,
            "explore",
            value => value == "tenants");

        Assert.That(resolution.IsRestored, Is.True);
    }

    [Test]
    public void Resolve_NullPredicate_Throws()
    {
        var (preferences, _, _) = Create();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => preferences.Resolve(ExplorerPreferenceKeys.ActiveArea, string.Empty, (Func<string, bool>)null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => preferences.Resolve(
                    ExplorerPreferenceKeys.ActiveArea,
                    string.Empty,
                    state: true,
                    (Func<string, bool, bool>)null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task RestoreAsync_UnresolvableValue_ForgetsItSoItCannotComeBack()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.Selection, "a-deleted-tree");

        var first = await preferences.RestoreAsync(
            ExplorerPreferenceKeys.Selection,
            string.Empty,
            state: false,
            static (_, exists) => exists);
        var second = await preferences.RestoreAsync(
            ExplorerPreferenceKeys.Selection,
            string.Empty,
            state: false,
            static (_, exists) => exists);

        Assert.Multiple(() =>
        {
            Assert.That(first.Reason, Is.EqualTo(ExplorerPreferenceFallbackReason.NotResolvable));

            // Second time round there is nothing left to abandon, so the user is
            // not told the same thing again on every restore.
            Assert.That(second.Reason, Is.EqualTo(ExplorerPreferenceFallbackReason.NotStored));
        });
    }

    [Test]
    public async Task RestoreAsync_ResolvableValue_KeepsIt()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.Selection, "orders");

        var resolution = await preferences.RestoreAsync(
            ExplorerPreferenceKeys.Selection,
            string.Empty,
            state: true,
            static (_, exists) => exists);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Value, Is.EqualTo("orders"));
            Assert.That(
                preferences.GetOrDefault(ExplorerPreferenceKeys.Selection, string.Empty),
                Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task GetRememberedRoute_WithNothingRemembered_IsRoot()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();

        Assert.That(preferences.GetRememberedRoute().IsBare, Is.True);
    }

    [Test]
    public async Task RememberRouteAsync_ThenGetRememberedRoute_RoundTrips()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "t/acme/orders")
            .WithSurface("data")
            .WithTenant("acme")
            .WithAllTenants(true);

        await preferences.RememberRouteAsync(route);

        Assert.That(preferences.GetRememberedRoute(), Is.EqualTo(route));
    }

    [Test]
    public async Task RememberRouteAsync_BareRoute_IsIgnored()
    {
        var (preferences, store, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.RememberRouteAsync(ExplorerRoute.Home);
        store.Writes.Clear();

        await preferences.RememberRouteAsync(ExplorerRoute.Root);

        Assert.Multiple(() =>
        {
            Assert.That(store.Writes, Is.Empty);
            Assert.That(preferences.GetRememberedRoute().Area, Is.EqualTo(ExplorerRouteSegments.Explore));
        });
    }

    [Test]
    public async Task RememberRouteAsync_BeforeHydration_WritesNothing()
    {
        // Comparing against an unhydrated mirror reads as "everything changed",
        // so an un-guarded write would clear the remembered selection the shell
        // is about to restore.
        var store = new FakeUiPreferenceStore { HydrateOnCall = 99 };
        var (preferences, _, _) = Create(store);

        await preferences.RememberRouteAsync(ExplorerRoute.Home);

        Assert.That(store.Writes, Is.Empty);
    }

    [Test]
    public async Task RememberRouteAsync_Null_Throws()
    {
        var (preferences, _, _) = Create();

        Assert.That(
            async () => await preferences.RememberRouteAsync(null!),
            Throws.ArgumentNullException);
        await Task.CompletedTask;
    }

    [Test]
    public async Task RememberRouteAsync_UnchangedRoute_WritesNothing()
    {
        var (preferences, store, _) = Create();
        await preferences.EnsureLoadedAsync();
        var route = ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders");
        await preferences.RememberRouteAsync(route);
        store.Writes.Clear();

        await preferences.RememberRouteAsync(route);

        // The durable store rewrites its whole document per write, so a repeated
        // navigation to the same view must cost nothing.
        Assert.That(store.Writes, Is.Empty);
    }

    [Test]
    public async Task RememberRouteAsync_OneChangedSegment_WritesOnlyThatKey()
    {
        var (preferences, store, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.RememberRouteAsync(
            ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders"));
        store.Writes.Clear();

        await preferences.RememberRouteAsync(
            ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "invoices"));

        Assert.That(store.Writes, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task RememberRouteAsync_ClearingASegment_ForgetsIt()
    {
        var (preferences, _, _) = Create();
        await preferences.EnsureLoadedAsync();
        await preferences.RememberRouteAsync(
            ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders").WithSurface("data"));

        await preferences.RememberRouteAsync(ExplorerRoute.Home.WithArea("tenants"));

        Assert.Multiple(() =>
        {
            Assert.That(preferences.GetRememberedRoute().Area, Is.EqualTo("tenants"));
            Assert.That(preferences.GetRememberedRoute().HasSelection, Is.False);
        });
    }

    [Test]
    public async Task GetRememberedRoute_WithACorruptedAreaValue_DegradesToRoot()
    {
        // A value that could never have been written through this contract - an
        // older build, or a hand-edited store - must not produce a route the
        // formatter would reject.
        var store = new FakeUiPreferenceStore();
        var (preferences, _, scope) = Create(store);
        await preferences.EnsureLoadedAsync();
        store.Seed(
            scope.Current.ToScopeToken(ExplorerPreferenceScope.UserAndCluster) + "." +
            ExplorerPreferenceKeys.ActiveArea.Name,
            "Not A Slug");

        Assert.That(preferences.GetRememberedRoute().IsBare, Is.True);
    }

    [Test]
    public async Task GetRememberedRoute_WithATenantButNoArea_StillCarriesTheScope()
    {
        var store = new FakeUiPreferenceStore();
        var (preferences, _, scope) = Create(store);
        await preferences.EnsureLoadedAsync();
        store.Seed(
            scope.Current.ToScopeToken(ExplorerPreferenceScope.UserAndCluster) + "." +
            ExplorerPreferenceKeys.ActiveTenant.Name,
            "acme");

        Assert.That(preferences.GetRememberedRoute().Tenant, Is.EqualTo("acme"));
    }

    [Test]
    public async Task GetRememberedRoute_WithAnUnusableCatalogKind_KeepsTheAreaOnly()
    {
        var store = new FakeUiPreferenceStore();
        var (preferences, _, scope) = Create(store);
        await preferences.EnsureLoadedAsync();
        var prefix = scope.Current.ToScopeToken(ExplorerPreferenceScope.UserAndCluster) + ".";
        store.Seed(prefix + ExplorerPreferenceKeys.ActiveArea.Name, "explore");
        store.Seed(prefix + ExplorerPreferenceKeys.CatalogKind.Name, "NOT A KIND");
        store.Seed(prefix + ExplorerPreferenceKeys.Selection.Name, "orders");

        var route = preferences.GetRememberedRoute();

        Assert.Multiple(() =>
        {
            Assert.That(route.Area, Is.EqualTo("explore"));
            Assert.That(route.HasSelection, Is.False);
        });
    }

    [Test]
    public void Dispose_DetachesFromTheScopeProvider()
    {
        var (preferences, _, scope) = Create();
        var changes = 0;
        preferences.Changed += () => changes++;

        preferences.Dispose();
        scope.MoveTo("bob", "https://cluster-b");

        Assert.That(changes, Is.Zero);
    }
}
