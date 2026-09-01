using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Coverage for the two things the switcher gained so that a tenant scope is
/// honest and durable: every outcome is stated, and an applied scope is
/// remembered through the shell's preference contract.
/// </summary>
/// <remarks>
/// The seam's fail-closed <see cref="bool"/> results used to be discarded by the
/// only caller, so a genuine refusal looked exactly like a switch that worked.
/// Publishing at the seam rather than at the call site means every caller - the
/// shell's control, and a tenant list offering "set as active tenant" - reports
/// the same way. Direct assertions against the real switcher, context, view and
/// preference contract; no cluster, timing, ordering or wall clock.
/// </remarks>
[TestFixture]
public class ExplorerTenantSwitcherFeedbackTests
{
    private static ExplorerShellPreferences Preferences(FakeUiPreferenceStore store) =>
        new(store, new ExplorerPreferenceCatalog(), new FakeExplorerPreferenceScopeProvider());

    private static ExplorerTenantSwitcher Switcher(
        ExplorerTenantContext context,
        bool isOperator,
        IExplorerShellPreferences? preferences = null,
        IExplorerTenantScopeNotices? notices = null)
    {
        var gate = new StubOperatorGate(isOperator);
        return new ExplorerTenantSwitcher(
            new ExplorerTenantView(context, gate),
            context,
            gate,
            scopeRefresher: null,
            preferences,
            notices);
    }

    // --- Honest feedback ---

    [Test]
    public async Task SwitchTenantAsync_applied_statesTheNewScope()
    {
        var notices = new ExplorerTenantScopeNotices();
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: true, notices: notices);

        await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.Multiple(() =>
        {
            Assert.That(notices.Current, Is.Not.Null);
            Assert.That(notices.Current!.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Applied));
            Assert.That(notices.Current.Message, Does.Contain(SampleTenant.TenantId));
        });
    }

    [Test]
    public async Task SwitchTenantAsync_refused_explainsTheRefusalRatherThanSayingNothing()
    {
        var notices = new ExplorerTenantScopeNotices();
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: false, notices: notices);

        var applied = await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.False);
            Assert.That(notices.Current, Is.Not.Null, "a denial must be explained, not merely enacted");
            Assert.That(notices.Current!.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Refused));
            Assert.That(notices.Current.IsDenial, Is.True);
        });
    }

    [Test]
    public async Task SetVisibilityAsync_applied_statesTheNewVisibility()
    {
        var notices = new ExplorerTenantScopeNotices();
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: true, notices: notices);

        await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(notices.Current, Is.Not.Null);
            Assert.That(notices.Current!.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Applied));
        });
    }

    [Test]
    public async Task SetVisibilityAsync_refused_explainsTheRefusal()
    {
        var notices = new ExplorerTenantScopeNotices();
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: false, notices: notices);

        var applied = await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.False);
            Assert.That(notices.Current!.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Refused));
        });
    }

    [Test]
    public async Task A_switcher_with_no_notice_sink_still_applies_the_switch()
    {
        var context = new ExplorerTenantContext();
        var switcher = Switcher(context, isOperator: true);

        var applied = await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
        });
    }

    // --- Remembering the applied scope ---

    [Test]
    public async Task SwitchTenantAsync_applied_remembersTheTenantForALaterSession()
    {
        var preferences = Preferences(new FakeUiPreferenceStore());
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: true, preferences);

        await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveTenant, string.Empty),
            Is.EqualTo(SampleTenant.TenantId));
    }

    [Test]
    public async Task SwitchTenantAsync_refused_remembersNothing()
    {
        // Fail-closed all the way down: a refused switch must not leave a
        // remembered scope behind to be restored next session.
        var store = new FakeUiPreferenceStore();
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: false, Preferences(store));

        await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.That(store.Writes, Is.Empty);
    }

    [Test]
    public async Task SetVisibilityAsync_applied_remembersTheAllTenantSetting()
    {
        var preferences = Preferences(new FakeUiPreferenceStore());
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: true, preferences);

        await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.AllTenantsVisible, false),
            Is.True);
    }

    [Test]
    public async Task SetVisibilityAsync_refused_remembersNothing()
    {
        var store = new FakeUiPreferenceStore();
        var switcher = Switcher(new ExplorerTenantContext(), isOperator: false, Preferences(store));

        await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.That(store.Writes, Is.Empty);
    }

    [Test]
    public async Task A_store_that_never_hydrates_neverUnwindsAnAppliedSwitch()
    {
        // Durable persistence is a convenience layered over the applied scope; a
        // prerender that cannot reach it is not an error.
        var context = new ExplorerTenantContext();
        var switcher = Switcher(
            context,
            isOperator: true,
            Preferences(new FakeUiPreferenceStore { HydrateOnCall = int.MaxValue }));

        var applied = await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
        });
    }

    [Test]
    public async Task A_failing_preference_store_neverUnwindsAnAppliedSwitch()
    {
        var context = new ExplorerTenantContext();
        var switcher = Switcher(context, isOperator: true, new ThrowingPreferences());

        var applied = await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
        });
    }

    [Test]
    public async Task A_failing_preference_store_neverUnwindsAnAppliedVisibilityChange()
    {
        var context = new ExplorerTenantContext();
        var switcher = Switcher(context, isOperator: true, new ThrowingPreferences());

        var applied = await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
        });
    }

    /// <summary>
    /// A preference contract whose every durable operation faults, so a test can
    /// prove an applied mutation survives a broken store.
    /// </summary>
    private sealed class ThrowingPreferences : IExplorerShellPreferences
    {
        public bool IsLoaded => false;

        public IReadOnlyList<ExplorerPreferenceKey> Keys => Array.Empty<ExplorerPreferenceKey>();

        public event Action? Changed
        {
            add { }
            remove { }
        }

        public Task EnsureLoadedAsync(CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("no durable store");

        public T GetOrDefault<T>(ExplorerPreferenceKey key, T fallback = default!) => fallback;

        public ExplorerPreferenceResolution<T> Resolve<T, TState>(
            ExplorerPreferenceKey key,
            T fallback,
            TState state,
            Func<T, TState, bool> isResolvable) =>
            ExplorerPreferenceResolution<T>.FellBack(fallback, ExplorerPreferenceFallbackReason.NotLoaded);

        public ExplorerPreferenceResolution<T> Resolve<T>(
            ExplorerPreferenceKey key,
            T fallback,
            Func<T, bool> isResolvable) =>
            ExplorerPreferenceResolution<T>.FellBack(fallback, ExplorerPreferenceFallbackReason.NotLoaded);

        public Task<ExplorerPreferenceResolution<T>> RestoreAsync<T, TState>(
            ExplorerPreferenceKey key,
            T fallback,
            TState state,
            Func<T, TState, bool> isResolvable,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("no durable store");

        public Task SetAsync<T>(ExplorerPreferenceKey key, T value, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("no durable store");

        public Task ClearAsync(ExplorerPreferenceKey key, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("no durable store");

        public Task ResetAsync(CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("no durable store");

        public Orleans.Lattice.Explorer.Core.Navigation.ExplorerRoute GetRememberedRoute() => Orleans.Lattice.Explorer.Core.Navigation.ExplorerRoute.Root;

        public Task RememberRouteAsync(
            Orleans.Lattice.Explorer.Core.Navigation.ExplorerRoute route,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("no durable store");
    }
}
