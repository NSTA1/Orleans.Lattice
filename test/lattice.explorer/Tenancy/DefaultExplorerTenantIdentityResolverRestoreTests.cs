using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// The regression suite for the resolver-clobber defect, and the remembered-scope
/// contract that replaced it.
/// </summary>
/// <remarks>
/// <para>
/// <b>The measured bug.</b> The shell's tenant control switched tenant and then
/// re-resolved to read the new scope back. The resolver unconditionally assigned
/// the default tenant, so the read-back overwrote the switch inside the same
/// call: the label never changed and nothing was reported. Every
/// <c>leavesTheSwitchedTenantInPlace</c> test below fails against that code and
/// passes against the fix.
/// </para>
/// <para>
/// Every row is asserted directly against the real resolver, switcher, context,
/// preference contract and in-memory store - no cluster, no timing, no ordering,
/// no wall clock, and no GC dependence. Hydration is driven explicitly by
/// <see cref="FakeUiPreferenceStore.HydrateOnCall"/> rather than by waiting.
/// </para>
/// </remarks>
[TestFixture]
public class DefaultExplorerTenantIdentityResolverRestoreTests
{
    private static IExplorerAuthSession Session(bool authenticated, string? username = "ada")
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(authenticated);
        session.Username.Returns(username);
        return session;
    }

    private static ExplorerTenantView ActiveView(ExplorerTenantContext context, bool isOperator = true) =>
        new(context, new StubOperatorGate(isOperator));

    private static ExplorerShellPreferences Preferences(FakeUiPreferenceStore store) =>
        new(store, new ExplorerPreferenceCatalog(), new FakeExplorerPreferenceScopeProvider());

    /// <summary>
    /// A hydrated preference contract already remembering <paramref name="tenantId"/>,
    /// as a previous session would have left it.
    /// </summary>
    private static async Task<ExplorerShellPreferences> RememberingAsync(string tenantId)
    {
        var preferences = Preferences(new FakeUiPreferenceStore());
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveTenant, tenantId);
        return preferences;
    }

    // --- The defect: a refresh must never revert a switch ---

    [Test]
    public async Task ResolveAsync_afterAnExplicitSwitch_leavesTheSwitchedTenantInPlace()
    {
        // Fails against the pre-fix resolver, which assigned the default tenant on
        // every call and therefore reverted the switch it was asked to read back.
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: true), context);
        await resolver.ResolveAsync();

        context.ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId);
        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
    }

    [Test]
    public async Task An_operator_switch_followed_by_the_shells_refresh_keeps_the_new_tenant()
    {
        // The exact sequence the shell performs: switch through the seam, then
        // re-resolve to read the scope back for display.
        var context = new ExplorerTenantContext();
        var view = ActiveView(context);
        var resolver = new DefaultExplorerTenantIdentityResolver(view, Session(authenticated: true), context);
        var switcher = new ExplorerTenantSwitcher(view, context, new StubOperatorGate(isOperator: true));
        await resolver.ResolveAsync();

        var applied = await switcher.SwitchTenantAsync(new ExplorerTenantId(SampleTenant.TenantId));
        await resolver.ResolveAsync();

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
        });
    }

    [Test]
    public async Task ResolveAsync_repeatedAfterASwitch_stillLeavesTheSwitchedTenantInPlace()
    {
        // The control resolves on every refresh, so preservation cannot be a
        // one-call reprieve.
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: true), context);
        await resolver.ResolveAsync();
        context.ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId);

        await resolver.ResolveAsync();
        await resolver.ResolveAsync();
        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
    }

    // --- Fail-closed: preservation never outlives the sign-in it belongs to ---

    [Test]
    public async Task ResolveAsync_signOut_clearsEvenAnExplicitSwitch()
    {
        // Preserving a switch across a sign-out would leave that tenant's trees
        // readable by an anonymous caller.
        var context = new ExplorerTenantContext();
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);
        session.Username.Returns("ada");
        var resolver = new DefaultExplorerTenantIdentityResolver(ActiveView(context), session, context);
        await resolver.ResolveAsync();
        context.ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId);

        session.IsAuthenticated.Returns(false);
        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_aDifferentSignIn_reestablishesRatherThanInheriting()
    {
        var context = new ExplorerTenantContext();
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);
        session.Username.Returns("ada");
        var resolver = new DefaultExplorerTenantIdentityResolver(ActiveView(context), session, context);
        await resolver.ResolveAsync();
        context.ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId);

        session.Username.Returns("grace");
        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(ExplorerTenantId.Default));
    }

    // --- Establishing the remembered tenant ---

    [Test]
    public async Task ResolveAsync_aRememberedReachableTenant_isRestored()
    {
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value, SampleTenant.TenantId),
            await RememberingAsync(SampleTenant.TenantId));

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
    }

    [Test]
    public async Task ResolveAsync_nothingRemembered_fallsBackToTheDefaultTenant()
    {
        var context = new ExplorerTenantContext();
        var preferences = Preferences(new FakeUiPreferenceStore());
        await preferences.EnsureLoadedAsync();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value, SampleTenant.TenantId),
            preferences);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(ExplorerTenantId.Default));
    }

    [Test]
    public async Task ResolveAsync_nothingRememberedAndNoReachableDefault_fallsBackToTheFirstReachableTenant()
    {
        var context = new ExplorerTenantContext();
        var preferences = Preferences(new FakeUiPreferenceStore());
        await preferences.EnsureLoadedAsync();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(SampleTenant.TenantId, SampleTenant.OtherTenantId),
            preferences);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
    }

    [Test]
    public async Task ResolveAsync_noAccessibleSource_stillEstablishesTheDefaultTenant()
    {
        // A deployment that registers nothing behaves exactly as it did before.
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: true), context);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(ExplorerTenantId.Default));
    }

    // --- Fail-closed restore: a remembered tenant that is no longer reachable ---

    [Test]
    public async Task ResolveAsync_aRememberedTenantThatIsNoLongerReachable_isNotRestored()
    {
        // A revoked grant, a suspended tenant or a deleted one: never land the
        // caller on a tenant they can no longer reach.
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value),
            await RememberingAsync(SampleTenant.TenantId));

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(ExplorerTenantId.Default));
    }

    [Test]
    public async Task ResolveAsync_anAbandonedRememberedTenant_isExplained()
    {
        var context = new ExplorerTenantContext();
        var notices = new ExplorerTenantScopeNotices();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value),
            await RememberingAsync(SampleTenant.TenantId),
            notices);

        await resolver.ResolveAsync();

        Assert.Multiple(() =>
        {
            Assert.That(notices.Current, Is.Not.Null, "an abandoned scope must never be silent");
            Assert.That(notices.Current!.Kind, Is.EqualTo(ExplorerTenantNoticeKind.RestoreAbandoned));
            Assert.That(notices.Current.Message, Does.Contain(ExplorerTenantId.Default.Value));
        });
    }

    [Test]
    public async Task ResolveAsync_aRestoredReachableTenant_saysNothing()
    {
        var context = new ExplorerTenantContext();
        var notices = new ExplorerTenantScopeNotices();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(SampleTenant.TenantId),
            await RememberingAsync(SampleTenant.TenantId),
            notices);

        await resolver.ResolveAsync();

        Assert.That(notices.Current, Is.Null, "restoring what the caller chose is not news");
    }

    [Test]
    public async Task ResolveAsync_anAbandonedRememberedTenant_isForgottenSoItIsNotExplainedTwice()
    {
        var context = new ExplorerTenantContext();
        var preferences = await RememberingAsync(SampleTenant.TenantId);
        var notices = new ExplorerTenantScopeNotices();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value),
            preferences,
            notices);
        await resolver.ResolveAsync();

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveTenant, string.Empty),
            Is.Empty,
            "a value that no longer resolves must not keep resurfacing");
    }

    // --- Hydration: a prerender cannot read the remembered tenant ---

    [Test]
    public async Task ResolveAsync_beforeTheStoreHydrates_usesTheFallbackAndReconsidersAfterwards()
    {
        // A prerender pass cannot reach browser storage, so the first
        // establishment is provisional and must be revisited - otherwise a
        // remembered tenant is never restored at all.
        var context = new ExplorerTenantContext();
        var preferences = Preferences(new FakeUiPreferenceStore { HydrateOnCall = 2 });
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value, SampleTenant.TenantId),
            preferences);

        await resolver.ResolveAsync();
        var provisional = context.ActiveTenant;

        // Hydrate, seed what an earlier session left behind, and resolve again.
        await preferences.EnsureLoadedAsync();
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(ExplorerPreferenceKeys.ActiveTenant, SampleTenant.TenantId);
        await resolver.ResolveAsync();

        Assert.Multiple(() =>
        {
            Assert.That(provisional, Is.EqualTo(ExplorerTenantId.Default));
            Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
        });
    }

    [Test]
    public async Task ResolveAsync_aSwitchMadeBeforeTheStoreHydrates_isStillNotOverwritten()
    {
        // The provisional re-attempt must reconsider only the resolver's own
        // value, never one the caller switched to in the meantime.
        var context = new ExplorerTenantContext();
        var preferences = Preferences(new FakeUiPreferenceStore { HydrateOnCall = 2 });
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context),
            Session(authenticated: true),
            context,
            new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value, SampleTenant.OtherTenantId),
            preferences);
        await resolver.ResolveAsync();

        context.ActiveTenant = new ExplorerTenantId(SampleTenant.OtherTenantId);
        await preferences.EnsureLoadedAsync();
        await preferences.EnsureLoadedAsync();
        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.OtherTenantId)));
    }

    // --- The inactive view is still untouched ---

    [Test]
    public async Task ResolveAsync_inactiveView_neverAsksForTheAccessibleTenants()
    {
        var context = new ExplorerTenantContext();
        var source = new FakeAccessibleTenantSource(SampleTenant.TenantId);
        var resolver = new DefaultExplorerTenantIdentityResolver(
            NullExplorerTenantView.Instance, Session(authenticated: true), context, source);

        await resolver.ResolveAsync();

        Assert.Multiple(() =>
        {
            Assert.That(context.ActiveTenant, Is.Null);
            Assert.That(source.Calls, Is.Zero);
        });
    }

    [Test]
    public async Task ResolveAsync_anEstablishedScope_isNotReestablishedOnEveryRefresh()
    {
        // Establishing is a one-off per sign-in, so a refresh does not re-ask the
        // cluster for the reachable list on every call.
        var context = new ExplorerTenantContext();
        var source = new FakeAccessibleTenantSource(ExplorerTenantId.Default.Value);
        var preferences = Preferences(new FakeUiPreferenceStore());
        await preferences.EnsureLoadedAsync();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: true), context, source, preferences);

        await resolver.ResolveAsync();
        await resolver.ResolveAsync();
        await resolver.ResolveAsync();

        Assert.That(source.Calls, Is.EqualTo(1));
    }
}
