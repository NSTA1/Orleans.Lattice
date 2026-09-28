using System.Reflection;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// The app bridge denial matrix: one test per authorization step, each proving the step fails closed and that
/// a refused request never reaches the data path, plus the order the steps run in.
/// </summary>
[TestFixture]
public sealed class LatticeAppBridgeAuthorizationTests
{
    private static BridgeHarness Editor() => new BridgeHarness().Installed("alice", BridgeHarness.Editors);

    private static void AssertEveryVerbDeniedUntouched(BridgeHarness harness, AppBridgeTarget? target = null, AppBridgeFailure expected = AppBridgeFailure.Denied)
    {
        BridgeAssert.EveryVerbFails(harness.Bridge, target ?? BridgeHarness.Target(), expected);
        harness.AssertDataPathUntouched();
    }

    [Test]
    public async Task An_editor_of_an_enabled_install_reaches_the_structural_tree_with_every_verb()
    {
        var harness = Editor();
        var bridge = harness.Bridge;
        var target = BridgeHarness.Target();

        await bridge.SetAsync(target, "k", new byte[] { 7 });
        Assert.That((await bridge.GetAsync(target, "k"))!.Value.ToArray(), Is.EqualTo(new byte[] { 7 }));
        Assert.That((await bridge.ScanAsync(target, string.Empty, 10)).Entries.Single().Key, Is.EqualTo("k"));
        Assert.That(await bridge.DeleteAsync(target, "k"), Is.True);

        Assert.That(harness.Dialled, Is.All.EqualTo(BridgeHarness.NotesTree));
    }

    // Step 1 - the install.

    [TestCase(AppRegistryLifecycleState.Installed)]
    [TestCase(AppRegistryLifecycleState.Disabled)]
    [TestCase(AppRegistryLifecycleState.Uninstalled)]
    public void An_install_that_is_not_enabled_is_denied(AppRegistryLifecycleState state)
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(state));

        AssertEveryVerbDeniedUntouched(harness);
    }

    [TestCase(BridgeHarness.Revision - 1)]
    [TestCase(BridgeHarness.Revision + 1)]
    [TestCase(0L)]
    public void A_target_whose_install_revision_does_not_match_is_denied(long revision) =>
        AssertEveryVerbDeniedUntouched(Editor(), BridgeHarness.Target(revision: revision));

    [Test]
    public void A_revision_mismatch_is_indistinguishable_from_an_app_that_does_not_exist()
    {
        var mismatch = Assert.ThrowsAsync<AppBridgeException>(() => Editor().Bridge.GetAsync(BridgeHarness.Target(revision: 99), "k"));
        var absent = Assert.ThrowsAsync<AppBridgeException>(() => new BridgeHarness().As("alice", BridgeHarness.Editors).Bridge
            .GetAsync(BridgeHarness.Target(), "k"));

        Assert.That(mismatch!.Failure, Is.EqualTo(absent!.Failure));
        Assert.That(mismatch.Message, Is.EqualTo(absent.Message));
    }

    [Test]
    public void An_app_that_is_not_installed_is_denied() =>
        AssertEveryVerbDeniedUntouched(new BridgeHarness().As("alice", BridgeHarness.Editors), BridgeHarness.Target(slug: "billing"));

    [Test]
    public void An_install_whose_ceiling_is_not_pinned_to_its_version_is_denied()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record() with { CeilingVersion = AppsControlHarness.V("0.9.0") });

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void An_install_whose_manifest_no_longer_resolves_is_denied()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record() with { Version = AppsControlHarness.V(AppsControlHarness.OtherVersion), CeilingVersion = AppsControlHarness.V(AppsControlHarness.OtherVersion) });

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void A_cross_tenant_install_is_denied()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(tenant: AppsControlHarness.Acme));
        harness.Tenants.Tenant = TenantId.Default;

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public async Task An_install_in_the_callers_tenant_resolves_to_the_tenant_composed_tree()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(tenant: AppsControlHarness.Acme));
        harness.Tenants.Tenant = AppsControlHarness.Acme;

        await harness.Bridge.SetAsync(BridgeHarness.Target(), "k", new byte[] { 1 });

        Assert.That(harness.Dialled, Is.EqualTo(new[] { "t/acme/a/crm/notes" }));
    }

    // Step 2 - bridge consent.

    [Test]
    public async Task An_unconsented_operation_is_denied()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(consented: AppUiBridgeRequest.Create([new AppUiBridgeGrant(AppUiBridgeOperations.DataRead)])));
        var bridge = harness.Bridge;

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.SetAsync(BridgeHarness.Target(), "k", new byte[] { 1 }));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.DeleteAsync(BridgeHarness.Target(), "k"));
        harness.AssertDataPathUntouched();

        Assert.That(await bridge.GetAsync(BridgeHarness.Target(), "k"), Is.Null, "the consented read still works");
    }

    [Test]
    public void An_install_with_no_recorded_bridge_consent_is_denied()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record() with { ConsentedBridge = null });

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void An_operation_the_manifest_requests_but_was_never_consented_is_denied()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(consented: AppUiBridgeRequest.Empty));

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void A_consented_operation_the_installed_manifest_no_longer_requests_is_denied()
    {
        var harness = new BridgeHarness(BridgeHarness.DefaultManifest(UiTestManifestsBridge(AppUiBridgeOperations.DataRead)))
            .As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(consented: AppUiBridgeRequest.FromManifest(BridgeHarness.DefaultManifest())));

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => harness.Bridge.SetAsync(BridgeHarness.Target(), "k", new byte[] { 1 }));
        harness.AssertDataPathUntouched();
    }

    [Test]
    public void A_manifest_requesting_no_bridge_operations_is_denied()
    {
        var harness = new BridgeHarness(BridgeHarness.DefaultManifest() with { Ui = null }).As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(consented: AppUiBridgeRequest.FromManifest(BridgeHarness.DefaultManifest())));

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void An_operation_consented_only_for_another_tree_is_denied() =>
        AssertEveryWriteDenied(Editor(), BridgeHarness.Target("archive"));

    // Step 3 - tree resolution.

    [Test]
    public void An_undeclared_tree_is_not_found()
    {
        var harness = Editor();

        BridgeAssert.Fails(AppBridgeFailure.NotFound, () => harness.Bridge.GetAsync(BridgeHarness.Target("missing"), "k"));
        BridgeAssert.Fails(AppBridgeFailure.NotFound, () => harness.Bridge.ScanAsync(BridgeHarness.Target("missing"), string.Empty, 10));
        harness.AssertDataPathUntouched();
    }

    [Test]
    public void The_adopted_physical_tree_id_is_not_an_address()
    {
        // "legacy-notes" is a well-formed name but not a declared one: the physical id cannot be named.
        var harness = new BridgeHarness().Installed("victor", BridgeHarness.Viewers);

        BridgeAssert.Fails(AppBridgeFailure.NotFound, () => harness.Bridge.GetAsync(BridgeHarness.Target(BridgeHarness.AdoptedTree), "k"));
        harness.AssertDataPathUntouched();
    }

    [TestCase("a/crm/notes")]
    [TestCase("t/acme/a/crm/notes")]
    [TestCase("Notes")]
    [TestCase("")]
    [TestCase("_lattice_registry")]
    public void A_physical_or_malformed_tree_name_is_invalid(string tree) =>
        AssertEveryVerbDeniedUntouched(Editor(), BridgeHarness.Target(tree), AppBridgeFailure.Invalid);

    [Test]
    public void A_tree_name_longer_than_the_bound_is_invalid() =>
        AssertEveryVerbDeniedUntouched(Editor(), BridgeHarness.Target(new string('a', AppBridgeLimits.MaxTreeNameLength + 1)), AppBridgeFailure.Invalid);

    [Test]
    public async Task A_declared_adopted_tree_resolves_to_its_adopted_id()
    {
        var harness = new BridgeHarness().Installed("victor", BridgeHarness.Viewers).Seed(BridgeHarness.AdoptedTree, "k", [5]);

        var value = await harness.Bridge.GetAsync(BridgeHarness.Target("legacy"), "k");

        Assert.That(value!.Value.ToArray(), Is.EqualTo(new byte[] { 5 }));
        Assert.That(harness.Dialled, Is.EqualTo(new[] { BridgeHarness.AdoptedTree }));
    }

    // Step 4 - the app-role grant.

    [Test]
    public void An_adopted_tree_without_its_exception_scope_is_denied()
    {
        var harness = new BridgeHarness().As("victor", BridgeHarness.Viewers);
        harness.Publish(harness.Record(ceiling: BridgeHarness.DefaultCeiling(LatticeScope.Tree("some-other-tree"))));

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => harness.Bridge.GetAsync(BridgeHarness.Target("legacy"), "k"));
        harness.AssertDataPathUntouched();
    }

    [Test]
    public void An_adopted_tree_covered_only_by_a_narrower_exception_is_denied_outside_it()
    {
        var harness = new BridgeHarness().As("victor", BridgeHarness.Viewers);
        harness.Publish(harness.Record(ceiling: BridgeHarness.DefaultCeiling(LatticeScope.Prefix(BridgeHarness.AdoptedTree, "p/"))));

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => harness.Bridge.GetAsync(BridgeHarness.Target("legacy"), "p/k"));
        harness.AssertDataPathUntouched();
    }

    [Test]
    public async Task A_viewer_may_read_but_is_denied_a_write_and_a_delete()
    {
        var harness = new BridgeHarness().Installed("victor", BridgeHarness.Viewers);
        var bridge = harness.Bridge;

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.SetAsync(BridgeHarness.Target(), "k", new byte[] { 1 }));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.DeleteAsync(BridgeHarness.Target(), "k"));
        harness.AssertDataPathUntouched();

        Assert.That(await bridge.GetAsync(BridgeHarness.Target(), "k"), Is.Null);
    }

    [Test]
    public void A_caller_with_broad_non_app_rights_but_only_the_viewer_role_is_denied_a_write()
    {
        // Olga is an operator: outside the app she may do anything to every tree, the app's own included. Inside
        // the app she holds only the viewer role, so the app's UI must not become a way to use her operator rights.
        var harness = new BridgeHarness().Installed("olga", BridgeHarness.Viewers, "g-operators");
        foreach (var tree in new[] { BridgeHarness.NotesTree, BridgeHarness.AdoptedTree, "a/crm/archive", LatticeScope.ClusterWideTreeId })
        {
            foreach (var operation in new[] { LatticeOperation.Read, LatticeOperation.Write, LatticeOperation.Delete })
            {
                harness.Gate.Grant("olga", tree, operation);
            }
        }

        var bridge = harness.Bridge;

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.SetAsync(BridgeHarness.Target(), "k", new byte[] { 1 }));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.DeleteAsync(BridgeHarness.Target(), "k"));
        harness.AssertDataPathUntouched();
        Assert.That(harness.Gate.Requests, Is.Zero, "the caller's own rules are never consulted to grant an app role");
    }

    [Test]
    public async Task A_role_that_may_read_keys_but_not_ranges_is_denied_a_scan()
    {
        // The data path enforces RangeRead for a scan, so the bridge must not treat Read as enough: a scan the
        // data plane would silently filter to nothing is refused as a denial instead.
        var manifest = BridgeHarness.DefaultManifest() with
        {
            Roles =
            [
                new AppRoleDeclaration { Name = "viewer", Operations = LatticeOperation.Read, Scopes = [new AppScopeTemplate { Tree = "notes" }] },
            ],
        };
        var harness = new BridgeHarness(manifest).Installed("victor", BridgeHarness.Viewers);
        var bridge = harness.Bridge;

        Assert.That(await bridge.GetAsync(BridgeHarness.Target(), "k"), Is.Null);
        var dialled = harness.Dialled.Count;
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.ScanAsync(BridgeHarness.Target(), string.Empty, 10));
        Assert.That(harness.Dialled, Has.Count.EqualTo(dialled));
    }

    [Test]
    public async Task A_ceiling_without_range_read_denies_a_scan_but_not_a_read()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(ceiling: new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete,
            ApprovedExceptionScopes = [LatticeScope.Tree(BridgeHarness.AdoptedTree)],
        }));
        var bridge = harness.Bridge;

        Assert.That(await bridge.GetAsync(BridgeHarness.Target(), "k"), Is.Null);
        var dialled = harness.Dialled.Count;
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.ScanAsync(BridgeHarness.Target(), string.Empty, 10));
        Assert.That(harness.Dialled, Has.Count.EqualTo(dialled));
    }

    [Test]
    public void A_caller_holding_no_role_of_the_app_is_denied_even_a_read() =>
        AssertEveryVerbDeniedUntouched(new BridgeHarness().Installed("mallory", "g-somebody-else"));

    [Test]
    public void A_caller_with_no_groups_is_denied() =>
        AssertEveryVerbDeniedUntouched(new BridgeHarness().Installed("mallory"));

    [Test]
    public void A_group_named_like_the_caller_does_not_stand_in_for_membership() =>
        AssertEveryVerbDeniedUntouched(new BridgeHarness().Installed(BridgeHarness.Editors));

    [Test]
    public void A_role_operation_outside_the_ceiling_is_denied()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(ceiling: new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.Read,
            ApprovedExceptionScopes = [LatticeScope.Tree(BridgeHarness.AdoptedTree)],
        }));

        AssertEveryWriteDenied(harness, BridgeHarness.Target());
    }

    [Test]
    public void A_role_bound_to_nothing_grants_nothing()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record() with { RoleBindings = [AppRoleBinding.Create("viewer", BridgeHarness.Viewers)] });

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void A_tree_no_role_reaches_is_denied_to_every_role()
    {
        var harness = new BridgeHarness().Installed("alice", BridgeHarness.Editors, BridgeHarness.Viewers, BridgeHarness.Drafters);

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => harness.Bridge.GetAsync(BridgeHarness.Target("archive"), "k"));
        harness.AssertDataPathUntouched();
    }

    [Test]
    public async Task A_prefix_scoped_role_is_confined_to_its_prefix()
    {
        var harness = new BridgeHarness().Installed("dana", BridgeHarness.Drafters);
        var bridge = harness.Bridge;
        var target = BridgeHarness.Target();

        await bridge.SetAsync(target, "drafts/1", new byte[] { 1 });
        Assert.That(await bridge.GetAsync(target, "drafts/1"), Is.Not.Null);
        Assert.That((await bridge.ScanAsync(target, "drafts/", 10)).Entries, Has.Length.EqualTo(1));
        Assert.That((await bridge.ScanAsync(target, "drafts/1", 10)).Entries, Has.Length.EqualTo(1));
        var allowed = harness.Dialled.Count;

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.SetAsync(target, "final/1", new byte[] { 1 }));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.GetAsync(target, "drafts"));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.ScanAsync(target, "drafts", 10));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.ScanAsync(target, string.Empty, 10));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.DeleteAsync(target, "drafts/1"));
        Assert.That(harness.Dialled, Has.Count.EqualTo(allowed), "no refused request reached the data path");
    }

    [Test]
    public async Task A_key_scoped_role_is_confined_to_its_key_and_never_scans()
    {
        var harness = new BridgeHarness().Installed("dana", BridgeHarness.Drafters);
        var bridge = harness.Bridge;
        var target = BridgeHarness.Target();

        await bridge.SetAsync(target, "pinned", new byte[] { 1 });
        Assert.That(await bridge.GetAsync(target, "pinned"), Is.Not.Null);
        var allowed = harness.Dialled.Count;

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.GetAsync(target, "pinned2"));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.ScanAsync(target, "pinned", 10));
        Assert.That(harness.Dialled, Has.Count.EqualTo(allowed));
    }

    // The caller.

    [Test]
    public void A_missing_membership_context_is_denied()
    {
        var harness = Editor();
        harness.Membership = null;

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void The_anonymous_null_membership_context_is_denied()
    {
        var harness = Editor();
        harness.Membership = (ILatticeMembershipContext)Activator.CreateInstance(
            typeof(ILatticeMembershipContext).Assembly.GetType("Orleans.Lattice.NullLatticeMembershipContext", throwOnError: true)!,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            args: null,
            culture: null)!;

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public void The_anonymous_subject_is_denied_even_when_placed_in_a_role_group() =>
        AssertEveryVerbDeniedUntouched(new BridgeHarness().Installed(LatticeSubject.AnonymousSubjectId, BridgeHarness.Editors));

    [Test]
    public void A_refused_tenant_resolution_is_denied()
    {
        var harness = Editor();
        harness.Tenants.Tenant = default;

        AssertEveryVerbDeniedUntouched(harness);
    }

    [Test]
    public async Task An_asynchronous_tenant_resolution_is_honoured()
    {
        var harness = Editor();
        harness.Tenants.Synchronous = false;

        await harness.Bridge.SetAsync(BridgeHarness.Target(), "k", new byte[] { 1 });

        Assert.That(harness.Tenants.AsyncResolutions, Is.EqualTo(1));
        Assert.That(harness.Dialled, Is.EqualTo(new[] { BridgeHarness.NotesTree }));
    }

    [TestCase("grains")]
    [TestCase("tenants")]
    [TestCase("gate")]
    [TestCase("projection")]
    [TestCase("source")]
    public void A_missing_collaborator_is_denied(string missing)
    {
        var harness = Editor();
        var bridge = harness.Create(
            withGrains: missing != "grains",
            withTenants: missing != "tenants",
            withGate: missing != "gate",
            withProjection: missing != "projection",
            withSource: missing != "source");

        BridgeAssert.EveryVerbFails(bridge, BridgeHarness.Target(), AppBridgeFailure.Denied);
        harness.AssertDataPathUntouched();
    }

    // Order.

    [Test]
    public void The_install_is_checked_before_the_tree_is_resolved()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        harness.Publish(harness.Record(AppRegistryLifecycleState.Disabled));

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => harness.Bridge.GetAsync(BridgeHarness.Target("missing"), "k"));
    }

    [Test]
    public void Consent_is_checked_before_the_tree_is_resolved() =>
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => Editor().Bridge.SetAsync(BridgeHarness.Target("missing"), "k", new byte[] { 1 }));

    [Test]
    public void The_tree_is_resolved_before_the_app_role_grant_is_checked() =>
        BridgeAssert.Fails(AppBridgeFailure.NotFound, () => new BridgeHarness().Installed("mallory").Bridge.GetAsync(BridgeHarness.Target("missing"), "k"));

    private static void AssertEveryWriteDenied(BridgeHarness harness, AppBridgeTarget target)
    {
        var bridge = harness.Bridge;
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.SetAsync(target, "k", new byte[] { 1 }));
        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.DeleteAsync(target, "k"));
        harness.AssertDataPathUntouched();
    }

    private static AppUiBridgeDeclaration UiTestManifestsBridge(string operation, params string[] trees) =>
        Orleans.Lattice.Apps.Tests.UiTestManifests.Bridge(operation, trees);
}
