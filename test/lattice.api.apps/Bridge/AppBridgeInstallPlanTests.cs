using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Tests;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// The per-install bridge plan: consent and request sets, server-side tree resolution, and the app-owned
/// grants derived from the role bindings with the ceiling re-checked.
/// </summary>
[TestFixture]
public sealed class AppBridgeInstallPlanTests
{
    private static AppBridgeInstallPlan Plan(AppManifest manifest, AppRegistryRecord record) =>
        AppBridgeInstallPlan.Build(new AppRoleGrantInstall(record, manifest, AppRoleGrantEvaluator.CompileRoles(record, manifest)));

    private static AppBridgeInstallPlan DefaultPlan(TenantId? tenant = null)
    {
        var harness = new BridgeHarness();
        return Plan(harness.Manifest, harness.Record(tenant: tenant));
    }

    private static IReadOnlyCollection<string> Groups(params string[] groups) => new HashSet<string>(groups, StringComparer.Ordinal);

    [Test]
    public void Build_rejects_a_null_install() =>
        Assert.Throws<ArgumentNullException>(() => AppBridgeInstallPlan.Build(null!));

    [Test]
    public void Declared_trees_resolve_to_their_local_and_effective_ids()
    {
        var plan = DefaultPlan(AppsControlHarness.Acme);

        Assert.That(plan.TryGetTree("notes", out var notes), Is.True);
        Assert.That(notes.LocalTreeId, Is.EqualTo("a/crm/notes"));
        Assert.That(notes.EffectiveTreeId, Is.EqualTo("t/acme/a/crm/notes"));
        Assert.That(plan.TryGetTree("legacy", out var legacy), Is.True);
        Assert.That(legacy.LocalTreeId, Is.EqualTo(BridgeHarness.AdoptedTree));
        Assert.That(legacy.EffectiveTreeId, Is.EqualTo("t/acme/" + BridgeHarness.AdoptedTree));
        Assert.That(plan.TryGetTree("missing", out _), Is.False);
        Assert.That(plan.TryGetTree(BridgeHarness.AdoptedTree, out _), Is.False, "a physical id is not a logical name");
    }

    [Test]
    public void Consent_comes_from_the_record_and_the_request_from_the_manifest()
    {
        var harness = new BridgeHarness();
        var consented = AppUiBridgeRequest.Create([new AppUiBridgeGrant(AppUiBridgeOperations.DataRead, "notes")]);

        var plan = Plan(harness.Manifest, harness.Record(consented: consented));

        Assert.That(plan.Consented, Is.EqualTo(consented));
        Assert.That(plan.Requested, Is.EqualTo(AppUiBridgeRequest.FromManifest(harness.Manifest)));
    }

    [Test]
    public void No_recorded_consent_is_the_empty_set()
    {
        var harness = new BridgeHarness();

        Assert.That(Plan(harness.Manifest, harness.Record() with { ConsentedBridge = null }).Consented.IsEmpty, Is.True);
    }

    [Test]
    public void A_malformed_bridge_request_is_treated_as_requesting_nothing()
    {
        var harness = new BridgeHarness();
        var manifest = BridgeHarness.DefaultManifest(UiTestManifests.Bridge("data.teleport"));

        Assert.That(Plan(manifest, harness.Record()).Requested.IsEmpty, Is.True);
    }

    [Test]
    public void Malformed_duplicate_and_non_adoptable_tree_declarations_are_not_addressable()
    {
        var harness = new BridgeHarness();
        var manifest = harness.Manifest with
        {
            Trees =
            [
                new AppTreeDeclaration { Name = "notes" },
                new AppTreeDeclaration { Name = "notes", AdoptedTreeId = "hijack" },
                new AppTreeDeclaration { Name = "Bad/Name" },
                new AppTreeDeclaration { Name = "sys", AdoptedTreeId = "sys-app-registry" },
                new AppTreeDeclaration { Name = "other", AdoptedTreeId = "a/billing/ledger" },
                null!,
            ],
        };

        var plan = Plan(manifest, harness.Record());

        Assert.That(plan.TryGetTree("notes", out var notes), Is.True);
        Assert.That(notes.LocalTreeId, Is.EqualTo("a/crm/notes"), "the first declaration wins");
        Assert.That(plan.TryGetTree("Bad/Name", out _), Is.False);
        Assert.That(plan.TryGetTree("sys", out _), Is.False);
        Assert.That(plan.TryGetTree("other", out _), Is.False);
    }

    [Test]
    public void A_grant_is_held_only_through_a_bound_group()
    {
        DefaultPlan().TryGetTree("notes", out var notes);

        Assert.That(notes.Allows(Groups(BridgeHarness.Editors), LatticeOperation.Write, "k", string.Empty), Is.True);
        Assert.That(notes.Allows(Groups(BridgeHarness.Viewers), LatticeOperation.Write, "k", string.Empty), Is.False);
        Assert.That(notes.Allows(Groups(BridgeHarness.Viewers), LatticeOperation.Read, "k", string.Empty), Is.True);
        Assert.That(notes.Allows(Groups(), LatticeOperation.Read, "k", string.Empty), Is.False);
        Assert.That(notes.Allows(null, LatticeOperation.Read, "k", string.Empty), Is.False);
        Assert.That(notes.Allows(Groups(BridgeHarness.Editors), LatticeOperation.None, "k", string.Empty), Is.False);
    }

    [Test]
    public void A_group_closure_that_is_not_a_set_is_matched_ordinally()
    {
        DefaultPlan().TryGetTree("notes", out var notes);

        Assert.That(notes.Allows(new List<string> { "other", BridgeHarness.Editors }, LatticeOperation.Write, "k", string.Empty), Is.True);
        Assert.That(notes.Allows(new List<string> { BridgeHarness.Editors.ToUpperInvariant() }, LatticeOperation.Write, "k", string.Empty), Is.False);
    }

    [Test]
    public void Every_operation_bit_requested_must_be_held()
    {
        DefaultPlan().TryGetTree("notes", out var notes);

        Assert.That(notes.Allows(Groups(BridgeHarness.Viewers), LatticeOperation.Read | LatticeOperation.Write, "k", string.Empty), Is.False);
        Assert.That(notes.Allows(Groups(BridgeHarness.Editors), LatticeOperation.Read | LatticeOperation.Write, "k", string.Empty), Is.True);
    }

    [Test]
    public void The_ceiling_is_intersected_with_every_role()
    {
        var harness = new BridgeHarness();
        var plan = Plan(harness.Manifest, harness.Record(ceiling: new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.Read | LatticeOperation.Telemetry | LatticeOperation.AppInstall,
        }));
        plan.TryGetTree("notes", out var notes);

        Assert.That(notes.Allows(Groups(BridgeHarness.Editors), LatticeOperation.Read, "k", string.Empty), Is.True);
        Assert.That(notes.Allows(Groups(BridgeHarness.Editors), LatticeOperation.Write, "k", string.Empty), Is.False);
        Assert.That(notes.Grants.Select(g => g.Operations), Is.All.EqualTo(LatticeOperation.Read), "scopeless capabilities are never role operations");
    }

    [Test]
    public void An_adopted_tree_needs_a_covering_exception_scope()
    {
        var harness = new BridgeHarness();
        Plan(harness.Manifest, harness.Record()).TryGetTree("legacy", out var covered);
        Plan(harness.Manifest, harness.Record(ceiling: BridgeHarness.DefaultCeiling(LatticeScope.Tree("elsewhere")))).TryGetTree("legacy", out var uncovered);
        Plan(harness.Manifest, harness.Record(ceiling: BridgeHarness.DefaultCeiling(LatticeScope.Key(BridgeHarness.AdoptedTree, "k")))).TryGetTree("legacy", out var keyOnly);
        Plan(harness.Manifest, harness.Record() with { Ceiling = null! }).TryGetTree("legacy", out var noCeiling);

        Assert.That(covered.Allows(Groups(BridgeHarness.Viewers), LatticeOperation.Read, "k", string.Empty), Is.True);
        Assert.That(uncovered.Grants, Is.Empty);
        Assert.That(keyOnly.Grants, Is.Empty, "a key exception does not cover a tree scope");
        Assert.That(noCeiling.Grants, Is.Empty);
    }

    [Test]
    public void A_prefix_or_key_exception_covers_only_scopes_inside_it()
    {
        var manifest = BridgeHarness.DefaultManifest() with
        {
            Roles =
            [
                new AppRoleDeclaration
                {
                    Name = "viewer",
                    Operations = LatticeOperation.Read,
                    Scopes =
                    [
                        new AppScopeTemplate { Tree = "legacy", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "p/q/" },
                        new AppScopeTemplate { Tree = "legacy", Kind = LatticeScopeKind.Key, KeyOrPrefix = "only" },
                        new AppScopeTemplate { Tree = "legacy", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "z/" },
                    ],
                },
            ],
        };
        var harness = new BridgeHarness(manifest);
        var record = harness.Record(ceiling: BridgeHarness.DefaultCeiling(
            LatticeScope.Prefix(BridgeHarness.AdoptedTree, "p/"),
            LatticeScope.Key(BridgeHarness.AdoptedTree, "only")));

        Plan(manifest, record).TryGetTree("legacy", out var legacy);

        Assert.That(legacy.Grants.Select(g => g.KeyOrPrefix), Is.EquivalentTo(new[] { "p/q/", "only" }));
    }

    [Test]
    public void A_prefix_grant_covers_keys_and_narrower_prefixes_only()
    {
        var grant = new AppBridgeInstallPlan.TreeGrant("g", LatticeOperation.Read, LatticeScopeKind.Prefix, "drafts/");

        Assert.That(grant.Covers("drafts/1", string.Empty), Is.True);
        Assert.That(grant.Covers("draft", string.Empty), Is.False);
        Assert.That(grant.Covers(null, "drafts/2025/"), Is.True);
        Assert.That(grant.Covers(null, "drafts"), Is.False);
        Assert.That(grant.Covers(null, string.Empty), Is.False);
    }

    [Test]
    public void A_key_grant_covers_its_key_and_never_a_prefix()
    {
        var grant = new AppBridgeInstallPlan.TreeGrant("g", LatticeOperation.Read, LatticeScopeKind.Key, "pinned");

        Assert.That(grant.Covers("pinned", string.Empty), Is.True);
        Assert.That(grant.Covers("pinned2", string.Empty), Is.False);
        Assert.That(grant.Covers(null, "pinned"), Is.False);
    }

    [Test]
    public void A_tree_grant_covers_everything_and_an_unknown_kind_nothing()
    {
        Assert.That(new AppBridgeInstallPlan.TreeGrant("g", LatticeOperation.Read, LatticeScopeKind.Tree, null).Covers(null, string.Empty), Is.True);
        Assert.That(new AppBridgeInstallPlan.TreeGrant("g", LatticeOperation.Read, (LatticeScopeKind)99, "k").Covers("k", string.Empty), Is.False);
    }

    [Test]
    public void A_role_scope_naming_another_app_never_reaches_this_apps_tree()
    {
        var manifest = BridgeHarness.DefaultManifest() with
        {
            Roles =
            [
                new AppRoleDeclaration
                {
                    Name = "editor",
                    Operations = LatticeOperation.Write,
                    Scopes = [new AppScopeTemplate { Tree = "notes", App = AppSlug.Parse("billing") }],
                },
            ],
        };
        var harness = new BridgeHarness(manifest);

        Plan(manifest, harness.Record()).TryGetTree("notes", out var notes);

        Assert.That(notes.Grants, Is.Empty);
    }
}
