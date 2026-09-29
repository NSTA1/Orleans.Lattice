using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// Regression coverage for #3902: an app role is held by binding, not by capability, and the workspace (whose
/// role report is what a frame is told it holds), the shared evaluator and the bridge all agree. A caller holds
/// a role if and only if it is a member of a group the install binds to that role; rights it holds under any
/// other rule never make it hold one.
/// </summary>
/// <remarks>
/// Uses the <see cref="BridgeHarness"/> install: roles <c>viewer</c>, <c>editor</c> and <c>drafter</c> bound to
/// <c>g-viewers</c>, <c>g-editors</c> and <c>g-drafters</c>.
/// </remarks>
[TestFixture]
public sealed class AppRoleHeldByBindingTests
{
    private const string Bob = "bob";
    private const LatticeOperation Everything =
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete;

    private static LatticeAppWorkspace Workspace(BridgeHarness harness) => new(
        new AppRoleGrantEvaluator(harness.Projection, harness.Sources, harness.Gate), harness.Sources, harness.Tenants, harness.Membership);

    private static AppRoleGrantEvaluator Evaluator(BridgeHarness harness) => new(harness.Projection, harness.Sources, harness.Gate);

    /// <summary>Grants <paramref name="subject"/> every operation on every tree the app declares, under no app rule.</summary>
    private static void GrantBroadRights(BridgeHarness harness, string subject)
    {
        foreach (var tree in new[] { BridgeHarness.NotesTree, BridgeHarness.AdoptedTree, "a/crm/archive" })
        {
            foreach (var operation in new[] { LatticeOperation.Read, LatticeOperation.RangeRead, LatticeOperation.Write, LatticeOperation.Delete })
            {
                harness.Gate.Grant(subject, tree, operation);
            }
        }
    }

    private static async Task<string[]> ReportedRolesAsync(BridgeHarness harness)
    {
        var apps = await Workspace(harness).ListMyAppsAsync();
        return apps.IsEmpty ? [] : [.. apps.Single().Roles];
    }

    private static async Task<string[]> HeldRolesAsync(BridgeHarness harness, LatticeSubject subject)
    {
        var evaluation = await Evaluator(harness).EvaluateAsync(TenantId.Default, AppSlug.Parse(BridgeHarness.Slug), subject, CancellationToken.None);
        return evaluation is null ? [] : [.. evaluation.HeldRoles];
    }

    private static LatticeSubject Subject(string id, params string[] groups) => new(id, new HashSet<string>(groups, StringComparer.Ordinal));

    [Test]
    public async Task A_caller_with_broad_rights_bound_only_to_viewer_holds_viewer_only()
    {
        var harness = new BridgeHarness().Installed(Bob, BridgeHarness.Viewers);
        GrantBroadRights(harness, Bob);

        Assert.Multiple(async () =>
        {
            Assert.That(await ReportedRolesAsync(harness), Is.EqualTo(new[] { "viewer" }), "the workspace (and so the frame) report");
            Assert.That(await HeldRolesAsync(harness, Subject(Bob, BridgeHarness.Viewers)), Is.EqualTo(new[] { "viewer" }), "the shared evaluation");
        });

        // The bridge agrees: the viewer reads, and the write the broad rights would allow is refused.
        harness.Seed(BridgeHarness.NotesTree, "n1", [1]);
        Assert.That(await harness.Bridge.GetAsync(BridgeHarness.Target(), "n1"), Is.Not.Null);
        var denied = Assert.ThrowsAsync<AppBridgeException>(() => harness.Bridge.SetAsync(BridgeHarness.Target(), "n1", new byte[] { 2 }));
        Assert.That(denied!.Failure, Is.EqualTo(AppBridgeFailure.Denied));
    }

    [Test]
    public async Task A_bound_editor_holds_editor_and_the_bridge_honours_it()
    {
        var harness = new BridgeHarness().Installed("alice", BridgeHarness.Editors);

        // What the compiled app-owned rule for the editor binding grants.
        foreach (var operation in new[] { LatticeOperation.Read, LatticeOperation.RangeRead, LatticeOperation.Write, LatticeOperation.Delete })
        {
            harness.Gate.Grant("alice", BridgeHarness.NotesTree, operation);
        }

        Assert.That(await ReportedRolesAsync(harness), Is.EqualTo(new[] { "editor" }));

        await harness.Bridge.SetAsync(BridgeHarness.Target(), "n1", new byte[] { 1 });
        Assert.That(harness.Store(BridgeHarness.NotesTree), Contains.Key("n1"));
    }

    [Test]
    public async Task A_member_of_several_bound_groups_holds_each_role_in_manifest_order()
    {
        var harness = new BridgeHarness().Installed("alice", BridgeHarness.Drafters, BridgeHarness.Viewers);

        Assert.That(await ReportedRolesAsync(harness), Is.EqualTo(new[] { "viewer", "drafter" }));
    }

    [Test]
    public async Task Rebinding_a_role_moves_it_on_the_next_evaluation()
    {
        var harness = new BridgeHarness().Installed("carol", "g-reviewers");
        GrantBroadRights(harness, "carol");
        var workspace = Workspace(harness);
        Assert.That(await workspace.ListMyAppsAsync(), Is.Empty, "carol's group is not bound yet");

        var rebound = harness.Record() with
        {
            Revision = BridgeHarness.Revision + 1,
            RoleBindings =
            [
                AppRoleBinding.Create("viewer", "g-reviewers"),
                AppRoleBinding.Create("editor", BridgeHarness.Editors),
                AppRoleBinding.Create("drafter", BridgeHarness.Drafters),
            ],
        };
        harness.Publish(rebound);
        var evaluator = Evaluator(harness);

        Assert.Multiple(async () =>
        {
            Assert.That((await workspace.ListMyAppsAsync()).Single().Roles, Is.EqualTo(new[] { "viewer" }), "the re-bound group now holds viewer, through the same cached evaluator");
            var previous = await evaluator.EvaluateAsync(TenantId.Default, AppSlug.Parse(BridgeHarness.Slug), Subject("dave", BridgeHarness.Viewers), CancellationToken.None);
            Assert.That(previous!.HeldRoles, Is.Empty, "the previous group no longer does");
        });

        harness.Seed(BridgeHarness.NotesTree, "n1", [1]);
        Assert.That(await harness.Bridge.GetAsync(BridgeHarness.Target(revision: BridgeHarness.Revision + 1), "n1"), Is.Not.Null);
    }

    /// <summary>
    /// The #3863 shape: a key-filtered allow that spells a prefix, or any allow at all, outside the app's own
    /// rules never makes a caller hold a role - the gate is not consulted.
    /// </summary>
    [Test]
    public async Task Rights_outside_the_app_rules_never_hold_a_role()
    {
        var filtered = new BridgeHarness().Installed(Bob, "g-operators");
        filtered.Gate.Override = static request => LatticeAccessDecision.Filtered(key => key.StartsWith("drafts/", StringComparison.Ordinal));
        var allowEverything = new BridgeHarness().Installed(Bob, "g-operators");
        GrantBroadRights(allowEverything, Bob);

        Assert.Multiple(async () =>
        {
            Assert.That(await ReportedRolesAsync(filtered), Is.Empty);
            Assert.That(await ReportedRolesAsync(allowEverything), Is.Empty);
            Assert.That(allowEverything.Gate.Requests, Is.Zero, "holding a role never consults the access gate");
        });
    }

    [Test]
    public async Task A_caller_with_no_resolved_group_holds_no_role()
    {
        var harness = new BridgeHarness().Installed(Bob);
        GrantBroadRights(harness, Bob);

        Assert.Multiple(async () =>
        {
            Assert.That(await ReportedRolesAsync(harness), Is.Empty);
            Assert.That(await HeldRolesAsync(harness, LatticeSubject.Anonymous with { GroupIds = [BridgeHarness.Viewers] }), Is.Empty, "the anonymous subject holds nothing");
        });
    }

    [Test]
    public async Task An_unreadable_binding_binds_nothing()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Viewers, string.Empty);
        GrantBroadRights(harness, "alice");
        harness.Publish(harness.Record() with
        {
            RoleBindings =
            [
                new AppRoleBinding { RoleName = "viewer", GroupId = string.Empty },
                null!,
                AppRoleBinding.Create("editor", BridgeHarness.Editors),
            ],
        });

        Assert.That(await ReportedRolesAsync(harness), Is.Empty);
    }

    [Test]
    public async Task A_role_the_ceiling_no_longer_covers_is_not_held()
    {
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);
        GrantBroadRights(harness, "alice");
        harness.Publish(harness.Record(ceiling: new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.None,
            ApprovedExceptionScopes = [LatticeScope.Tree(BridgeHarness.AdoptedTree)],
        }));

        Assert.That(await ReportedRolesAsync(harness), Is.Empty);
    }
}
