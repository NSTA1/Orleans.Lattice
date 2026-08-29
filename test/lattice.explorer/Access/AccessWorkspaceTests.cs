using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.Access.Workspace;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// The Access plugin's view state, lifted out of the panel's code-behind so the
/// split views share one model. These cover the behaviour the Razor split must
/// not have changed: the gate is read fail-closed and re-read on a store change,
/// the sub-surface switch closes open forms and loads on demand, and every
/// operation stays guarded by the advisory access decision.
/// </summary>
/// <remarks>
/// Nothing here depends on wall-clock timing: the workspace issues its own
/// awaits directly and the subject-picker debounce (the one timing seam) is not
/// part of this surface.
/// </remarks>
[TestFixture]
public sealed class AccessWorkspaceTests
{
    private static readonly AuthGroup Admins = new() { GroupId = "admins", DisplayName = "Administrators" };
    private static readonly AuthGroup Readers = new() { GroupId = "readers" };

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var store = new ExplorerPluginAccessStore();

        Assert.Multiple(() =>
        {
            Assert.That(() => new AccessWorkspace(null!, store), Throws.ArgumentNullException);
            Assert.That(() => new AccessWorkspace(StubDomain(), null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Before_any_probe_the_workspace_is_denied_and_loads_nothing()
    {
        var domain = StubDomain();
        using var workspace = new AccessWorkspace(domain, new ExplorerPluginAccessStore());

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.False, "an unprobed key must fail closed");
            Assert.That(workspace.AuthenticationRequired, Is.False);
            Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(AccessSurfaces.Groups));
            Assert.That(workspace.Groups, Is.Empty);
            Assert.That(workspace.Trees, Is.Empty);
        });
    }

    [Test]
    public async Task InitializeAsync_does_not_touch_the_cluster_while_the_gate_denies()
    {
        var domain = StubDomain();
        using var workspace = new AccessWorkspace(domain, new ExplorerPluginAccessStore());

        await workspace.InitializeAsync();

        await domain.Membership.DidNotReceive().GetAccessModelAsync(Arg.Any<CancellationToken>());
        await domain.Catalog.DidNotReceiveWithAnyArgs().LoadAsync(default, default, default);
    }

    [Test]
    public void An_authentication_required_decision_is_distinguished_from_a_denial()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(AccessPluginKeys.PluginId, ExplorerPluginAccess.AuthenticationRequired);

        using var workspace = new AccessWorkspace(StubDomain(), store);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.False);
            Assert.That(
                workspace.AuthenticationRequired,
                Is.True,
                "the panel prompts a sign-in for this state rather than greying out");
        });
    }

    [Test]
    public async Task InitializeAsync_loads_the_model_the_trees_and_the_active_surface_when_allowed()
    {
        var domain = StubDomain(groups: [Admins, Readers], trees: ["orders", "audit"]);
        using var workspace = new AccessWorkspace(domain, AllowedStore());

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.True);
            Assert.That(workspace.Trees.Select(t => t.Id), Is.EqualTo(new[] { "orders", "audit" }));
            Assert.That(workspace.Groups.Select(g => g.GroupId), Is.EqualTo(new[] { "admins", "readers" }));
            Assert.That(workspace.AccessModel.DirectoryAvailable, Is.True);
        });
    }

    [Test]
    public async Task The_tree_catalog_excludes_restore_shadows()
    {
        var domain = StubDomain(trees: ["orders"]);
        domain.Catalog.LoadAsync(CatalogKind.Trees, Arg.Any<string?>(), Arg.Any<int>()).Returns(new CatalogPage
        {
            Items =
            [
                new CatalogItem { Id = "orders", Kind = CatalogKind.Trees },
                new CatalogItem { Id = "orders-shadow", Kind = CatalogKind.Trees, RestoreShadowOfTreeId = "orders" },
            ],
        });
        using var workspace = new AccessWorkspace(domain, AllowedStore());

        await workspace.InitializeAsync();

        Assert.That(workspace.Trees.Select(t => t.Id), Is.EqualTo(new[] { "orders" }));
    }

    [Test]
    public async Task A_catalog_failure_surfaces_as_a_retryable_error_rather_than_throwing()
    {
        var domain = StubDomain();
        domain.Catalog
            .LoadAsync(CatalogKind.Trees, Arg.Any<string?>(), Arg.Any<int>())
            .Returns<CatalogPage>(_ => throw new InvalidOperationException("no endpoint"));
        using var workspace = new AccessWorkspace(domain, AllowedStore());

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.TreesError, Is.EqualTo("no endpoint"));
            Assert.That(workspace.TreesLoading, Is.False);
        });
    }

    [Test]
    public async Task Selecting_a_surface_closes_an_open_form_and_clears_the_last_result()
    {
        var domain = StubDomain(groups: [Admins]);
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        await workspace.InitializeAsync();
        workspace.NewGroup();

        await workspace.SelectSurfaceAsync(AccessSurfaces.Policies);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(AccessSurfaces.Policies));
            Assert.That(workspace.GroupFormOpen, Is.False);
            Assert.That(workspace.RuleFormOpen, Is.False);
            Assert.That(workspace.LastResult, Is.Null);
        });
    }

    [Test]
    public async Task Selecting_the_active_surface_again_is_a_no_op()
    {
        var domain = StubDomain(groups: [Admins]);
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        await workspace.InitializeAsync();
        workspace.NewGroup();

        await workspace.SelectSurfaceAsync(AccessSurfaces.Groups);

        Assert.That(workspace.GroupFormOpen, Is.True, "re-selecting the active surface must not close the form");
    }

    [Test]
    public async Task Selecting_the_policies_surface_loads_its_rules_ranked_by_precedence()
    {
        var domain = StubDomain(groups: [Admins], rules:
        [
            new LatticeAuthorizationRule(
                "allow-all", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("orders"),
                LatticeOperation.Read, LatticeEffect.Allow),
            new LatticeAuthorizationRule(
                "deny-key", LatticeSubjectSelector.User("alice"), LatticeScope.Key("orders", "k"),
                LatticeOperation.Read, LatticeEffect.Deny),
        ]);
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        await workspace.InitializeAsync();

        await workspace.SelectSurfaceAsync(AccessSurfaces.Policies);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.RankedRules, Has.Count.EqualTo(2));
            Assert.That(
                workspace.RankedRules[0].Rule.RuleId,
                Is.EqualTo("deny-key"),
                "the most specific scope ranks first");
            Assert.That(workspace.RankedRules[0].DenyOverrides, Is.True);
        });
    }

    [Test]
    public async Task A_freshly_opened_gate_populates_the_trees_without_a_manual_refresh()
    {
        var store = new ExplorerPluginAccessStore();
        var domain = StubDomain(trees: ["orders"]);
        using var workspace = new AccessWorkspace(domain, store);
        await workspace.InitializeAsync();
        Assume.That(workspace.Trees, Is.Empty);

        var loaded = new TaskCompletionSource();
        workspace.Changed += () =>
        {
            if (workspace.Trees.Count > 0)
            {
                loaded.TrySetResult();
            }
        };

        store.Set(AccessPluginKeys.PluginId, ExplorerPluginAccess.Allowed);
        await loaded.Task.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.True);
            Assert.That(workspace.Trees.Select(t => t.Id), Is.EqualTo(new[] { "orders" }));
        });
    }

    [Test]
    public void A_scoped_decision_for_this_plugin_does_not_re_gate_the_workspace()
    {
        var store = new ExplorerPluginAccessStore();
        using var workspace = new AccessWorkspace(StubDomain(), store);

        store.Set(AccessPluginKeys.PluginId, AccessPluginKeys.DirectoryScope, ExplorerPluginAccess.Allowed);

        Assert.That(
            workspace.Allowed,
            Is.False,
            "a sub-capability decision is not the plugin-level gate");
    }

    [Test]
    public void A_sibling_plugins_decision_does_not_re_gate_the_workspace()
    {
        var store = new ExplorerPluginAccessStore();
        using var workspace = new AccessWorkspace(StubDomain(), store);

        store.Set("orleans.lattice.backups", ExplorerPluginAccess.Allowed);

        Assert.That(workspace.Allowed, Is.False);
    }

    [Test]
    public void Disposing_the_workspace_detaches_it_from_the_store()
    {
        var store = new ExplorerPluginAccessStore();
        var workspace = new AccessWorkspace(StubDomain(), store);
        workspace.Dispose();

        store.Set(AccessPluginKeys.PluginId, ExplorerPluginAccess.Allowed);

        Assert.That(workspace.Allowed, Is.False, "a disposed workspace must stop tracking the gate");
    }

    [Test]
    public async Task Saving_a_group_is_refused_while_the_gate_denies()
    {
        var domain = StubDomain();
        using var workspace = new AccessWorkspace(domain, new ExplorerPluginAccessStore());
        workspace.GroupIdInput = "admins";

        await workspace.SaveGroupAsync();

        await domain.Membership.DidNotReceive().UpsertGroupAsync(Arg.Any<AuthGroup>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Saving_a_new_group_fails_closed_when_the_directory_does_not_know_the_id()
    {
        var domain = StubDomain();
        domain.Membership
            .ResolveDirectoryPrincipalAsync("ghost", Arg.Any<CancellationToken>())
            .Returns((DirectoryPrincipalDescriptor?)null);
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        await workspace.InitializeAsync();
        workspace.NewGroup();
        workspace.GroupIdInput = "ghost";

        await workspace.SaveGroupAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(workspace.GroupCreateError, Is.EqualTo(AccessCreateModel.NoSuchPrincipalReason));
            await domain.Membership
                .DidNotReceive()
                .UpsertGroupAsync(Arg.Any<AuthGroup>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task Saving_a_new_group_writes_it_and_reports_a_friendly_status()
    {
        var domain = StubDomain(groups: [Admins]);
        domain.Membership
            .ResolveDirectoryPrincipalAsync("admins", Arg.Any<CancellationToken>())
            .Returns(new DirectoryPrincipalDescriptor
            {
                Id = "admins",
                DisplayName = "Administrators",
                Kind = DirectoryPrincipalKind.Group,
            });
        domain.Membership
            .UpsertGroupAsync(Arg.Any<AuthGroup>(), Arg.Any<CancellationToken>())
            .Returns(AccessOperationResult.Success("ok"));
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        await workspace.InitializeAsync();
        workspace.NewGroup();
        workspace.GroupIdInput = "admins";
        workspace.GroupDisplayInput = "Administrators";

        await workspace.SaveGroupAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastResult!.IsSuccess, Is.True);
            Assert.That(workspace.LastResult.Message, Is.EqualTo("Saved group 'Administrators'."));
            Assert.That(workspace.SelectedGroupId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public async Task A_denied_list_read_folds_into_a_denied_status_rather_than_throwing()
    {
        var domain = StubDomain();
        domain.Membership
            .ListGroupsAsync(Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new AccessListView<AuthGroup>
            {
                Status = AccessOperationStatus.Denied,
                Message = "not permitted",
            });
        using var workspace = new AccessWorkspace(domain, AllowedStore());

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastResult!.Status, Is.EqualTo(AccessOperationStatus.Denied));
            Assert.That(workspace.LastResult.Message, Is.EqualTo("not permitted"));
        });
    }

    [Test]
    public void Selecting_a_tree_pins_it_without_issuing_a_request()
    {
        var domain = StubDomain();
        using var workspace = new AccessWorkspace(domain, AllowedStore());

        workspace.SelectTree("orders");

        Assert.That(workspace.SelectedTreeId, Is.EqualTo("orders"));
    }

    [Test]
    public void The_rule_form_requires_an_id_a_subject_a_tree_and_an_operation()
    {
        using var workspace = new AccessWorkspace(StubDomain(), AllowedStore());
        workspace.NewRule();

        Assert.That(workspace.CanSaveRule(), Is.False);

        workspace.RuleIdInput = "r1";
        workspace.RuleSubjectId = "alice";
        Assert.That(workspace.CanSaveRule(), Is.False, "a tree and an operation are still missing");

        workspace.SelectTree("orders");
        workspace.ToggleOperation(LatticeOperation.Read, enabled: true);
        Assert.That(workspace.CanSaveRule(), Is.True);

        workspace.ToggleOperation(LatticeOperation.Read, enabled: false);
        Assert.That(workspace.CanSaveRule(), Is.False);
    }

    [Test]
    public void The_delegation_affordance_supplies_its_own_tree_scope_and_operation()
    {
        using var workspace = new AccessWorkspace(StubDomain(), AllowedStore());
        workspace.NewRule();
        workspace.RuleIdInput = "delegate";
        workspace.RuleSubjectId = "alice";
        workspace.RuleDelegateAccessAdmin = true;

        Assert.That(
            workspace.CanSaveRule(),
            Is.True,
            "the affordance supplies the reserved tree, whole-tree scope, and Admin");
    }

    [Test]
    public void The_all_trees_affordance_supplies_the_scope_but_still_needs_an_operation()
    {
        using var workspace = new AccessWorkspace(StubDomain(), AllowedStore());
        workspace.NewRule();
        workspace.RuleIdInput = "cluster-wide";
        workspace.RuleSubjectId = "alice";
        workspace.RuleAllTrees = true;

        Assert.That(workspace.CanSaveRule(), Is.False);

        workspace.ToggleOperation(LatticeOperation.Read, enabled: true);
        Assert.That(workspace.CanSaveRule(), Is.True);
    }

    [Test]
    public void Editing_an_existing_delegation_rule_reflects_the_affordance()
    {
        using var workspace = new AccessWorkspace(StubDomain(), AllowedStore());

        workspace.EditRule(new LatticeAuthorizationRule(
            "delegate",
            LatticeSubjectSelector.Group("ops"),
            LatticeScope.Tree(LatticeAuthReservedTrees.PolicyTreeId),
            LatticeOperation.Admin,
            LatticeEffect.Allow));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.RuleFormOpen, Is.True);
            Assert.That(workspace.EditingExistingRule, Is.True);
            Assert.That(workspace.RuleDelegateAccessAdmin, Is.True);
            Assert.That(workspace.RuleAllTrees, Is.False);
            Assert.That(workspace.HasRuleOperation(LatticeOperation.Admin), Is.True);
            Assert.That(workspace.RuleSubjectKind, Is.EqualTo(LatticeSubjectSelectorKind.Group));
        });
    }

    [Test]
    public void Editing_an_existing_all_trees_rule_reflects_the_affordance()
    {
        using var workspace = new AccessWorkspace(StubDomain(), AllowedStore());

        workspace.EditRule(new LatticeAuthorizationRule(
            "cluster-wide",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.ClusterWide(),
            LatticeOperation.Read,
            LatticeEffect.Allow));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.RuleAllTrees, Is.True);
            Assert.That(workspace.RuleDelegateAccessAdmin, Is.False);
        });
    }

    [Test]
    public void Cancelling_the_rule_form_clears_it()
    {
        using var workspace = new AccessWorkspace(StubDomain(), AllowedStore());
        workspace.NewRule();
        workspace.RuleIdInput = "r1";
        workspace.ToggleOperation(LatticeOperation.Write, enabled: true);

        workspace.CancelRuleForm();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.RuleFormOpen, Is.False);
            Assert.That(workspace.RuleIdInput, Is.Empty);
            Assert.That(workspace.HasRuleOperation(LatticeOperation.Write), Is.False);
        });
    }

    [Test]
    public async Task Explain_and_effective_are_refused_until_their_inputs_are_supplied()
    {
        var domain = StubDomain();
        using var workspace = new AccessWorkspace(domain, AllowedStore());

        Assert.Multiple(() =>
        {
            Assert.That(workspace.CanExplain, Is.False);
            Assert.That(workspace.CanListEffective, Is.False);
        });

        await workspace.RunExplainAsync();
        await workspace.RunEffectiveAsync();

        Assert.Multiple(async () =>
        {
            await domain.Policy.DidNotReceiveWithAnyArgs().ExplainAsync(default!, default, default!);
            await domain.Policy.DidNotReceiveWithAnyArgs().EffectivePermissionsAsync(default!);
        });
    }

    [Test]
    public async Task Explain_renders_the_facades_verdict_verbatim()
    {
        var domain = StubDomain();
        domain.Policy
            .ExplainAsync(
                "alice",
                LatticeOperation.Read,
                Arg.Any<LatticeScope>(),
                LatticeSubjectSelectorKind.User,
                Arg.Any<CancellationToken>())
            .Returns(new ExplainView
            {
                Status = AccessOperationStatus.Succeeded,
                Explanation = new AuthExplanation
                {
                    Allowed = true,
                    SubjectId = "alice",
                    Scope = LatticeScope.Tree("orders"),
                    Reason = "matched rule r1",
                    MatchedRules = [],
                    GroupIds = [],
                },
            });
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        workspace.SelectTree("orders");
        workspace.ExplainSubjectId = "alice";

        await workspace.RunExplainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Explanation!.Allowed, Is.True);
            Assert.That(workspace.Explanation.Reason, Is.EqualTo("matched rule r1"));
            Assert.That(workspace.Effective, Is.Null, "running Explain clears the effective-permissions view");
        });
    }

    [Test]
    public async Task A_failed_explain_folds_into_a_status_rather_than_a_verdict()
    {
        var domain = StubDomain();
        domain.Policy
            .ExplainAsync(
                Arg.Any<string>(),
                Arg.Any<LatticeOperation>(),
                Arg.Any<LatticeScope>(),
                Arg.Any<LatticeSubjectSelectorKind>(),
                Arg.Any<CancellationToken>())
            .Returns(new ExplainView { Status = AccessOperationStatus.Denied, Message = "not permitted" });
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        workspace.SelectTree("orders");
        workspace.ExplainSubjectId = "alice";

        await workspace.RunExplainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Explanation, Is.Null);
            Assert.That(workspace.LastResult!.Status, Is.EqualTo(AccessOperationStatus.Denied));
        });
    }

    [Test]
    public async Task Effective_permissions_rank_the_returned_rules()
    {
        var domain = StubDomain();
        domain.Policy.EffectivePermissionsAsync("alice", Arg.Any<CancellationToken>()).Returns(
            new EffectivePermissionsView
            {
                Status = AccessOperationStatus.Succeeded,
                Permissions = new AuthEffectivePermissions
                {
                    SubjectId = "alice",
                    GroupIds = [],
                    Rules =
                    [
                        new LatticeAuthorizationRule(
                            "r1", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("orders"),
                            LatticeOperation.Read, LatticeEffect.Allow),
                    ],
                },
            });
        using var workspace = new AccessWorkspace(domain, AllowedStore());
        workspace.ExplainSubjectId = "alice";

        await workspace.RunEffectiveAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Effective!.SubjectId, Is.EqualTo("alice"));
            Assert.That(workspace.EffectiveRankedRules, Has.Count.EqualTo(1));
            Assert.That(workspace.Explanation, Is.Null);
        });
    }

    [Test]
    public void ResultClass_maps_each_status_to_its_banner_modifier()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessWorkspace.ResultClass(AccessOperationStatus.Succeeded), Is.EqualTo("is-success"));
            Assert.That(AccessWorkspace.ResultClass(AccessOperationStatus.Denied), Is.EqualTo("is-denied"));
            Assert.That(AccessWorkspace.ResultClass(AccessOperationStatus.Failed), Is.EqualTo("is-failed"));
        });
    }

    [Test]
    public void The_member_picker_kind_bridges_the_two_kind_vocabularies()
    {
        using var workspace = new AccessWorkspace(StubDomain(), AllowedStore());

        Assert.That(workspace.MemberPickerKind, Is.EqualTo(LatticeSubjectSelectorKind.User));

        workspace.MemberPickerKind = LatticeSubjectSelectorKind.Group;
        Assert.That(workspace.MemberPickerKind, Is.EqualTo(LatticeSubjectSelectorKind.Group));
    }

    private static ExplorerPluginAccessStore AllowedStore()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(AccessPluginKeys.PluginId, ExplorerPluginAccess.Allowed);
        return store;
    }

    /// <summary>
    /// A domain whose reads all succeed with the supplied data. Substituted at
    /// the plugin's own contract, which is the whole of its reach - so a test
    /// never has to stand up a connection, a channel, or a container.
    /// </summary>
    private static IAccessDomain StubDomain(
        IReadOnlyList<AuthGroup>? groups = null,
        IReadOnlyList<string>? trees = null,
        IReadOnlyList<LatticeAuthorizationRule>? rules = null) =>
        StubAccessDomain.Create(groups, trees, rules);
}
