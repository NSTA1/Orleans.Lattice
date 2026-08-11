using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Integration coverage for cluster-wide all-trees grants through the control
/// facade (issue #1349). With
/// <see cref="LatticeAuthOptions.AllTreesGrantsEnabled"/> set, a bootstrap
/// administrator authors a whole-tree <c>Tree:*</c> rule, and the facade's
/// Explain / EffectivePermissions / per-tree listing then reflect the resolved
/// verdict for an ordinary application tree - honouring the four-tier precedence
/// (global deny wins, specific overrides global allow, global allow otherwise),
/// while the reserved authorization namespace is never governed by the tier.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthAdminAllTreesTests
{
    private const string PolicyTree = "sys-auth-policy";
    private const string AppTree = "orders";

    private AuthAdminAllTreesClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthAdminAllTreesClusterFixture();
        await _fixture.InitializeAsync();

        using (AuthAdminAllTreesClusterFixture.AsSubject(AuthAdminAllTreesClusterFixture.BootstrapAdmin))
        {
            // alice: a plain all-trees Read allow, no specific rule (tier 3 allow).
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "all-alice-read",
                LatticeSubjectSelector.User("alice"),
                LatticeScope.ClusterWide(),
                LatticeOperation.Read,
                LatticeEffect.Allow));

            // bob: an all-trees Read allow, overridden by a specific deny on the
            // application tree (tier 2 specific deny beats tier 3 global allow).
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "all-bob-read",
                LatticeSubjectSelector.User("bob"),
                LatticeScope.ClusterWide(),
                LatticeOperation.Read,
                LatticeEffect.Allow));
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "bob-orders-deny",
                LatticeSubjectSelector.User("bob"),
                LatticeScope.Tree(AppTree),
                LatticeOperation.Read,
                LatticeEffect.Deny));

            // carol: an all-trees Read deny that a specific allow cannot override
            // (tier 1 global deny wins outright).
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "all-carol-deny",
                LatticeSubjectSelector.User("carol"),
                LatticeScope.ClusterWide(),
                LatticeOperation.Read,
                LatticeEffect.Deny));
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "carol-orders-allow",
                LatticeSubjectSelector.User("carol"),
                LatticeScope.Tree(AppTree),
                LatticeOperation.Read,
                LatticeEffect.Allow));
        }

        await _fixture.RebuildPolicyAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<AuthExplanation> ExplainAsync(string subject, LatticeOperation op, LatticeScope scope)
    {
        using (AuthAdminAllTreesClusterFixture.AsSubject(AuthAdminAllTreesClusterFixture.BootstrapAdmin))
        {
            return await _fixture.Admin.ExplainAsync(subject, op, scope);
        }
    }

    [Test]
    public async Task All_trees_allow_is_honoured_on_an_application_tree()
    {
        var explanation = await ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree(AppTree));

        Assert.That(explanation.Allowed, Is.True,
            "a Tree:* allow grants Read on every non-system tree when the flag is enabled");
    }

    [Test]
    public async Task Specific_deny_overrides_an_all_trees_allow()
    {
        var explanation = await ExplainAsync("bob", LatticeOperation.Read, LatticeScope.Tree(AppTree));

        Assert.That(explanation.Allowed, Is.False,
            "a specific-tree deny beats a global all-trees allow");
    }

    [Test]
    public async Task All_trees_deny_is_not_overridden_by_a_specific_allow()
    {
        var explanation = await ExplainAsync("carol", LatticeOperation.Read, LatticeScope.Tree(AppTree));

        Assert.That(explanation.Allowed, Is.False,
            "a global all-trees deny wins outright over a specific-tree allow");
    }

    [Test]
    public async Task All_trees_grant_does_not_confer_a_different_operation()
    {
        // alice's grant is Read-only; a Write request falls through to DefaultEffect (Deny).
        var explanation = await ExplainAsync("alice", LatticeOperation.Write, LatticeScope.Tree(AppTree));

        Assert.That(explanation.Allowed, Is.False,
            "operation-bit separation: a Tree:* Read grant does not confer Write");
    }

    [Test]
    public async Task All_trees_allow_does_not_leak_into_the_reserved_control_plane()
    {
        // The all-trees tier is skipped for the reserved authorization namespace, so a
        // Tree:* allow can never satisfy control-plane admin authorization.
        var explanation = await ExplainAsync("alice", LatticeOperation.Admin, LatticeScope.Tree(PolicyTree));

        Assert.That(explanation.Allowed, Is.False,
            "a Tree:* allow must never govern the reserved sys-auth-* control plane");
    }

    [Test]
    public async Task Per_tree_listing_folds_the_all_trees_rule_exactly_once()
    {
        AuthRulePage page;
        using (AuthAdminAllTreesClusterFixture.AsSubject(AuthAdminAllTreesClusterFixture.BootstrapAdmin))
        {
            page = await _fixture.Admin.ListRulesForTreeAsync(AppTree, new AuthPageRequest());
        }

        var aliceAllTrees = page.Entries.Where(r => r.RuleId == "all-alice-read").ToList();

        Assert.Multiple(() =>
        {
            Assert.That(aliceAllTrees, Has.Count.EqualTo(1),
                "the folded Tree:* rule is cited once, not double-counted");
            Assert.That(aliceAllTrees[0].Scope.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId),
                "the folded rule retains its cluster-wide scope so it is identifiable as all-trees");
        });
    }

    [Test]
    public async Task Effective_permissions_include_the_all_trees_rule()
    {
        AuthEffectivePermissions permissions;
        using (AuthAdminAllTreesClusterFixture.AsSubject(AuthAdminAllTreesClusterFixture.BootstrapAdmin))
        {
            permissions = await _fixture.Admin.EffectivePermissionsAsync("alice");
        }

        Assert.That(permissions.Rules.Select(r => r.RuleId), Does.Contain("all-alice-read"),
            "a subject's cluster-wide grant appears in its effective permissions");
    }

    [Test]
    public async Task Explain_surfaces_the_enabled_all_trees_posture()
    {
        var explanation = await ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree(AppTree));

        Assert.That(explanation.Posture.AllTreesGrantsEnabled, Is.True,
            "explain must report the all-trees tier as enabled on a cluster where the flag is set");
    }

    [Test]
    public async Task Effective_permissions_surface_the_enabled_all_trees_posture()
    {
        AuthEffectivePermissions permissions;
        using (AuthAdminAllTreesClusterFixture.AsSubject(AuthAdminAllTreesClusterFixture.BootstrapAdmin))
        {
            permissions = await _fixture.Admin.EffectivePermissionsAsync("alice");
        }

        Assert.That(permissions.Posture.AllTreesGrantsEnabled, Is.True,
            "effective-permissions must report the all-trees tier as enabled");
    }

    [Test]
    public async Task Access_model_surfaces_the_enabled_all_trees_posture()
    {
        AccessModelDescriptor model;
        using (AuthAdminAllTreesClusterFixture.AsSubject(AuthAdminAllTreesClusterFixture.BootstrapAdmin))
        {
            model = await _fixture.Admin.GetAccessModelAsync();
        }

        Assert.That(model.AllTreesGrantsEnabled, Is.True,
            "the access model must report the all-trees tier as enabled so the Explorer can badge it");
    }
}
