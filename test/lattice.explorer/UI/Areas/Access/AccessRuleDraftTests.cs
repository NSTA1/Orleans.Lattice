using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The rule editor's working copy: validation (including the scopeless
/// cluster-wide capabilities and the app-owned id namespace), and the round trip
/// to and from a rule.
/// </summary>
[TestFixture]
public sealed class AccessRuleDraftTests
{
    [Test]
    public void A_complete_tree_draft_validates_and_converts()
    {
        var draft = Valid();

        var rule = draft.ToRule();

        Assert.Multiple(() =>
        {
            Assert.That(draft.Validate().HasAny, Is.False);
            Assert.That(rule, Is.EqualTo(new LatticeAuthorizationRule("r1", LatticeSubjectSelector.Group("ops"), LatticeScope.Tree("orders"), LatticeOperation.Read, LatticeEffect.Allow)));
        });
    }

    [Test]
    [TestCase(AccessRuleDraft.KeyScope, LatticeScopeKind.Key)]
    [TestCase(AccessRuleDraft.PrefixScope, LatticeScopeKind.Prefix)]
    public void A_key_or_prefix_draft_carries_its_key(string kind, LatticeScopeKind expected)
    {
        var draft = Valid();
        draft.ScopeKind = kind;
        draft.KeyOrPrefix = "draft/";

        var scope = draft.ToRule().Scope;

        Assert.That((scope.Kind, scope.TreeId, scope.KeyOrPrefix), Is.EqualTo((expected, "orders", "draft/")));
    }

    [Test]
    public void The_cluster_and_access_administration_scopes_name_their_reserved_trees()
    {
        var cluster = Valid();
        cluster.ScopeKind = AccessRuleDraft.ClusterScope;
        cluster.Operations = LatticeOperation.Telemetry;
        var delegation = Valid();
        delegation.ScopeKind = AccessRuleDraft.AccessAdministrationScope;
        delegation.Operations = LatticeOperation.Admin;

        Assert.Multiple(() =>
        {
            Assert.That(cluster.ToRule().Scope, Is.EqualTo(LatticeScope.ClusterWide()));
            Assert.That(delegation.ToRule().Scope, Is.EqualTo(LatticeScope.Tree(LatticeAuthReservedTrees.PolicyTreeId)));
        });
    }

    [Test]
    [TestCase(LatticeOperation.AppInstall)]
    [TestCase(LatticeOperation.Telemetry)]
    [TestCase(LatticeOperation.Read | LatticeOperation.Telemetry)]
    public void A_scopeless_capability_with_a_narrower_scope_is_refused(LatticeOperation operations)
    {
        foreach (var kind in new[] { AccessRuleDraft.TreeScope, AccessRuleDraft.PrefixScope, AccessRuleDraft.KeyScope, AccessRuleDraft.AccessAdministrationScope })
        {
            var draft = Valid();
            draft.ScopeKind = kind;
            draft.KeyOrPrefix = "k";
            draft.Operations = operations;

            Assert.That(draft.Validate().Operations, Is.EqualTo(AccessRuleDraft.ScopelessMessage), kind);
            Assert.Throws<InvalidOperationException>(() => draft.ToRule());
        }
    }

    [Test]
    public void A_scopeless_capability_at_the_cluster_wide_scope_is_accepted()
    {
        var draft = Valid();
        draft.ScopeKind = AccessRuleDraft.ClusterScope;
        draft.Operations = LatticeOperation.AppInstall | LatticeOperation.Telemetry;

        Assert.That(draft.Validate().HasAny, Is.False);
    }

    [Test]
    public void Every_missing_field_is_reported()
    {
        var draft = AccessRuleDraft.New();
        draft.ScopeKind = AccessRuleDraft.KeyScope;

        var errors = draft.Validate();

        Assert.Multiple(() =>
        {
            Assert.That(errors.RuleId, Is.EqualTo("Enter a rule id."));
            Assert.That(errors.Subject, Is.Not.Null);
            Assert.That(errors.Tree, Is.Not.Null);
            Assert.That(errors.KeyOrPrefix, Is.EqualTo("Enter the key."));
            Assert.That(errors.Operations, Is.EqualTo("Choose at least one operation."));
            Assert.That(errors.Form, Is.Null);
            Assert.That(errors.HasAny, Is.True);
        });
    }

    [Test]
    [TestCase("*", "Choose the cluster-wide scope to govern every tree.")]
    [TestCase("sys-auth-policy", "Reserved system trees cannot be named here.")]
    public void A_tree_scope_cannot_name_a_reserved_tree(string tree, string expected)
    {
        var draft = Valid();
        draft.TreeId = tree;

        Assert.That(draft.Validate().Tree, Is.EqualTo(expected));
    }

    [Test]
    public void A_new_rule_cannot_take_an_app_owned_id_but_an_existing_one_keeps_its_own()
    {
        var draft = Valid();
        draft.RuleId = "app:crm:viewer:x";
        var existing = AccessRuleDraft.From(new LatticeAuthorizationRule("app:crm:viewer:x", LatticeSubjectSelector.Group("g"), LatticeScope.Tree("t"), LatticeOperation.Read, LatticeEffect.Allow));

        Assert.Multiple(() =>
        {
            Assert.That(draft.Validate().RuleId, Does.StartWith("Ids starting with app: belong to installed apps"));
            Assert.That(existing.Validate().RuleId, Is.Null);
        });
    }

    [Test]
    [TestCase("tree")]
    [TestCase("prefix")]
    [TestCase("key")]
    [TestCase("cluster")]
    [TestCase("delegation")]
    public void From_round_trips_every_scope(string shape)
    {
        var scope = shape switch
        {
            "prefix" => LatticeScope.Prefix("orders", "p/"),
            "key" => LatticeScope.Key("orders", "k"),
            "cluster" => LatticeScope.ClusterWide(),
            "delegation" => LatticeScope.Tree(LatticeAuthReservedTrees.PolicyTreeId),
            _ => LatticeScope.Tree("orders"),
        };
        var operations = shape == "cluster" ? LatticeOperation.Telemetry : LatticeOperation.Admin;
        var rule = new LatticeAuthorizationRule("r", LatticeSubjectSelector.User("alice"), scope, operations, LatticeEffect.Deny, "when x");

        var draft = AccessRuleDraft.From(rule);

        Assert.Multiple(() =>
        {
            Assert.That(draft.IsExisting, Is.True);
            Assert.That(draft.ToRule(), Is.EqualTo(rule));
        });
    }

    [Test]
    public void Operations_are_set_and_cleared_one_flag_at_a_time()
    {
        var draft = AccessRuleDraft.New();

        draft.SetOperation(LatticeOperation.Read, true);
        draft.SetOperation(LatticeOperation.Write, true);
        draft.SetOperation(LatticeOperation.Read, false);

        Assert.Multiple(() =>
        {
            Assert.That(draft.Operations, Is.EqualTo(LatticeOperation.Write));
            Assert.That(draft.HasOperation(LatticeOperation.Write), Is.True);
            Assert.That(draft.HasOperation(LatticeOperation.Read), Is.False);
        });
    }

    [Test]
    public void A_blank_condition_is_dropped_and_the_ids_are_trimmed()
    {
        var draft = Valid();
        draft.RuleId = "  r1 ";
        draft.SubjectId = " ops ";
        draft.Condition = "   ";

        var rule = draft.ToRule();

        Assert.That((rule.RuleId, rule.Subject.Id, rule.Condition), Is.EqualTo(("r1", "ops", (string?)null)));
    }

    private static AccessRuleDraft Valid()
    {
        var draft = AccessRuleDraft.New();
        draft.RuleId = "r1";
        draft.SubjectKind = LatticeSubjectSelectorKind.Group;
        draft.SubjectId = "ops";
        draft.ScopeKind = AccessRuleDraft.TreeScope;
        draft.TreeId = "orders";
        draft.Operations = LatticeOperation.Read;
        return draft;
    }
}
