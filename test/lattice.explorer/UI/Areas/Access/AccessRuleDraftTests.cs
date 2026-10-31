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
    [TestCase("t/acme/acme-inventory", null)]
    [TestCase("t/globex/orders", "tree")]
    [TestCase("orders", "tree")]
    [TestCase("t/acme/a/task-board/tasks", "tree")]
    [TestCase("t/acme/sys-index", "tree")]
    public void A_rule_naming_a_tenant_group_may_govern_only_that_tenants_own_trees(string tree, string? field)
    {
        var draft = Valid();
        draft.SubjectId = "t/acme/acme-analysts";
        draft.TreeId = tree;

        var errors = draft.Validate();

        Assert.That(errors.Tree, field is null
            ? Is.Null
            : Is.EqualTo("A rule naming tenant group t/acme/acme-analysts may govern only tenant acme's own trees (t/acme/...). Choose one of them."));
    }

    [Test]
    public void A_tenant_group_subject_is_refused_on_a_scope_without_a_tree_and_when_malformed()
    {
        var cluster = Valid();
        cluster.SubjectId = "t/acme/acme-analysts";
        cluster.ScopeKind = AccessRuleDraft.ClusterScope;
        var malformed = Valid();
        malformed.SubjectId = "t/acme";
        var user = Valid();
        user.SubjectKind = LatticeSubjectSelectorKind.User;
        user.SubjectId = "t/acme/acme-analysts";

        Assert.Multiple(() =>
        {
            Assert.That(cluster.Validate().Subject, Does.StartWith("A rule naming tenant group t/acme/acme-analysts may govern only"));
            Assert.That(malformed.Validate().Tree, Is.EqualTo("t/acme is not a well-formed tenant group id (t/{tenant}/{name})."));
            Assert.That(user.Validate().HasAny, Is.False, "a user subject is not confined here; the picker refuses it");
        });
    }

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
    public void A_new_draft_carries_no_condition_and_the_ids_are_trimmed()
    {
        var draft = Valid();
        draft.RuleId = "  r1 ";
        draft.SubjectId = " ops ";

        var rule = draft.ToRule();

        Assert.Multiple(() =>
        {
            Assert.That(draft.HasCondition, Is.False);
            Assert.That((rule.RuleId, rule.Subject.Id, rule.Condition), Is.EqualTo(("r1", "ops", (string?)null)));
        });
    }

    [Test]
    [TestCase("region == \"eu\"")]
    [TestCase("  padded condition  ")]
    public void An_existing_rules_condition_is_carried_through_a_save_verbatim(string condition)
    {
        var existing = new LatticeAuthorizationRule(
            "r1", LatticeSubjectSelector.Group("ops"), LatticeScope.Tree("orders"), LatticeOperation.Read, LatticeEffect.Allow, condition);

        var draft = AccessRuleDraft.From(existing);
        draft.SetOperation(LatticeOperation.Write, true);
        var rule = draft.ToRule();

        Assert.Multiple(() =>
        {
            Assert.That(draft.HasCondition, Is.True);
            Assert.That(draft.Condition, Is.EqualTo(condition));
            Assert.That(rule.Condition, Is.EqualTo(condition));
            Assert.That(rule.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
        });
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
