using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// Issue #4163: the tenant rule editor's working copy and its naming helpers -
/// the scopes and operations a tenant rule may have, the client-side confinement
/// of its tree, its validation, the local id it suggests, and the draft it sends.
/// </summary>
[TestFixture]
public sealed class TenantRuleFormTests
{
    [Test]
    public void The_data_plane_operations_are_exactly_the_tenant_mask()
    {
        var offered = TenantRuleFormat.DataPlaneOperations.Aggregate(LatticeOperation.None, (all, option) => all | option.Flag);

        Assert.Multiple(() =>
        {
            Assert.That(offered, Is.EqualTo(LatticeAuthOperations.All));
            Assert.That(TenantRuleFormat.DataPlaneOperations.Select(option => option.Flag), Has.None.EqualTo(LatticeOperation.Telemetry));
            Assert.That(TenantRuleFormat.DataPlaneOperations.Select(option => option.Flag), Has.None.EqualTo(LatticeOperation.AppInstall));
            Assert.That(TenantRuleFormat.DataPlaneOperations.Select(option => option.Flag), Has.None.EqualTo(LatticeOperation.Replication));
            Assert.That(TenantRuleFormat.DataPlaneOperations.Select(option => option.Flag), Has.None.EqualTo(LatticeOperation.TreeLifecycle));
            Assert.That(TenantRuleFormat.DataPlaneOperations.Select(option => option.Group).Distinct(), Is.EqualTo(new[] { AccessOperationGroup.Data, AccessOperationGroup.Administration }));
        });
    }

    [Test]
    [TestCase("a/crm/contacts", "App-owned")]
    [TestCase("t/globex/billing", "another tenant's tree")]
    [TestCase("sys-auth-policy", "Reserved and system")]
    [TestCase("_lattice_registry", "Reserved and system")]
    [TestCase("*", "Every tree in this tenant")]
    public void A_tree_the_tenant_may_not_govern_is_refused_before_anything_is_sent(string tree, string reason)
    {
        Assert.That(TenantRuleFormat.TreeProblem(tree), Does.Contain(reason));
    }

    [Test]
    [TestCase("orders")]
    [TestCase("billing/2026")]
    [TestCase("  orders  ")]
    [TestCase("")]
    [TestCase(null)]
    public void One_of_the_tenants_own_trees_is_accepted(string? tree)
    {
        Assert.That(TenantRuleFormat.TreeProblem(tree), Is.Null);
    }

    [Test]
    public void Scope_values_round_trip_and_an_unknown_one_reads_as_a_whole_tree()
    {
        Assert.Multiple(() =>
        {
            foreach (var kind in Enum.GetValues<TenantRuleScopeKind>())
            {
                Assert.That(TenantRuleFormat.ScopeKind(TenantRuleFormat.ScopeValue(kind)), Is.EqualTo(kind), kind.ToString());
            }

            Assert.That(TenantRuleFormat.ScopeKind("cluster"), Is.EqualTo(TenantRuleScopeKind.Tree));
            Assert.That(TenantRuleFormat.ScopeKind(null), Is.EqualTo(TenantRuleScopeKind.Tree));
        });
    }

    [Test]
    public void Subject_kinds_round_trip_through_the_pickers_values()
    {
        Assert.Multiple(() =>
        {
            foreach (var kind in Enum.GetValues<TenantSubjectKind>())
            {
                Assert.That(TenantRuleFormat.SubjectKindOf(TenantRuleFormat.SubjectKindValue(kind)), Is.EqualTo(kind), kind.ToString());
            }

            Assert.That(TenantRuleFormat.SubjectKindOf("group"), Is.EqualTo(TenantSubjectKind.ClusterGroup), "the cluster page's group kind");
            Assert.That(TenantRuleFormat.SubjectKindOf(null), Is.EqualTo(TenantSubjectKind.User));
        });
    }

    [Test]
    public void Labels_name_the_scope_subject_layer_origin_and_confinement()
    {
        TenantRuleView Rule(TenantRuleScopeKind scope, string? tree, string? keyOrPrefix = null) =>
            new() { RuleId = "r", ScopeKind = scope, TreeName = tree, KeyOrPrefix = keyOrPrefix };

        Assert.Multiple(() =>
        {
            Assert.That(TenantRuleFormat.ScopeLabel(Rule(TenantRuleScopeKind.TenantWide, null)), Is.EqualTo("every tree in this tenant"));
            Assert.That(TenantRuleFormat.ScopeLabel(Rule(TenantRuleScopeKind.Tree, "orders")), Is.EqualTo("orders"));
            Assert.That(TenantRuleFormat.ScopeLabel(Rule(TenantRuleScopeKind.Key, "orders", "o-1")), Is.EqualTo("orders key o-1"));
            Assert.That(TenantRuleFormat.ScopeLabel(Rule(TenantRuleScopeKind.Prefix, "orders", "eu/")), Is.EqualTo("orders prefix eu/"));
            Assert.That(TenantRuleFormat.ScopeLabel(Rule(TenantRuleScopeKind.Tree, null)), Is.EqualTo(TenantRuleFormat.WithheldText));
            Assert.That(() => TenantRuleFormat.ScopeLabel(null!), Throws.ArgumentNullException);
            Assert.That(TenantRuleFormat.SubjectLabel(TenantSubjectKind.TenantGroup, "eng"), Is.EqualTo("tenant-group:eng"));
            Assert.That(TenantRuleFormat.SubjectLabel(TenantSubjectKind.ClusterGroup, "ops"), Is.EqualTo("group:ops"));
            Assert.That(TenantRuleFormat.SubjectLabel(TenantSubjectKind.User, "ada"), Is.EqualTo("user:ada"));
            Assert.That(TenantRuleFormat.SubjectLabel(TenantSubjectKind.User, null), Is.EqualTo(TenantRuleFormat.WithheldText));
            Assert.That(TenantRuleFormat.LayerLabel(TenantRuleLayer.Platform), Is.EqualTo("Platform"));
            Assert.That(TenantRuleFormat.LayerLabel(TenantRuleLayer.Tenant), Is.EqualTo("Tenant"));
            Assert.That(Enum.GetValues<TenantRuleOrigin>().Select(TenantRuleFormat.OriginLabel), Is.Unique);
            Assert.That(Enum.GetValues<TenantAccessConfinementRule>().Select(TenantRuleFormat.ConfinementReason), Is.Unique);
        });
    }

    [Test]
    public void A_new_form_is_a_read_allow_over_one_tree_for_a_tenant_group()
    {
        var form = TenantRuleForm.New();

        Assert.Multiple(() =>
        {
            Assert.That(form.IsExisting, Is.False);
            Assert.That(form.ScopeKind, Is.EqualTo(TenantRuleScopeKind.Tree));
            Assert.That(form.SubjectKind, Is.EqualTo(TenantSubjectKind.TenantGroup));
            Assert.That(form.Operations, Is.EqualTo(LatticeOperation.Read));
            Assert.That(form.Effect, Is.EqualTo(LatticeEffect.Allow));
            Assert.That(form.NeedsTree, Is.True);
            Assert.That(form.NeedsKeyOrPrefix, Is.False);
        });
    }

    [Test]
    public void Every_missing_field_is_named()
    {
        var form = TenantRuleForm.New();
        form.ScopeValue = TenantRuleFormat.PrefixScope;
        form.Operations = LatticeOperation.None;

        var errors = form.Validate();

        Assert.Multiple(() =>
        {
            Assert.That(errors.RuleId, Is.Not.Null);
            Assert.That(errors.Subject, Is.Not.Null);
            Assert.That(errors.Tree, Is.Not.Null);
            Assert.That(errors.KeyOrPrefix, Is.EqualTo("Name the key prefix."));
            Assert.That(errors.Operations, Is.EqualTo("Tick at least one operation."));
            Assert.That(errors.HasAny, Is.True);
        });
    }

    [Test]
    public void An_app_tree_and_a_capability_beyond_the_data_plane_are_refused()
    {
        var form = Filled();
        form.TreeName = "a/crm/contacts";
        form.Operations = LatticeOperation.Read | LatticeOperation.Telemetry;

        var errors = form.Validate();

        Assert.Multiple(() =>
        {
            Assert.That(errors.Tree, Does.Contain("App-owned"));
            Assert.That(errors.Operations, Does.Contain("data-plane"));
        });
    }

    [Test]
    public void A_tenant_wide_rule_needs_no_tree_and_sends_none()
    {
        var form = Filled();
        form.ScopeValue = TenantRuleFormat.TenantWideScope;
        form.TreeName = "a/stale/choice";
        form.KeyOrPrefix = "stale";

        var draft = form.ToDraft();

        Assert.Multiple(() =>
        {
            Assert.That(form.Validate().HasAny, Is.False);
            Assert.That(draft.ScopeKind, Is.EqualTo(TenantRuleScopeKind.TenantWide));
            Assert.That(draft.TreeName, Is.Null);
            Assert.That(draft.KeyOrPrefix, Is.Null);
        });
    }

    [Test]
    public void The_draft_is_trimmed_and_keeps_every_field()
    {
        var form = Filled();
        form.RuleId = "  eng-orders ";
        form.SubjectId = " eng ";
        form.TreeName = " orders ";
        form.ScopeValue = TenantRuleFormat.KeyScope;
        form.KeyOrPrefix = " k ";
        form.Effect = LatticeEffect.Deny;
        form.SetOperation(LatticeOperation.Write, true);

        var draft = form.ToDraft();

        Assert.Multiple(() =>
        {
            Assert.That(draft.RuleId, Is.EqualTo("eng-orders"));
            Assert.That(draft.SubjectId, Is.EqualTo("eng"));
            Assert.That(draft.SubjectKind, Is.EqualTo(TenantSubjectKind.TenantGroup));
            Assert.That(draft.TreeName, Is.EqualTo("orders"));
            Assert.That(draft.KeyOrPrefix, Is.EqualTo(" k "), "a key is sent as typed");
            Assert.That(draft.ScopeKind, Is.EqualTo(TenantRuleScopeKind.Key));
            Assert.That(draft.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
            Assert.That(draft.Effect, Is.EqualTo(LatticeEffect.Deny));
        });
    }

    [Test]
    public void An_existing_rule_is_edited_with_its_id_and_scope_fixed()
    {
        var form = TenantRuleForm.From(new TenantRuleView
        {
            RuleId = "readers",
            Layer = TenantRuleLayer.Tenant,
            Origin = TenantRuleOrigin.Tenant,
            Editable = true,
            SubjectId = "ops",
            SubjectKind = TenantSubjectKind.ClusterGroup,
            ScopeKind = TenantRuleScopeKind.Prefix,
            TreeName = "orders",
            KeyOrPrefix = "eu/",
            Operations = LatticeOperation.RangeRead,
            Effect = LatticeEffect.Deny,
        });

        Assert.Multiple(() =>
        {
            Assert.That(form.IsExisting, Is.True);
            Assert.That(form.ScopeValue, Is.EqualTo(TenantRuleFormat.PrefixScope));
            Assert.That(form.ToDraft(), Is.EqualTo(new TenantRuleDraft
            {
                RuleId = "readers",
                SubjectId = "ops",
                SubjectKind = TenantSubjectKind.ClusterGroup,
                ScopeKind = TenantRuleScopeKind.Prefix,
                TreeName = "orders",
                KeyOrPrefix = "eu/",
                Operations = LatticeOperation.RangeRead,
                Effect = LatticeEffect.Deny,
            }));
            Assert.That(form.SuggestId(), Is.Null, "an existing rule keeps its id");
            Assert.That(() => TenantRuleForm.From(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Operations_are_ticked_and_cleared_one_at_a_time()
    {
        var form = TenantRuleForm.New();

        form.SetOperation(LatticeOperation.Write, true);
        form.SetOperation(LatticeOperation.Read, false);

        Assert.Multiple(() =>
        {
            Assert.That(form.HasOperation(LatticeOperation.Write), Is.True);
            Assert.That(form.HasOperation(LatticeOperation.Read), Is.False);
            Assert.That(form.Operations, Is.EqualTo(LatticeOperation.Write));
        });
    }

    [Test]
    public void The_suggested_id_is_built_from_the_effect_subject_and_scope()
    {
        var form = Filled();
        form.SubjectId = "Eng Team";
        form.TreeName = "orders/EU";

        Assert.That(form.SuggestId(), Is.EqualTo("allow-eng-team-orders-eu"));

        form.Effect = LatticeEffect.Deny;
        form.ScopeValue = TenantRuleFormat.TenantWideScope;
        Assert.That(form.SuggestId(), Is.EqualTo("deny-eng-team-all-trees"));

        form.ScopeValue = TenantRuleFormat.KeyScope;
        form.KeyOrPrefix = "#42";
        Assert.That(form.SuggestId(), Is.EqualTo("deny-eng-team-orders-eu-42"));
    }

    [Test]
    public void No_id_is_suggested_without_a_subject_and_a_long_one_is_cut_cleanly()
    {
        var form = TenantRuleForm.New();
        Assert.That(form.SuggestId(), Is.Null);

        form.SubjectId = new string('x', 59);
        form.TreeName = "orders";
        var suggested = form.SuggestId();

        Assert.Multiple(() =>
        {
            Assert.That(suggested, Has.Length.LessThanOrEqualTo(TenantRuleForm.MaximumSuggestedIdLength));
            Assert.That(suggested, Does.Not.EndWith("-"));
            Assert.That(suggested, Does.StartWith("allow-xxx"));
        });
    }

    private static TenantRuleForm Filled()
    {
        var form = TenantRuleForm.New();
        form.RuleId = "eng-orders";
        form.SubjectId = "eng";
        form.TreeName = "orders";
        return form;
    }
}
