using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// Issue #4162: the words the tenant Groups and Members pages use - kinds, the local
/// group-name grammar (D1), cap usage (D13), the typed confinement reasons (D3) and
/// the removal report.
/// </summary>
[TestFixture]
public sealed class TenantGroupFormatTests
{
    [Test]
    [TestCase(TenantSubjectKind.User, "User", "user")]
    [TestCase(TenantSubjectKind.TenantGroup, "This tenant's group", "tenant-group")]
    [TestCase(TenantSubjectKind.ClusterGroup, "Cluster group", "cluster-group")]
    public void Each_kind_has_a_label_and_a_value(TenantSubjectKind kind, string label, string value)
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGroupFormat.KindLabel(kind), Is.EqualTo(label));
            Assert.That(TenantGroupFormat.KindValue(kind), Is.EqualTo(value));
        });
    }

    [Test]
    [TestCase("ops")]
    [TestCase("eng-team_2.a")]
    [TestCase("a")]
    public void A_name_in_the_grammar_is_accepted(string name) =>
        Assert.That(TenantGroupFormat.NameError("acme", name), Is.Null);

    [Test]
    [TestCase("Ops")]
    [TestCase("eng team")]
    [TestCase("eng/team")]
    [TestCase("caf\u00e9")]
    public void A_name_outside_the_grammar_is_refused(string name) =>
        Assert.That(TenantGroupFormat.NameError("acme", name), Is.EqualTo(TenantGroupFormat.NameGrammarMessage));

    [Test]
    public void A_name_longer_than_the_limit_is_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGroupFormat.NameError("acme", new string('a', LatticeTenantGroupId.MaxNameLength)), Is.Null);
            Assert.That(TenantGroupFormat.NameError("acme", new string('a', LatticeTenantGroupId.MaxNameLength + 1)), Is.EqualTo(TenantGroupFormat.NameGrammarMessage));
        });
    }

    [Test]
    public void An_empty_name_asks_for_one_and_a_missing_tenant_throws()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGroupFormat.NameError("acme", null), Is.EqualTo("Enter the group's name."));
            Assert.That(TenantGroupFormat.NameError("acme", string.Empty), Is.EqualTo("Enter the group's name."));
            Assert.That(() => TenantGroupFormat.NameError(null!, "ops"), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_dimension_is_at_its_cap_only_when_its_usage_reaches_a_limit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGroupFormat.AtCap(new TenantQuotaDimensionUsage { Usage = 500, Limit = 500 }), Is.True);
            Assert.That(TenantGroupFormat.AtCap(new TenantQuotaDimensionUsage { Usage = 501, Limit = 500 }), Is.True);
            Assert.That(TenantGroupFormat.AtCap(new TenantQuotaDimensionUsage { Usage = 499, Limit = 500 }), Is.False);
            Assert.That(TenantGroupFormat.AtCap(new TenantQuotaDimensionUsage { Usage = 9, Limit = null }), Is.False);
            Assert.That(TenantGroupFormat.AtCap(new TenantQuotaDimensionUsage { Usage = null, Limit = 5 }), Is.False);
        });
    }

    [Test]
    public void The_cap_text_names_usage_and_limit_or_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGroupFormat.CapText(new TenantQuotaDimensionUsage { Usage = 3, Limit = 500 }, "group", "groups"), Is.EqualTo("3 of 500 groups"));
            Assert.That(TenantGroupFormat.CapText(new TenantQuotaDimensionUsage { Usage = 0, Limit = 1 }, "group", "groups"), Is.EqualTo("0 of 1 group"));
            Assert.That(TenantGroupFormat.CapText(TenantQuotaDimensionUsage.Unbounded, "group", "groups"), Is.Null);
            Assert.That(
                TenantGroupFormat.CapReason(new TenantQuotaDimensionUsage { Usage = 2, Limit = 2 }, "groups", "acme"),
                Is.EqualTo("Tenant acme is at its cap of 2 groups. Remove one, or ask a platform operator to raise the cap."));
        });
    }

    [Test]
    [TestCase(TenantAccessConfinementRule.GroupNesting, TenantGroupFormat.NestingMessage)]
    [TestCase(TenantAccessConfinementRule.ForeignTenantGroup, TenantGroupFormat.ForeignGroupMessage)]
    [TestCase(TenantAccessConfinementRule.RuleTree, "raw reason")]
    public void A_confinement_refusal_is_shown_with_its_typed_reason(TenantAccessConfinementRule rule, string expected)
    {
        var refusal = new TenantAccessConfinementException("acme", rule, "raw reason");

        Assert.Multiple(() =>
        {
            Assert.That(TenantGroupFormat.ConfinementMessage(refusal), Is.EqualTo(expected));
            Assert.That(TenantGroupFormat.RefusalMessage(refusal, new AccessFailure(AccessFailureKind.Invalid, "classified")), Is.EqualTo(expected));
        });
    }

    [Test]
    public void Any_other_refusal_is_shown_with_its_classified_message()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGroupFormat.RefusalMessage(new InvalidOperationException("x"), new AccessFailure(AccessFailureKind.Unavailable, "classified")),
                Is.EqualTo("classified"));
            Assert.That(() => TenantGroupFormat.RefusalMessage(null!, new AccessFailure(AccessFailureKind.Invalid, "x")), Throws.ArgumentNullException);
            Assert.That(() => TenantGroupFormat.RefusalMessage(new InvalidOperationException(), null!), Throws.ArgumentNullException);
            Assert.That(() => TenantGroupFormat.ConfinementMessage(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_count_takes_its_singular_or_plural_noun()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGroupFormat.Count(1, "rule", "rules"), Is.EqualTo("1 rule"));
            Assert.That(TenantGroupFormat.Count(0, "rule", "rules"), Is.EqualTo("0 rules"));
            Assert.That(TenantGroupFormat.Count(12000, "rule", "rules"), Is.EqualTo("12000 rules"));
        });
    }

    [Test]
    public void The_removal_report_names_what_the_cascade_took()
    {
        var result = new TenantGroupRemovalResult
        {
            TenantId = "acme",
            GroupName = "ops",
            Removed = true,
            EdgesRemoved = 3,
            RemovedFromMemberSet = true,
            RemovedFromAdminSet = true,
            RemovedRuleIds = ["readers", "writers"],
        };

        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGroupFormat.RemovalText(result),
                Is.EqualTo("Group ops deleted, with 3 membership entries, 2 rules, its member-set entry, its administrator entry."));
            Assert.That(
                TenantGroupFormat.RemovalText(result with { EdgesRemoved = 1, RemovedRuleIds = [], RemovedFromMemberSet = false, RemovedFromAdminSet = false }),
                Is.EqualTo("Group ops deleted, with 1 membership entry, 0 rules."));
            Assert.That(TenantGroupFormat.RemovalText(result with { Removed = false }), Is.EqualTo("Group ops was already gone."));
            Assert.That(() => TenantGroupFormat.RemovalText(null!), Throws.ArgumentNullException);
        });
    }
}
