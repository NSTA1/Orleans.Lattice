using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Explain;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Explain;

/// <summary>
/// Issue #4163: the tenant's layer-aware Explain - the Platform layer drawn above
/// the Tenant layer, the deciding layer and rule marked and the losing layer
/// dimmed, a platform-wide or app decision named by id and effect only, the
/// default when nothing matched, and a subject that cannot act as the tenant
/// told so first with a link to Members.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantExplainViewTests : TenantRulesTestContext
{
    [Test]
    public void A_platform_rule_decides_first_and_the_tenant_layer_is_dimmed_with_its_shadowed_rules()
    {
        SeedPlatformRule("guard", "orders", "ada", kind: TenantSubjectKind.User);
        SeedTenantRule("ada-orders", "orders", "ada", kind: TenantSubjectKind.User);
        AdaIsMember();

        var cut = Explain();

        cut.WaitUntil(() =>
        {
            Assert.That(Layer(cut, "platform").GetAttribute("data-lt-layer-state"), Is.EqualTo("deciding"));
            Assert.That(Layer(cut, "platform").GetAttribute("aria-current"), Is.EqualTo("true"));
            Assert.That(Layer(cut, "platform").QuerySelector(".lt-access-layer__head .lt-node--join"), Is.Not.Null, "the deciding layer sits on the join node");
            Assert.That(Layer(cut, "platform").QuerySelector("[data-lt-rule-id=guard] .lt-node--join"), Is.Not.Null, "and so does the deciding rule");
            Assert.That(Layer(cut, "platform").QuerySelector("[data-lt-rule-id=guard]")!.TextContent, Does.Contain("Deciding rule"));
            Assert.That(Layer(cut, "tenant").GetAttribute("data-lt-layer-state"), Is.EqualTo("losing"));
            Assert.That(Layer(cut, "tenant").ClassList, Does.Contain("lt-access-layer--losing"));
            Assert.That(Layer(cut, "tenant").QuerySelectorAll(".lt-node--join"), Is.Empty);
            Assert.That(Layer(cut, "tenant").QuerySelector("[data-lt-shadowed-by]")!.TextContent, Is.EqualTo("These rules match but do not take effect: platform rule guard decided first."));
            Assert.That(cut.Find("[data-lt-explanation]").GetAttribute("data-lt-explanation"), Is.EqualTo("denied"));
        });
    }

    [Test]
    public void The_tenant_layer_decides_when_no_platform_rule_matches()
    {
        SeedTenantRule("ada-orders", "orders", "ada", kind: TenantSubjectKind.User);
        AdaIsMember();

        var cut = Explain();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-access-layers").GetAttribute("data-lt-deciding-layer"), Is.EqualTo("tenant"));
            Assert.That(Layer(cut, "tenant").GetAttribute("data-lt-layer-state"), Is.EqualTo("deciding"));
            Assert.That(Layer(cut, "tenant").QuerySelector("[data-lt-rule-id=ada-orders]")!.ClassList, Does.Contain("lt-access-layer__rule--deciding"));
            Assert.That(Layer(cut, "platform").GetAttribute("data-lt-layer-state"), Is.EqualTo("losing"));
            Assert.That(Layer(cut, "platform").TextContent, Does.Contain("No platform rule matched."));
            Assert.That(cut.Find("[data-lt-explanation]").GetAttribute("data-lt-explanation"), Is.EqualTo("allowed"));
            Assert.That(cut.FindAll("[data-lt-default-decided]"), Is.Empty);
        });
    }

    [Test]
    public void With_no_rule_in_either_layer_the_default_decides()
    {
        AdaIsMember();

        var cut = Explain();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-access-layers").GetAttribute("data-lt-deciding-layer"), Is.EqualTo("default"));
            Assert.That(cut.FindAll("[data-lt-layer-state=deciding]"), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-layer-state=losing]"), Is.Empty);
            Assert.That(cut.Find("[data-lt-default-decided]").TextContent, Does.Contain("the default decided: Deny"));
        });
    }

    [Test]
    [TestCase(TenantRuleOrigin.PlatformWide, "A platform-wide rule")]
    [TestCase(TenantRuleOrigin.AppRole, "An app role")]
    public void A_platform_wide_or_app_decision_names_only_its_id_and_effect(TenantRuleOrigin origin, string phrase)
    {
        SeedPlatformRule("wide", null, "ada", kind: TenantSubjectKind.User, scope: TenantRuleScopeKind.TenantWide, origin: origin);
        AdaIsMember();

        var cut = Explain();

        cut.WaitUntil(() =>
        {
            var withheld = Layer(cut, "platform").QuerySelector("[data-lt-withheld-rule]")!;
            Assert.That(withheld.GetAttribute("data-lt-withheld-rule"), Is.EqualTo("wide"));
            Assert.That(withheld.TextContent, Does.Contain(phrase).And.Contain("wide").And.Contain("Deny"));
            Assert.That(Layer(cut, "platform").TextContent, Does.Not.Contain("ada"), "the subject is withheld");
            Assert.That(Layer(cut, "platform").QuerySelectorAll("[data-lt-rule-id]"), Is.Empty);
            Assert.That(Layer(cut, "tenant").GetAttribute("data-lt-layer-state"), Is.EqualTo("losing"));
        });
    }

    [Test]
    public void A_subject_that_cannot_act_as_the_tenant_is_told_so_first_with_a_link_to_members()
    {
        SeedTenantRule("ada-orders", "orders", "ada", kind: TenantSubjectKind.User);

        var cut = Explain();

        cut.WaitUntil(() =>
        {
            var explanation = cut.Find("[data-lt-explanation]");
            var verdict = explanation.FirstElementChild!;
            Assert.That(verdict.GetAttribute("data-lt-cannot-act"), Is.EqualTo("true"), "the tenant gate's verdict comes first");
            Assert.That(verdict.TextContent, Does.Contain("Cannot act as tenant acme"));
            Assert.That(explanation.QuerySelector(".lt-access-notice .lt-access-mono")!.TextContent, Is.EqualTo("user:ada"));
            Assert.That(explanation.TextContent, Does.Contain("is neither an administrator nor a member of tenant acme"));
            Assert.That(explanation.QuerySelector("a[href$='t/acme/access/members']")!.TextContent, Is.EqualTo("Add it on the Members page"));
            Assert.That(cut.FindAll(".lt-access-layers"), Has.Count.EqualTo(1), "the layers still say what a member would get");
        });
    }

    [Test]
    public void A_member_or_an_unknown_standing_is_never_called_an_outsider()
    {
        AdaIsMember();
        var member = Explain();
        member.WaitUntil(() => Assert.That(member.FindAll("[data-lt-explanation]"), Has.Count.EqualTo(1)));

        Assert.That(member.FindAll("[data-lt-cannot-act]"), Is.Empty);
    }

    [Test]
    public void Without_a_tenant_directory_the_standing_is_not_guessed()
    {
        TenantFacades.ServesDirectory = false;

        var cut = Explain();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[data-lt-explanation]"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("[data-lt-cannot-act]"), Is.Empty);
        });
    }

    [Test]
    public void The_address_fills_the_form_and_a_missing_subject_or_tree_is_named()
    {
        var cut = RenderAt<AccessExplainPage>("t/acme/access/explain?subject=eng&kind=cluster-group&tree=orders&operation=write");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form[data-lt-tenant-explain]"), Has.Count.EqualTo(1)));

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.Field(cut, "Subject").GetAttribute("value"), Is.EqualTo("eng"));
            Assert.That(AccessForms.Field(cut, "Subject kind").QuerySelector("option[selected]")?.GetAttribute("value"), Is.EqualTo("cluster-group"));
            Assert.That(AccessForms.Field(cut, "Tree").GetAttribute("value"), Is.EqualTo("orders"));
            Assert.That(AccessForms.Field(cut, "Operation").QuerySelector("option[selected]")?.GetAttribute("value"), Is.EqualTo("write"));
            Assert.That(AccessForms.Field(cut, "Operation").QuerySelectorAll("option").Select(option => option.GetAttribute("value")), Has.None.EqualTo("telemetry").And.None.EqualTo("treelifecycle"));
        });

        AccessForms.Type(cut, "Subject", string.Empty);
        AccessForms.Type(cut, "Tree", "a/crm/contacts");
        cut.Find("form[data-lt-tenant-explain]").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.Not.Null.And.Not.Empty);
            Assert.That(AccessForms.ErrorOf(cut, "Tree"), Does.Contain("App-owned"));
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantPolicyAdmin.ExplainAsync)));
        });
    }

    [Test]
    public void A_refused_explanation_shows_why()
    {
        Script().Faults[nameof(ILatticeTenantPolicyAdmin.ExplainAsync)] = new LatticeAuthorizationDeniedException("denied");
        AdaIsMember();

        var cut = Explain();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=explain]").TextContent, Does.Contain(TenantAccessFailure.DeniedMessage(Acme))));
    }

    private void AdaIsMember()
    {
        TenantFacades.Gate.Enabled = true;
        TenantFacades.DirectoryFake.AddMemberAsync(Acme, "ada", TenantSubjectKind.User).GetAwaiter().GetResult();
        TenantFacades.Gate.Calls.Clear();
    }

    private IRenderedComponent<AccessExplainPage> Explain()
    {
        var cut = RenderAt<AccessExplainPage>("t/acme/access/explain?subject=ada&kind=user&tree=orders&operation=read");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form[data-lt-tenant-explain]"), Has.Count.EqualTo(1)));
        cut.Find("form[data-lt-tenant-explain]").Submit();
        return cut;
    }

    private static AngleSharp.Dom.IElement Layer(IRenderedComponent<AccessExplainPage> cut, string layer) =>
        cut.Find($"[data-lt-layer={layer}]");
}
