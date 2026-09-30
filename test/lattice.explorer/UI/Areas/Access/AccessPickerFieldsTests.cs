using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// Issue #3949: every Access field that names an existing tree, principal or
/// rule is a type-ahead picker that offers the existing values, and a
/// pick-existing field refuses a value that names nothing before anything is
/// sent.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessPickerFieldsTests : AccessTestContext
{
    [Test]
    public void Explain_offers_the_catalogues_trees_and_refuses_one_that_does_not_exist()
    {
        Services.UseTreeCatalogue("orders", "orders-archive", "billing");
        var cut = RenderAt<AccessExplainPage>("access/explain");

        Assert.That(SuggestionFields.Offers(cut, "Tree", "ord"), Is.EqualTo(new[] { "orders", "orders-archive" }));

        AccessForms.Type(cut, "Subject", "alice");
        SuggestionFields.Box(cut, "Tree").Input("ordrs");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Is.EqualTo("No tree is named ordrs. Choose one from the list.")));
        Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.ExplainAsync)));
    }

    [Test]
    public void Explain_offers_directory_subjects()
    {
        Admin.WithPrincipal("alice", "Alice Liddell", DirectoryPrincipalKind.User);
        var cut = RenderAt<AccessExplainPage>("access/explain");

        Assert.That(SuggestionFields.Offers(cut, "Subject", "ali"), Is.EqualTo(new[] { "alice" }));
    }

    [Test]
    public void The_rule_editor_offers_trees_and_refuses_one_that_does_not_exist()
    {
        Services.UseTreeCatalogue("orders", "billing");
        var cut = RenderEditor();

        Assert.That(SuggestionFields.Offers(cut, "Tree", "bil"), Is.EqualTo(new[] { "billing" }));

        AccessRuleEditorTests.Fill(cut, "readers", "ops", "nowhere");
        cut.Find("[data-lt-operation=\"read\"]").Change(true);
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Is.EqualTo("No tree is named nowhere. Choose one from the list.")));
        Assert.That(Admin.Rules, Is.Empty, "nothing is written for a tree that does not exist");
    }

    [Test]
    public void A_new_rule_id_already_used_under_the_tree_is_refused_in_a_plain_text_box()
    {
        Services.UseTreeCatalogue("orders");
        Admin.WithRule(Rule("readers", tree: "orders"));
        var cut = RenderEditor();
        SuggestionFields.NameBox(cut, "Rule id").Input("read");
        Assert.That(cut.FindAll("[role=option]"), Is.Empty, "existing rule ids are not offered for a new one");

        AccessRuleEditorTests.Fill(cut, "readers", "ops", "orders");
        cut.Find("[data-lt-operation=\"read\"]").Change(true);
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Rule id"), Is.EqualTo("A rule with this id already governs this tree.")));
        Assert.That(Admin.Rules, Has.Count.EqualTo(1), "the existing rule is not overwritten");
    }

    [Test]
    public void The_rule_id_source_lists_only_the_ids_under_the_governed_tree()
    {
        Admin.WithRule(Rule("readers", tree: "orders")).WithRule(Rule("writers", tree: "billing"));
        var tree = "orders";
        var source = new AccessRuleIdSuggestionSource(Services.GetService<AccessCatalog>()!, () => tree);

        var orders = source.SuggestAsync(string.Empty, 10, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        tree = string.Empty;
        var all = source.SuggestAsync(string.Empty, 10, CancellationToken.None).AsTask().GetAwaiter().GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(orders.Items.Select(item => item.Value), Is.EqualTo(new[] { "readers" }));
            Assert.That(all.Items.Select(item => item.Value), Is.EquivalentTo(new[] { "readers", "writers" }));
        });
    }

    [Test]
    public void The_rule_id_source_fails_closed_to_a_note()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ListRulesAsync), new InvalidOperationException("down"));
        var source = new AccessRuleIdSuggestionSource(Services.GetService<AccessCatalog>()!, () => null);

        var answer = source.SuggestAsync("x", 5, CancellationToken.None).AsTask().GetAwaiter().GetResult();

        Assert.That(answer.UnavailableReason, Is.EqualTo(AccessRuleIdSuggestionSource.UnavailableReason));
    }

    private IRenderedComponent<AccessRuleEditor> RenderEditor(LatticeAuthorizationRule? rule = null) =>
        Render<AccessRuleEditor>(parameters => parameters.Add(editor => editor.Rule, rule));
}
