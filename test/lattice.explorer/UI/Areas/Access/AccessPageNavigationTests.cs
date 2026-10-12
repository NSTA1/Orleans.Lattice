using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>Address changes on reused Access pages and answers from superseded reads.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessPageNavigationTests : AccessTestContext
{
    [Test]
    public void A_create_rule_command_opens_the_editor_on_an_already_loaded_list()
    {
        var cut = RenderAt<AccessRulesPage>("access/rules");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table__empty"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo("access/rules?new=true");
        cut.Render();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("New rule")));
        Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ListRulesAsync)), Is.EqualTo(1));
    }

    [Test]
    public void A_create_group_command_opens_the_dialog_on_an_already_loaded_list()
    {
        var cut = RenderAt<AccessGroupsPage>("access/groups");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table__empty"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo("access/groups?new=true");
        cut.Render();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("New group")));
        Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ListGroupsAsync)), Is.EqualTo(1));
    }

    [Test]
    public void A_changed_explain_address_asks_the_new_question_and_resets_the_scope()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&operation=read&tree=orders&key=old");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-verdict"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo("access/explain?subject=bob&operation=appinstall");
        cut.Render();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.Field(cut, "Subject").GetAttribute("value"), Is.EqualTo("bob"));
            Assert.That(AccessForms.HasField(cut, "Tree"), Is.False);
            Assert.That(cut.FindAll(".lt-dl__value").Select(value => value.TextContent.Trim()), Does.Contain("bob").And.Contain("all trees (cluster-wide)"));
        });
        Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ExplainAsync)), Is.EqualTo(2));
    }

    [Test]
    public void A_permissions_address_replaces_the_previous_explanation()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&operation=read&tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-verdict"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo("access/explain?subject=bob&view=permissions");
        cut.Render();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("#lt-access-permissions").TextContent, Does.Contain("bob"));
            Assert.That(cut.FindAll(".lt-access-verdict"), Is.Empty);
        });
    }

    [Test]
    public void An_empty_explain_address_clears_the_previous_answer_and_form()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&operation=read&tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-verdict"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo("access/explain");
        cut.Render();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-access-verdict"), Is.Empty);
            Assert.That(AccessForms.Field(cut, "Subject").GetAttribute("value"), Is.Empty);
            Assert.That(AccessForms.Field(cut, "Tree").GetAttribute("value"), Is.Empty);
        });
    }

    [Test]
    public Task A_superseded_rule_list_failure_does_not_replace_the_new_list() =>
        AssertLateListFailureAsync<AccessRulesPage>("rules", nameof(FakeAuthAdmin.ListRulesAsync), "Rules could not be read");

    [Test]
    public Task A_superseded_group_list_failure_does_not_replace_the_new_list() =>
        AssertLateListFailureAsync<AccessGroupsPage>("groups", nameof(FakeAuthAdmin.ListGroupsAsync), "Groups could not be read");

    [Test]
    public async Task A_superseded_explanation_cannot_replace_the_current_permissions()
    {
        var previous = Admin.Hold(nameof(FakeAuthAdmin.ExplainAsync));
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&operation=read&tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo("access/explain?subject=bob&view=permissions");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("#lt-access-permissions").TextContent, Does.Contain("bob")));

        previous.SetResult();

        Assert.That(await TestPoll.TryUntilAsync(() => cut.FindAll(".lt-access-verdict").Count > 0, TimeSpan.FromSeconds(1)),
            Is.False, "the old question never replaces the current answer");
    }

    [TestCase(false)]
    [TestCase(true)]
    public void A_submitted_question_is_not_asked_again_when_its_address_renders(bool permissions)
    {
        var cut = RenderAt<AccessExplainPage>("access/explain");
        AccessForms.Type(cut, "Subject", "alice");
        if (permissions)
        {
            AccessForms.Button(cut, "Effective permissions").Click();
        }
        else
        {
            AccessForms.Type(cut, "Tree", "orders");
            cut.Find("form.lt-access-form").Submit();
        }

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.Contain("subject=alice")));
        cut.Render();

        var method = permissions ? nameof(FakeAuthAdmin.EffectivePermissionsAsync) : nameof(FakeAuthAdmin.ExplainAsync);
        Assert.That(Admin.Calls.Count(call => call == method), Is.EqualTo(1));
    }

    [TestCase("rules")]
    [TestCase("groups")]
    public void A_dismissed_create_command_is_not_reopened_by_an_unchanged_address(string segment)
    {
        var cut = RenderList(segment, $"access/{segment}?new=true");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog__title"), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "Cancel").Click();
        cut.Render();

        Assert.That(cut.FindAll(".lt-dialog__title"), Is.Empty);
    }

    [TestCase("rules", nameof(FakeAuthAdmin.ListRulesAsync))]
    [TestCase("groups", nameof(FakeAuthAdmin.ListGroupsAsync))]
    public async Task A_superseded_page_cannot_append_to_the_current_list(string segment, string method)
    {
        Admin.ForcedPageSize = 1;
        Admin.WithRule(Rule("a-read")).WithRule(Rule("b-read")).WithGroup("a").WithGroup("b");
        var cut = RenderList(segment, $"access/{segment}");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th"), Has.Count.EqualTo(1)));
        var previous = Admin.Hold(method);

        AccessForms.Button(cut, $"Load more {segment}").Click();
        Navigation.NavigateTo($"t/default/access/{segment}");
        cut.Render();
        Admin.Hold(method).SetResult();
        Navigation.NavigateTo($"access/{segment}");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th"), Has.Count.EqualTo(1)));

        previous.SetResult();

        Assert.That(await TestPoll.TryUntilAsync(() => cut.FindAll("tbody th").Count > 1, TimeSpan.FromSeconds(1)),
            Is.False, "the old page never appends to the fresh listing");
        AccessForms.Button(cut, $"Load more {segment}").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th"), Has.Count.EqualTo(2)));
    }

    private IRenderedComponent<IComponent> RenderList(string segment, string address) =>
        segment == "rules"
            ? RenderAt<AccessRulesPage>(address)
            : RenderAt<AccessGroupsPage>(address);

    private async Task AssertLateListFailureAsync<TPage>(string segment, string method, string staleTitle)
        where TPage : IComponent
    {
        Admin.WithRule(Rule("orders-read")).WithGroup("ops");
        var previous = Admin.Hold(method);
        var cut = RenderAt<TPage>($"access/{segment}");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo($"t/default/access/{segment}");
        cut.Render();
        Admin.Hold(method).SetResult();
        Navigation.NavigateTo($"access/{segment}");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th"), Has.Count.EqualTo(1)));

        previous.SetException(new TimeoutException("previous listing"));

        Assert.That(await TestPoll.TryUntilAsync(() => cut.FindAll(".lt-empty h2").Any(title => title.TextContent == staleTitle),
            TimeSpan.FromSeconds(1)), Is.False, "the superseded load never replaces the current list with its fault");
        Assert.That(cut.FindAll("tbody th"), Has.Count.EqualTo(1));
    }
}
