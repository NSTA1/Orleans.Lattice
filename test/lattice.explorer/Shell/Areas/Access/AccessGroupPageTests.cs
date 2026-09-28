using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// One group's page: its members, the add and confirmed remove lifecycle with
/// directory validation on the member field, rename, confirmed delete, the
/// groups it belongs to and the rules that apply to it, and token-only membership.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessGroupPageTests : AccessTestContext
{
    [Test]
    public void The_group_shows_its_members_parents_and_rules()
    {
        Admin.WithGroup("ops", "Operations", "alice", "oncall")
            .WithGroup("oncall", "On call")
            .WithGroup("staff", "Staff", "ops")
            .WithRule(Rule("orders-read", group: "ops"))
            .WithRule(Rule("staff-read", tree: "wiki", group: "staff"))
            .WithRule(Rule("unrelated", group: "sales"));

        var cut = RenderAt<AccessGroupPage>("access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("ops"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Is.EqualTo("Operations"));
            var members = cut.FindAll("section")[1].QuerySelectorAll("tbody tr");
            Assert.That(members.Select(row => row.QuerySelector("th")!.TextContent.Trim()), Is.EqualTo(new[] { "alice", "oncall" }));
            Assert.That(members[1].QuerySelector("th a")!.GetAttribute("href"), Is.EqualTo("access/groups/oncall"), "a nested group links to its page");
            Assert.That(members[0].QuerySelector("th a"), Is.Null);
            Assert.That(cut.Find(".lt-access-links a").GetAttribute("href"), Is.EqualTo("access/groups/staff"));
            Assert.That(cut.FindAll("section")[3].QuerySelectorAll("tbody th").Select(cell => cell.TextContent), Is.EquivalentTo(new[] { "orders-read", "staff-read" }));
        });
    }

    [Test]
    public void Adding_a_member_goes_through_the_facade_and_lists_it()
    {
        Admin.WithGroup("ops");
        var cut = Loaded();

        AccessForms.Choose(cut, "Member kind", "group");
        AccessForms.Type(cut, "Member", "oncall");
        cut.Find("section:nth-of-type(2) form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Members["ops"], Does.Contain("oncall"));
            Assert.That(Admin.MemberKinds[("ops", "oncall")], Is.EqualTo(MembershipMemberKind.Group));
            Assert.That(cut.FindAll("section")[1].QuerySelectorAll("tbody th a").Select(link => link.TextContent), Does.Contain("oncall"));
            Assert.That(AccessForms.Field(cut, "Member").GetAttribute("value"), Is.Empty);
        });
    }

    [Test]
    public void With_a_directory_an_unresolved_member_is_refused_on_the_field()
    {
        Admin.WithGroup("ops").WithPrincipal("alice", "Alice", DirectoryPrincipalKind.User);
        var cut = Loaded();

        AccessForms.Type(cut, "Member", "mallory");
        cut.Find("section:nth-of-type(2) form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo("No principal with the id mallory exists in the identity directory."));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.AddMemberAsync)));
        });
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void The_servers_directory_refusal_of_a_member_is_a_field_error(bool typed)
    {
        Exception failure = typed
            ? LatticeDirectoryValidationException.Unresolved("mallory", DirectoryPrincipalKind.User, "memberId")
            : new ArgumentException("Directory validation failed: the User id 'mallory' does not resolve to any principal in the configured identity directory.");
        Admin.WithGroup("ops");
        Admin.Fail(nameof(FakeAuthAdmin.AddMemberAsync), failure);
        var cut = Loaded();

        AccessForms.Type(cut, "Member", "mallory");
        cut.Find("section:nth-of-type(2) form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Member"), Does.StartWith("Directory validation failed:"));
            Assert.That(AccessForms.Field(cut, "Member").GetAttribute("aria-invalid"), Is.EqualTo("true"));
        });
    }

    [Test]
    public void A_group_cannot_join_itself_and_a_blank_member_is_refused()
    {
        Admin.WithGroup("ops");
        var cut = Loaded();

        cut.Find("section:nth-of-type(2) form").Submit();
        Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo("Enter the member's id."));

        AccessForms.Type(cut, "Member", "ops");
        cut.Find("section:nth-of-type(2) form").Submit();
        Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo("A group cannot be a member of itself."));
    }

    [Test]
    public void Removing_a_member_asks_for_its_id_first()
    {
        Admin.WithGroup("ops", null, "alice");
        var cut = Loaded();

        cut.Find("button[aria-label=\"Remove alice\"]").Click();
        Assert.That(cut.Find(".lt-confirm__name").TextContent, Is.EqualTo("alice"));
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Members["ops"], Is.Empty);
            Assert.That(cut.FindAll("section")[1].QuerySelector(".lt-table__empty")!.TextContent.Trim(), Is.EqualTo("This group has no direct members."));
        });
    }

    [Test]
    public void Renaming_saves_the_display_name()
    {
        Admin.WithGroup("ops", "Ops");
        var cut = Loaded();

        AccessForms.Type(cut, "Display name", "Operations");
        cut.Find("section:nth-of-type(1) form").Submit();

        cut.WaitUntil(() => Assert.That(Admin.Groups["ops"].DisplayName, Is.EqualTo("Operations")));
    }

    [Test]
    public void Deleting_asks_for_the_group_id_then_removes_it_and_returns_to_the_list()
    {
        Admin.WithGroup("ops");
        var cut = Loaded();

        AccessForms.Button(cut, "Delete group").Click();
        AccessForms.Type(cut, "Group name", "ops");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Groups, Is.Empty);
            Assert.That(Navigation.Uri, Does.EndWith("/access/groups"));
        });
    }

    [Test]
    public void Under_token_only_membership_members_stay_visible_but_cannot_change()
    {
        Admin.Model = Admin.Model with { LocalMembershipEffective = false };
        Admin.WithGroup("ops", null, "alice");

        var cut = Loaded();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-access-notice").TextContent, Does.Contain("only from identity tokens"));
            Assert.That(cut.Find("button[aria-label=\"Remove alice\"]").HasAttribute("disabled"), Is.True);
            Assert.That(AccessForms.Button(cut, "Add member").HasAttribute("disabled"), Is.True);
            Assert.That(AccessForms.Field(cut, "Member").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void An_unknown_group_is_not_found()
    {
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt<AccessGroupPage>("access/groups/missing");

        Assert.That(notFound, Is.EqualTo(1));
    }

    [Test]
    public void A_failed_removal_is_reported_and_the_member_stays()
    {
        Admin.WithGroup("ops", null, "alice");
        Admin.Fail(nameof(FakeAuthAdmin.RemoveMemberAsync), new InvalidOperationException("down"));
        var toasts = Services.GetRequiredService<LtToastService>();
        var cut = Loaded();

        cut.Find("button[aria-label=\"Remove alice\"]").Click();
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Members["ops"], Does.Contain("alice"));
            Assert.That(toasts.Toasts, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Below_the_small_breakpoint_a_members_actions_are_in_its_detail_sheet()
    {
        Admin.WithGroup("ops", null, "alice");

        var cut = RenderAt<AccessGroupPage>("access/groups/ops", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__open"), Has.Count.EqualTo(1)));

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("alice"));
            Assert.That(cut.Find(".lt-dialog__actions button").TextContent.Trim(), Is.EqualTo("Remove member"));
        });
    }

    [Test]
    public void The_explain_link_asks_for_the_groups_effective_permissions()
    {
        Admin.WithGroup("ops");

        var cut = Loaded();

        Assert.That(cut.FindAll("a").Single(link => link.TextContent == "Explain for this group").GetAttribute("href"),
            Is.EqualTo("access/explain?subject=ops&kind=group&view=permissions"));
    }

    private IRenderedComponent<AccessGroupPage> Loaded()
    {
        var cut = RenderAt<AccessGroupPage>("access/groups/ops");
        cut.WaitUntil(() => Assert.That(cut.FindAll("section"), Has.Count.EqualTo(4)));
        return cut;
    }
}
