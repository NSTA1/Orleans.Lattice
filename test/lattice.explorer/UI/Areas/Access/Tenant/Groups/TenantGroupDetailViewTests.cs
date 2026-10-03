using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// Issue #4162: one of the tenant's own groups (<c>/t/acme/access/groups/{name}</c>) -
/// its members with each one's kind and source, add through the tenant-aware picker
/// and remove, a nesting refusal shown with its typed reason, "Add me" for a tenant
/// administrator who is not in it, the read-only "Used by" section, its history, its
/// membership cap, delete, and the operator versus tenant administrator states.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantGroupDetailViewTests : TenantAccessPagesTestContext
{
    [Test]
    public async Task Members_are_listed_with_their_kind_and_source()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops").WithGroup("acme", "eng");
        await AddGroupMemberAsync("ops", "alice");
        await AddGroupMemberAsync("ops", "eng", TenantSubjectKind.TenantGroup);
        await AddGroupMemberAsync("ops", "entra-sre", TenantSubjectKind.ClusterGroup);

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            var rows = MemberRows(cut);
            Assert.That(rows, Is.EqualTo(new[]
            {
                new[] { "alice", "User" },
                new[] { "eng", "This tenant's group" },
                new[] { "entra-sre", "Cluster group" },
            }));
            Assert.That(cut.Find("a[href$='t/acme/access/groups/eng']").TextContent, Is.EqualTo("eng"));
        });
    }

    [Test]
    public void While_the_group_is_read_a_skeleton_is_shown()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(new TaskCompletionSource<TenantGroupDescriptor?>().Task);
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=group]").InnerHtml, Does.Contain("Loading the group")));
    }

    [Test]
    public void A_failed_read_shows_the_error_state()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantGroupDescriptor?>(new InvalidOperationException("down")));
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=group]").TextContent, Does.Contain("The group could not be read"));
            Assert.That(AccessForms.Button(cut, "Try again"), Is.Not.Null);
        });
    }

    [Test]
    public void A_group_with_no_members_says_so()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=group]").TextContent, Does.Contain("This group has no direct members.")));
    }

    [Test]
    public void A_member_is_added_through_the_picker()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));

        AccessForms.Type(cut, "Member", "alice");
        AddForm(cut).Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(TenantFacades.DirectoryFake.ListGroupMembersAsync("acme", "ops").Result,
                Is.EqualTo(new[] { new TenantGroupMember { MemberId = "alice", Kind = TenantSubjectKind.User } }));
            Assert.That(MemberRows(cut), Is.EqualTo(new[] { new[] { "alice", "User" } }));
        });
    }

    [Test]
    public void A_tenant_group_is_added_as_a_tenant_group()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops").WithGroup("acme", "eng");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member kind"), Is.True));

        AccessForms.Choose(cut, "Member kind", "tenant-group");
        AccessForms.Type(cut, "Member", "eng");
        AddForm(cut).Submit();

        cut.WaitUntil(() => Assert.That(
            TenantFacades.DirectoryFake.ListGroupMembersAsync("acme", "ops").Result,
            Is.EqualTo(new[] { new TenantGroupMember { MemberId = "eng", Kind = TenantSubjectKind.TenantGroup } })));
    }

    [Test]
    public void A_nesting_refusal_is_shown_with_its_typed_reason()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");

        TenantFacades.Gate.NextFailure = new TenantAccessConfinementException(
            "acme", TenantAccessConfinementRule.GroupNesting, "nesting", "memberId");
        AddForm(cut).Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo(TenantGroupFormat.NestingMessage));
            Assert.That(MemberRows(cut), Is.Empty);
        });
    }

    [Test]
    public async Task A_member_is_removed_after_a_confirmation()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await AddGroupMemberAsync("ops", "alice");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(MemberRows(cut), Has.Count.EqualTo(1)));

        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(TenantFacades.DirectoryFake.ListGroupMembersAsync("acme", "ops").Result, Is.Empty);
            Assert.That(MemberRows(cut), Is.Empty);
        });
    }

    [Test]
    public void A_tenant_admin_who_is_not_in_the_group_is_offered_add_me()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-add-me]"), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "Add me to this group").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(
                TenantFacades.DirectoryFake.ListGroupMembersAsync("acme", "ops").Result,
                Is.EqualTo(new[] { new TenantGroupMember { MemberId = "ops@example.com", Kind = TenantSubjectKind.User } }));
            Assert.That(cut.FindAll("[data-lt-add-me]"), Is.Empty, "a direct member is not offered it again");
        });
    }

    [Test]
    public async Task A_tenant_admin_already_in_the_group_is_not_offered_add_me()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await AddGroupMemberAsync("ops", "ops@example.com");

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(MemberRows(cut), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("[data-lt-add-me]"), Is.Empty);
        });
    }

    [Test]
    public void A_token_sign_in_that_names_no_subject_is_not_offered_add_me()
    {
        Auth.SignIn("Ops Person", Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthSchemes.Entra);
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=group]").GetAttribute("data-lt-caller"), Is.EqualTo("tenant-admin"));
            Assert.That(cut.FindAll("[data-lt-add-me]"), Is.Empty);
        });
    }

    [Test]
    public void An_operator_is_not_offered_add_me_and_is_told_how_they_administer()
    {
        TenantFacades.AsOperator().WithGroup("acme", "ops");

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=group]").GetAttribute("data-lt-caller"), Is.EqualTo("operator"));
            Assert.That(cut.Find("[data-lt-operator-note]").TextContent, Does.Contain("as a platform operator"));
            Assert.That(cut.FindAll("[data-lt-add-me]"), Is.Empty);
            Assert.That(AccessForms.HasField(cut, "Member"), Is.True, "an operator still administers the members");
        });
    }

    [Test]
    public async Task Used_by_lists_the_rules_and_app_role_bindings_read_only()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await NameInTenantRuleAsync("readers", "ops");
        NameInPlatformRule("platform-deny", "ops");
        NameInAppRole("app:crm:viewer:0123", "ops");

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("[data-lt-used-by] tbody tr")
                .Select(row => row.QuerySelectorAll("th, td").Select(cell => cell.TextContent.Trim()).ToArray())
                .ToArray();
            Assert.That(rows, Is.EquivalentTo(new[]
            {
                new[] { "platform-deny", "Platform", "Tree orders", "Deny" },
                new[] { "app:crm:viewer:0123", "App role binding", "Not shown", "Allow" },
                new[] { "readers", "Tenant rule", "All of the tenant's trees", "Allow" },
            }));
            Assert.That(cut.Find("[data-lt-used-by] a").GetAttribute("href"), Does.EndWith("t/acme/access/rules/readers"));
            Assert.That(
                cut.FindAll("[data-lt-used-by] button").Select(button => button.TextContent.Trim()),
                Has.None.AnyOf("Remove", "Delete", "Edit", "Remove rule"),
                "the section is read-only");
        });
    }

    [Test]
    public void The_history_is_read_through_the_history_reader()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        History.LoadAsync("sys-membership-groups", "t/acme/ops", Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new HistoryPage
            {
                Revisions =
                [
                    new HistoryRevisionRow { Hlc = new HybridLogicalClock { WallClockTicks = new DateTimeOffset(2026, 9, 1, 10, 0, 0, TimeSpan.Zero).UtcTicks }, Kind = HistoryRowKind.Set },
                    new HistoryRevisionRow { Hlc = new HybridLogicalClock { WallClockTicks = new DateTimeOffset(2026, 9, 2, 10, 0, 0, TimeSpan.Zero).UtcTicks }, Kind = HistoryRowKind.Set },
                ],
            });

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-group-history]").GetAttribute("data-lt-group-history"), Is.EqualTo("ready"));
            Assert.That(
                cut.FindAll("[data-lt-group-history] tbody tr").Select(row => row.QuerySelector("th, td")!.TextContent.Trim()),
                Is.EqualTo(new[] { "2026-09-02 10:00:00 UTC", "2026-09-01 10:00:00 UTC" }));
            Assert.That(cut.Markup, Does.Not.Contain("sys-membership-groups"), "a physical tree id is never shown");
        });
    }

    [Test]
    public void A_caller_who_may_not_read_the_history_is_told_so()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        History.LoadAsync(default!, default!, default, default, default).ReturnsForAnyArgs(
            Task.FromException<HistoryPage>(new Orleans.Lattice.LatticeAuthorizationDeniedException("no")));

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-group-history]").GetAttribute("data-lt-group-history"), Is.EqualTo("unavailable"));
            Assert.That(cut.Find("[data-lt-group-history]").TextContent, Does.Contain(TenantGroupHistory.DeniedText));
        });
    }

    [Test]
    public void At_the_membership_cap_adding_is_disabled_with_the_reason()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        TenantFacades.PolicyFake.MembershipEdges = new TenantQuotaDimensionUsage { Usage = 10, Limit = 10 };

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.Button(cut, "Add member").HasAttribute("disabled"), Is.True);
            Assert.That(AccessForms.Button(cut, "Add me to this group").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-cap-reason=membership-edges]").TextContent, Does.Contain("cap of 10 membership entries"));
        });
    }

    [Test]
    public void The_display_name_is_edited()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops", "Operators");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Display name"), Is.True));

        AccessForms.Type(cut, "Display name", "Site reliability");
        cut.FindAll("form.lt-access-form")[0].Submit();

        cut.WaitUntil(() => Assert.That(TenantFacades.DirectoryFake.GetGroupAsync("acme", "ops").Result?.DisplayName, Is.EqualTo("Site reliability")));
    }

    [Test]
    public async Task Delete_previews_the_cascade_and_returns_to_the_list()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await AddGroupMemberAsync("ops", "alice");
        await NameInTenantRuleAsync("readers", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(MemberRows(cut), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "Delete group").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-cascade-text]").TextContent, Is.EqualTo("Deleting ops removes 1 member entry, 1 rule, 0 app bindings.")));
        AccessForms.Type(cut, "Group name", "ops");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(TenantFacades.DirectoryFake.GetGroupAsync("acme", "ops").Result, Is.Null);
            Assert.That(Navigation.Uri, Does.EndWith("/t/acme/access/groups"));
        });
    }

    [Test]
    public void The_last_admin_entry_is_refused_with_its_reason()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        TenantFacades.DirectoryFake.SeedAdmin("acme", new TenantMemberEntry { SubjectId = "ops", Kind = TenantSubjectKind.TenantGroup });
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));

        AccessForms.Button(cut, "Delete group").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-cascade-text]").TextContent, Does.Contain("its administrator entry")));
        AccessForms.Type(cut, "Group name", "ops");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain(TenantGroupFormat.LastAdminMessage));
            Assert.That(TenantFacades.DirectoryFake.GetGroupAsync("acme", "ops").Result, Is.Not.Null);
        });
    }

    [Test]
    public void Another_tenants_group_is_not_found()
    {
        TenantFacades.AsTenantAdmin().WithGroup("globex", "ops");

        RenderAt<AccessGroupPage>("t/acme/access/groups/ops").WaitUntil(() => Assert.That(NotFound, Is.EqualTo(1)));
    }

    private static AngleSharp.Dom.IElement AddForm(IRenderedComponent<AccessGroupPage> cut) =>
        cut.FindAll("form.lt-access-form").Single(form => form.QuerySelector(".lt-combobox, [role=combobox]") is not null);

    private static List<string[]> MemberRows(IRenderedComponent<AccessGroupPage> cut) =>
        [.. cut.FindAll("section[aria-labelledby=lt-access-tenant-group-members] tbody tr")
            .Select(row => row.QuerySelectorAll("th, td").Take(2).Select(cell => cell.TextContent.Trim()).ToArray())];
}
