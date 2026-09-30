using Bunit;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The group list and the fail-closed create form: directory validation before
/// anything is written, the server's refusal on the id field, a duplicate id,
/// token-only membership, and the compact form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessGroupsPageTests : AccessTestContext
{
    [Test]
    public void Groups_are_listed_with_links_and_the_search_narrows_them()
    {
        Admin.WithGroup("ops", "Operations").WithGroup("sales", "Sales team");

        var cut = RenderAt<AccessGroupsPage>("access/groups");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th a").Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "access/groups/ops", "access/groups/sales" })));

        cut.Find("input[type=search]").Input("team");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { "sales" }));
            Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("1 of 2 groups"));
        });
    }

    [Test]
    public void While_groups_load_the_page_shows_a_skeleton()
    {
        var hold = Admin.Hold(nameof(FakeAuthAdmin.ListGroupsAsync));

        var cut = RenderAt<AccessGroupsPage>("access/groups");

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("No groups are defined on this cluster yet.")));
    }

    [Test]
    public void The_create_group_command_has_a_visible_control_and_its_target_opens_the_form()
    {
        var command = new AccessArea(Services).Commands.Single(candidate => candidate.Id == AccessArea.CreateGroupCommandId);

        var cut = RenderAt<AccessGroupsPage>(command.Target!.ToHref());

        cut.WaitUntil(() =>
        {
            ExplorerCommandControls.AssertVisibleControl(cut, command);
            Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("New group"));
        });
    }

    [Test]
    public void The_new_group_id_is_a_plain_text_box_not_a_combo_box()
    {
        Admin.WithPrincipal("0f1e-ops", "Operations", DirectoryPrincipalKind.Group).WithGroup("ops");
        var cut = OpenCreate();

        var input = AccessForms.Field(cut, "Group id");
        input.Input("o");

        Assert.Multiple(() =>
        {
            Assert.That(input.TagName, Is.EqualTo("INPUT"));
            Assert.That(input.GetAttribute("type"), Is.EqualTo("text"));
            Assert.That(input.HasAttribute("role"), Is.False, "a text box, not a combobox");
            Assert.That(input.HasAttribute("aria-controls") || input.HasAttribute("aria-expanded"), Is.False, "it owns no popup");
            Assert.That(cut.FindAll(".lt-dialog [role=combobox], .lt-dialog [role=listbox], .lt-dialog [role=option], .lt-dialog .lt-combobox__chevron"), Is.Empty);
        });
    }

    [Test]
    public void With_a_directory_an_unresolved_id_is_refused_before_anything_is_written()
    {
        Admin.WithPrincipal("alice", "Alice", DirectoryPrincipalKind.User);
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "ghost");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.EqualTo("ghost is not a group in the identity directory (Microsoft Entra ID)."));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.UpsertGroupAsync)));
        });
    }

    [Test]
    public void With_a_directory_an_id_it_does_not_list_is_refused_by_name_when_the_field_is_left()
    {
        Admin.WithPrincipal("0f1e-ops", "Operations", DirectoryPrincipalKind.Group);
        Admin.Model = Admin.Model with { DirectoryProviderId = "static" };
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "ghost");
        Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.Null, "the directory is asked once per id, not once per key");
        AccessForms.Field(cut, "Group id").Blur();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.EqualTo("ghost is not a group in the identity directory (static roster)."));
            Assert.That(AccessForms.Field(cut, "Group id").GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.UpsertGroupAsync)));
        });
    }

    [Test]
    public void With_a_directory_a_user_id_typed_as_a_group_is_refused()
    {
        Admin.WithPrincipal("alice", "Alice", DirectoryPrincipalKind.User);
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "alice");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.EqualTo("alice is a user in the identity directory (Microsoft Entra ID), not a group.")));
    }

    [Test]
    public void A_directory_group_that_is_not_defined_yet_is_created_with_its_display_name()
    {
        Admin.WithPrincipal("0f1e-ops", "Operations", DirectoryPrincipalKind.Group);
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "0f1e-ops");
        AccessForms.Type(cut, "Display name (optional)", "Operations");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Groups["0f1e-ops"].DisplayName, Is.EqualTo("Operations"));
            Assert.That(Navigation.Uri, Does.EndWith("/access/groups/0f1e-ops"));
        });
    }

    [Test]
    public void Without_a_directory_the_id_is_taken_as_typed_and_the_form_says_so()
    {
        var cut = OpenCreate();

        Assert.That(AccessForms.Field(cut, "Group id").Closest(".lt-field")!.QuerySelector(".lt-field__hint")!.TextContent,
            Is.EqualTo("No identity directory is configured, so the id is used as typed. It must not name a group that already exists."));
        Assert.That(AccessForms.HasField(cut, "Subject kind") || cut.FindAll("button").Any(button => button.TextContent == "Search the directory"), Is.False);

        AccessForms.Type(cut, "Group id", "anything");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(Admin.Groups.ContainsKey("anything"), Is.True));
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void The_servers_directory_refusal_is_shown_beside_the_id(bool typed)
    {
        Exception failure = typed
            ? LatticeDirectoryValidationException.KindMismatch("bob", DirectoryPrincipalKind.Group, DirectoryPrincipalKind.User, "group")
            : new ArgumentException("Directory validation failed: the id 'bob' resolves to a User principal, but a Group was expected.");
        Admin.Fail(nameof(FakeAuthAdmin.UpsertGroupAsync), failure);
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "bob");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Group id"), Does.StartWith("Directory validation failed:")));
    }

    [Test]
    public void An_id_that_already_names_a_group_is_refused_as_it_is_typed()
    {
        Admin.WithGroup("ops");
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "ops");

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.EqualTo("A group named ops already exists."));
            Assert.That(AccessForms.Field(cut, "Group id").GetAttribute("aria-invalid"), Is.EqualTo("true"));
        });

        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.EqualTo("A group named ops already exists."));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.UpsertGroupAsync)));
        });
    }

    [Test]
    public void A_group_beyond_the_listed_page_is_still_refused_before_it_is_written()
    {
        Admin.WithGroup("ops");
        Admin.ForcedPageSize = 1;
        Admin.WithGroup("aaa");
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "ops");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.EqualTo("A group named ops already exists."));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.UpsertGroupAsync)));
        });
    }

    [Test]
    [TestCase("denied", "The group was not created: you are not permitted to administer access on this cluster. Ask a cluster administrator for the Admin grant on access administration.")]
    [TestCase("unreachable", "The group was not created: the cluster could not be reached. Check the connection and try again.")]
    [TestCase("fault", "The group was not created: the cluster did not answer. Try again.")]
    public void A_refused_create_says_why_and_keeps_the_dialog_open_with_the_id(string refusal, string expected)
    {
        Exception failure = refusal switch
        {
            "denied" => new LatticeAuthorizationDeniedException("_lattice_policy", LatticeOperation.Admin, "ops", "no"),
            "unreachable" => new InvalidOperationException("channel down"),
            _ => new TimeoutException("slow"),
        };
        Admin.Fail(nameof(FakeAuthAdmin.UpsertGroupAsync), failure);
        var cut = OpenCreate();

        AccessForms.Type(cut, "Group id", "new-team");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-dialog [data-lt-field=form][role=alert]").TextContent.Trim(), Is.EqualTo(expected));
            Assert.That(AccessForms.Field(cut, "Group id").GetAttribute("value"), Is.EqualTo("new-team"));
            Assert.That(cut.Markup, Does.Not.Contain("channel down").And.Not.Contain("slow"));
            Assert.That(Navigation.Uri, Does.Not.Contain("/access/groups/new-team"));
        });
    }

    [Test]
    public void At_a_tenant_rooted_address_the_page_says_groups_are_cluster_wide_and_links_there()
    {
        Admin.WithGroup("ops");

        var cut = RenderAt<AccessGroupsPage>("t/default/access/groups");

        cut.WaitUntil(() =>
        {
            var caveat = cut.Find("[data-lt-cluster-wide-groups]");
            Assert.That(caveat.TextContent, Does.Contain("Groups belong to the whole cluster, not to one tenant"));
            Assert.That(caveat.TextContent, Does.Not.Contain("No groups").And.Not.Contain("none"));
            Assert.That(caveat.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("access/groups"));
            Assert.That(cut.FindAll("[data-lt-command=\"access.create-group\"]"), Is.Empty, "a group is not created here");
            Assert.That(cut.FindAll(".lt-table__empty"), Is.Empty);
        });
    }

    [Test]
    public void Asked_to_create_a_group_at_a_tenant_rooted_address_the_page_links_to_where_it_is_created()
    {
        var cut = RenderAt<AccessGroupsPage>("t/default/access/groups?new=true");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty);
            Assert.That(cut.Find("[data-lt-create-group-elsewhere] a").GetAttribute("href"), Is.EqualTo("access/groups?new=true"));
        });
    }

    [Test]
    public void A_blank_id_is_refused()
    {
        var cut = OpenCreate();

        cut.Find("form.lt-access-form").Submit();

        Assert.That(AccessForms.ErrorOf(cut, "Group id"), Is.EqualTo("Enter the group id."));
    }

    [Test]
    public void Under_token_only_membership_create_is_off_and_the_banner_says_why()
    {
        Admin.Model = Admin.Model with { LocalMembershipEffective = false };
        Admin.WithGroup("ops");

        var cut = RenderAt<AccessGroupsPage>("access/groups?new=true");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-command=\"access.create-group\"]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty, "the create command does not open the form either");
            Assert.That(cut.FindAll(".lt-access-notice").Select(notice => notice.TextContent), Has.Some.Contains("only from identity tokens"));
            Assert.That(cut.FindAll("tbody th"), Has.Count.EqualTo(1), "existing groups stay visible");
        });
    }

    [Test]
    public void A_restricted_identity_is_told_it_is_not_permitted()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new LatticeAuthorizationDeniedException("_lattice_policy", LatticeOperation.Admin, "ops", "no"));

        var cut = RenderAt<AccessGroupsPage>("access/groups");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Not permitted")));
    }

    [Test]
    public void Load_more_appends_the_next_page()
    {
        Admin.ForcedPageSize = 1;
        Admin.WithGroup("a").WithGroup("b");
        var cut = RenderAt<AccessGroupsPage>("access/groups");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th"), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "Load more groups").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { "a", "b" })));
    }

    [Test]
    public void Below_the_small_breakpoint_groups_are_rows_opening_a_sheet_and_create_is_a_sheet()
    {
        Admin.WithGroup("ops", "Operations");

        var cut = RenderAt<AccessGroupsPage>("access/groups", LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent.Trim(), Is.EqualTo("ops"));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent.Trim(), Is.EqualTo("Operations"));
        });

        cut.Find(".lt-table-list__open").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog .lt-dialog__actions a").GetAttribute("href"), Is.EqualTo("access/groups/ops")));

        var create = RenderAt<AccessGroupsPage>("access/groups?new=true", LtBreakpoint.Compact);
        create.WaitUntil(() => Assert.That(create.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end")));
    }

    private IRenderedComponent<AccessGroupsPage> OpenCreate()
    {
        var cut = RenderAt<AccessGroupsPage>("access/groups?new=true");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-access-form"), Has.Count.EqualTo(1)));
        return cut;
    }
}
