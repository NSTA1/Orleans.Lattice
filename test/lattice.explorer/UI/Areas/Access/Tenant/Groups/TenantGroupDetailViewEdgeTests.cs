using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The tenant group page's edges (<c>/t/acme/access/groups/{name}</c>): the source
/// and scope labels of every rule shape, reads that fail, answer nothing, or answer
/// after the view moved to another group or was left, the "Used by" section when its
/// rules cannot be read, and every refusal and late answer the display name, add,
/// "Add me" and remove writes can meet.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantGroupDetailViewEdgeTests : TenantAccessPagesTestContext
{
    private const string Unreachable = "The cluster could not be reached. Check the connection and try again.";

    [TestCase(TenantRuleOrigin.Tenant, "Tenant rule")]
    [TestCase(TenantRuleOrigin.AppRole, "App role binding")]
    [TestCase(TenantRuleOrigin.PlatformWide, "Platform, all trees")]
    [TestCase(TenantRuleOrigin.PlatformTree, "Platform")]
    public void The_source_label_names_where_a_rule_comes_from(TenantRuleOrigin origin, string expected) =>
        Assert.That(TenantGroupDetailView.SourceLabel(new TenantRuleView { RuleId = "r", Origin = origin }), Is.EqualTo(expected));

    [TestCase(TenantRuleScopeKind.TenantWide, null, null, "All of the tenant's trees")]
    [TestCase(TenantRuleScopeKind.Tree, null, null, "Not shown")]
    [TestCase(TenantRuleScopeKind.Key, "orders", "k1", "Key k1 in orders")]
    [TestCase(TenantRuleScopeKind.Prefix, "orders", "eu/", "Prefix eu/ in orders")]
    [TestCase(TenantRuleScopeKind.Tree, "orders", null, "Tree orders")]
    public void The_scope_label_names_what_a_rule_governs(TenantRuleScopeKind scope, string? tree, string? keyOrPrefix, string expected) =>
        Assert.That(
            TenantGroupDetailView.ScopeLabel(new TenantRuleView { RuleId = "r", ScopeKind = scope, TreeName = tree, KeyOrPrefix = keyOrPrefix }),
            Is.EqualTo(expected));

    [Test]
    public void Rules_that_cannot_be_read_are_told_in_the_used_by_section()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(_ =>
        {
            TenantFacades.Gate.NextFailure = new InvalidOperationException("down");
            return Task.FromResult<IReadOnlyList<TenantGroupMember>>([]);
        });

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-used-by-unavailable]").TextContent, Is.EqualTo("The rules that name this group could not be read."));
            Assert.That(cut.FindAll("[data-lt-used-by] table"), Is.Empty);
        });
    }

    [Test]
    public void Members_that_answer_nothing_are_an_empty_member_set()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>(null!));

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=group]").TextContent, Does.Contain("This group has no direct members.")));
    }

    [Test]
    public void A_name_that_is_no_valid_group_name_is_not_found()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantGroupDescriptor?>(new ArgumentException("not a group name", "name")));
        UseDirectory(directory);

        RenderAt<AccessGroupPage>("t/acme/access/groups/ops").WaitUntil(() => Assert.That(NotFound, Is.EqualTo(1)));
    }

    [Test]
    public void A_confinement_refusal_of_the_read_is_shown_rather_than_not_found()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantGroupDescriptor?>(new TenantAccessConfinementException("acme", TenantAccessConfinementRule.GroupNesting, "nesting", "name")));
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=group]").TextContent, Does.Contain("The group could not be read"));
            Assert.That(NotFound, Is.Zero);
        });
    }

    [Test]
    public void A_denied_read_is_titled_not_permitted()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantGroupDescriptor?>(new LatticeAuthorizationDeniedException("no")));
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=group]").TextContent, Does.Contain("Not permitted")));
    }

    [Test]
    public void Try_again_reads_the_group_again()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantGroupDescriptor?>(new InvalidOperationException("down")),
            Task.FromResult<TenantGroupDescriptor?>(new TenantGroupDescriptor { Name = "ops" }));
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        UseDirectory(directory);
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Try again"), Is.Not.Null));

        AccessForms.Button(cut, "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=group]").TextContent, Does.Contain("This group has no direct members.")));
    }

    [Test]
    public void A_plain_member_is_shown_as_an_unknown_caller()
    {
        TenantFacades.AsMember().WithGroup("acme", "ops");
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));

        var cut = RenderView("ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=group]").GetAttribute("data-lt-caller"), Is.EqualTo("unknown"));
            Assert.That(cut.FindAll("[data-lt-add-me]"), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-operator-note]"), Is.Empty);
        });
    }

    [Test]
    public void Rendering_again_for_the_same_group_does_not_read_again()
    {
        TenantFacades.AsTenantAdmin();
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        var cut = RenderView("ops");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("This group has no direct members.")));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "acme").Add(view => view.Name, "ops"));

        directory.Received(1).GetGroupAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_group_read_for_a_group_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var ops = new TaskCompletionSource<TenantGroupDescriptor?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync("acme", "ops", Arg.Any<CancellationToken>()).Returns(ops.Task);
        directory.GetGroupAsync("acme", "eng", Arg.Any<CancellationToken>()).Returns(new TenantGroupDescriptor { Name = "eng", DisplayName = "Engineering" });
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        UseDirectory(directory);
        var cut = RenderView("ops");
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls(), Is.Not.Empty));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "acme").Add(view => view.Name, "eng"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Engineering")));
        ops.SetResult(new TenantGroupDescriptor { Name = "ops", DisplayName = "Operators" });

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains("Operators", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Markup, Does.Contain("Engineering"));
    }

    [Test]
    public async Task A_member_read_for_a_group_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var ops = new TaskCompletionSource<IReadOnlyList<TenantGroupMember>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync("acme", "ops", Arg.Any<CancellationToken>()).Returns(ops.Task);
        directory.ListGroupMembersAsync("acme", "eng", Arg.Any<CancellationToken>()).Returns(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        var cut = RenderView("ops");
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.ListGroupMembersAsync)), Is.EqualTo(1)));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "acme").Add(view => view.Name, "eng"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("This group has no direct members.")));
        ops.SetResult([Member("alice")]);

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains("alice", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public async Task A_read_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupDescriptor?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = RenderView("ops");
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls(), Is.Not.Empty));
        var token = (CancellationToken)directory.ReceivedCalls().Single().GetArguments()[2]!;

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException(token));

        Assert.That(token.IsCancellationRequested, Is.True, "leaving the view cancels its read");
        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => NotFound > 0 || ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public void A_display_name_of_whitespace_is_saved_as_none()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops", "Operators");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(cut.Find("p.lt-shell-page-lede").TextContent, Is.EqualTo("Operators")));

        AccessForms.Type(cut, "Display name", "   ");
        cut.FindAll("form.lt-access-form")[0].Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(TenantFacades.DirectoryFake.GetGroupAsync("acme", "ops").Result?.DisplayName, Is.Null);
            Assert.That(ToastMessages, Does.Contain("Group ops renamed."));
            Assert.That(cut.FindAll("p.lt-shell-page-lede"), Is.Empty);
        });
    }

    [Test]
    public void A_failed_rename_is_told_under_the_display_name()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops", "Operators");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Display name"), Is.True));
        AccessForms.Type(cut, "Display name", "Site reliability");

        TenantFacades.Gate.NextFailure = new InvalidOperationException("down");
        cut.FindAll("form.lt-access-form")[0].Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Display name"), Is.EqualTo(Unreachable));
            Assert.That(TenantFacades.DirectoryFake.GetGroupAsync("acme", "ops").Result?.DisplayName, Is.EqualTo("Operators"));
            Assert.That(AccessForms.Button(cut, "Save name").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public async Task A_rename_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupDescriptor>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        directory.UpsertGroupAsync(default!, default!, default).ReturnsForAnyArgs(pending.Task);
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Display name"), Is.True));
        AccessForms.Type(cut, "Display name", "Site reliability");
        cut.FindAll("form.lt-access-form")[0].Submit();
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Save name").HasAttribute("disabled"), Is.True));

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public void A_submit_with_no_member_asks_for_one()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));

        AddForm(cut).Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo("Choose the member.")));
        Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.AddGroupMemberAsync)));
    }

    [Test]
    public void A_group_cannot_be_added_to_itself()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member kind"), Is.True));

        AccessForms.Choose(cut, "Member kind", "tenant-group");
        AccessForms.Type(cut, "Member", "ops");
        AddForm(cut).Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo("A group cannot be a member of itself.")));
        Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.AddGroupMemberAsync)));
    }

    [Test]
    public void A_tenant_group_id_typed_as_a_user_is_refused_by_the_picker()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));

        AccessForms.Type(cut, "Member", "t/other/admins");
        AddForm(cut).Submit();

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain(AccessSubjectPicker.ForeignTenantGroupMessage)));
        Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.AddGroupMemberAsync)));
    }

    [Test]
    public void An_add_at_the_membership_cap_is_refused_with_the_cap()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");

        TenantFacades.Gate.NextFailure = new LatticeQuotaExceededException("cap", string.Empty, "MaxMembershipEdges", 10, 10, "acme");
        AddForm(cut).Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo("Tenant acme is at its cap of membership entries, so alice was not added."));
            Assert.That(MemberRows(cut), Is.Empty);
        });
    }

    [Test]
    public void An_added_member_is_announced_and_the_field_is_cleared()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");

        AddForm(cut).Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain("alice added to ops."));
            Assert.That(AccessForms.Field(cut, "Member").GetAttribute("value") ?? string.Empty, Is.Empty);
        });
    }

    [Test]
    public void Adding_a_direct_member_again_does_not_list_it_twice()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([Member("alice")]));
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(MemberRows(cut), Has.Count.EqualTo(1)));
        AccessForms.Type(cut, "Member", "alice");

        AddForm(cut).Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain("alice added to ops."));
            Assert.That(MemberRows(cut), Is.EqualTo(new[] { new[] { "alice", "User" } }));
        });
    }

    [Test]
    public async Task An_add_that_answers_after_the_view_moved_to_another_group_is_not_listed()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMembershipChangeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        directory.AddGroupMemberAsync(default!, default!, default!, default, default).ReturnsForAnyArgs(pending.Task);
        var cut = RenderView("ops");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("This group has no direct members.")));
        cut.FindAll("input").First(input => input.GetAttribute("role") == "combobox" || input.ClassList.Contains("lt-combobox__input")).Input("alice");
        cut.FindAll("form.lt-access-form")[1].Submit();
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.AddGroupMemberAsync)), Is.True));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "acme").Add(view => view.Name, "eng"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("This group has no direct members.")));
        pending.SetResult(null!);

        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0 || cut.Markup.Contains(">alice<", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public async Task An_add_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMembershipChangeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        directory.AddGroupMemberAsync(default!, default!, default!, default, default).ReturnsForAnyArgs(pending.Task);
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");
        AddForm(cut).Submit();
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Add member").HasAttribute("disabled"), Is.True));

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public void A_refused_add_me_is_told_in_a_notice()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-add-me]"), Has.Count.EqualTo(1)));

        TenantFacades.Gate.NextFailure = new InvalidOperationException("down");
        AccessForms.Button(cut, "Add me to this group").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain(Unreachable));
            Assert.That(cut.FindAll("[data-lt-add-me]"), Has.Count.EqualTo(1), "the caller is still offered it");
            Assert.That(MemberRows(cut), Is.Empty);
        });
    }

    [Test]
    public async Task A_removed_member_is_announced()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await AddGroupMemberAsync("ops", "alice");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(MemberRows(cut), Has.Count.EqualTo(1)));

        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() => Assert.That(ToastMessages, Does.Contain("alice removed from ops.")));
    }

    [Test]
    public async Task A_failed_remove_is_told_in_a_notice_and_keeps_the_member()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await AddGroupMemberAsync("ops", "alice");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(MemberRows(cut), Has.Count.EqualTo(1)));
        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");

        TenantFacades.Gate.NextFailure = new InvalidOperationException("down");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain(Unreachable));
            Assert.That(MemberRows(cut), Is.EqualTo(new[] { new[] { "alice", "User" } }));
            Assert.That(TenantFacades.DirectoryFake.ListGroupMembersAsync("acme", "ops").Result, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_remove_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMembershipChangeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = ScriptedDirectory();
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([Member("alice")]));
        directory.RemoveGroupMemberAsync(default!, default!, default!, default, default).ReturnsForAnyArgs(pending.Task);
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(MemberRows(cut), Has.Count.EqualTo(1)));
        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.RemoveGroupMemberAsync)), Is.True));

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public async Task Cancel_closes_the_remove_confirmation_without_a_write()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await AddGroupMemberAsync("ops", "alice");
        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");
        cut.WaitUntil(() => Assert.That(MemberRows(cut), Has.Count.EqualTo(1)));
        cut.Find("button[aria-label='Remove alice']").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-confirm"), Is.Not.Empty));

        AccessForms.Button(cut, "Cancel").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-confirm"), Is.Empty));
        Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.RemoveGroupMemberAsync)));
        Assert.That(MemberRows(cut), Has.Count.EqualTo(1));
    }

    private static TenantGroupMember Member(string id) => new() { MemberId = id, Kind = TenantSubjectKind.User };

    /// <summary>A scripted directory that finds every group by the name asked for; members are left to the test.</summary>
    private ILatticeTenantDirectoryAdmin ScriptedDirectory()
    {
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs(
            call => Task.FromResult<TenantGroupDescriptor?>(new TenantGroupDescriptor { Name = call.ArgAt<string>(1) }));
        UseDirectory(directory);
        return directory;
    }

    private IRenderedComponent<TenantGroupDetailView> RenderView(string name) =>
        Render<TenantGroupDetailView>(parameters => parameters.Add(view => view.Tenant, "acme").Add(view => view.Name, name));

    private static AngleSharp.Dom.IElement AddForm(IRenderedComponent<AccessGroupPage> cut) =>
        cut.FindAll("form.lt-access-form").Single(form => form.QuerySelector(".lt-combobox, [role=combobox]") is not null);

    private static List<string[]> MemberRows(IRenderedComponent<AccessGroupPage> cut) =>
        [.. cut.FindAll("section[aria-labelledby=lt-access-tenant-group-members] tbody tr")
            .Select(row => row.QuerySelectorAll("th, td").Take(2).Select(cell => cell.TextContent.Trim()).ToArray())];
}
