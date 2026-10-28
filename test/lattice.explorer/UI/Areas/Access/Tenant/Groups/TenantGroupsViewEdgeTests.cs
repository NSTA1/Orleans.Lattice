using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The tenant groups list's edges (<c>/t/acme/access/groups</c>): paging with
/// "Load more groups" and its usage read in batches, pages that answer nothing or
/// fail, figures that could not be read, every refusal a create can meet, the
/// create and delete dialogs closed without a write, and reads that answer after
/// the view moved to another tenant or was left.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantGroupsViewEdgeTests : TenantAccessPagesTestContext
{
    private const string Unreachable = "The cluster could not be reached. Check the connection and try again.";

    [Test]
    public void Load_more_appends_the_next_page_and_reads_its_usage_in_batches()
    {
        TenantFacades.AsTenantAdmin();
        var next = Enumerable.Range(0, TenantGroupsView.UsageBatch + 1).Select(i => Group($"g{i:D2}")).ToArray();
        var directory = PagedDirectory(new TenantGroupPage { Entries = next });

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() =>
        {
            Assert.That(Names(cut), Is.EqualTo(new[] { "alpha" }));
            Assert.That(Count(cut), Is.EqualTo("1 group, more to load"));
        });

        AccessForms.Button(cut, "Load more groups").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Names(cut), Has.Count.EqualTo(TenantGroupsView.UsageBatch + 2));
            Assert.That(Count(cut), Is.EqualTo($"{TenantGroupsView.UsageBatch + 2} groups"));
            Assert.That(Figures(cut).Select(row => row[0]), Is.All.EqualTo("0"), "every group's usage was read, across both batches");
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Load more groups"));
        });
        directory.Received(1).ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_next_page_that_answers_nothing_ends_the_paging()
    {
        TenantFacades.AsTenantAdmin();
        PagedDirectory(next: null);

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(Count(cut), Is.EqualTo("1 group, more to load")));

        AccessForms.Button(cut, "Load more groups").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Names(cut), Is.EqualTo(new[] { "alpha" }));
            Assert.That(Count(cut), Is.EqualTo("1 group"));
        });
    }

    [Test]
    public void A_failed_next_page_is_told_in_a_notice_and_keeps_the_rows()
    {
        TenantFacades.AsTenantAdmin();
        var directory = PagedDirectory(new TenantGroupPage());
        directory.ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TenantGroupPage>(new InvalidOperationException("down")));

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(Names(cut), Is.EqualTo(new[] { "alpha" })));

        AccessForms.Button(cut, "Load more groups").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain(Unreachable));
            Assert.That(Names(cut), Is.EqualTo(new[] { "alpha" }));
            Assert.That(AccessForms.Button(cut, "Load more groups"), Is.Not.Null, "the page can still be asked for again");
        });
    }

    [Test]
    public void A_read_that_answers_nothing_is_an_empty_group_set()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<TenantGroupPage>(null!));
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=groups]").TextContent, Does.Contain("Tenant acme has no groups of its own yet."));
            Assert.That(Count(cut), Is.EqualTo("0 groups"));
        });
    }

    [Test]
    public void A_denied_read_is_titled_not_permitted()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantGroupPage>(new LatticeAuthorizationDeniedException("no")));
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=groups]").TextContent, Does.Contain("Not permitted"));
            Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.True);
            Assert.That(cut.FindAll("[data-lt-tenant-view=groups] .lt-access-count[role=status]"), Is.Empty);
        });
    }

    [Test]
    public void A_figure_that_could_not_be_read_is_shown_as_a_dash()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage { Entries = [Group("ops")] });
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<IReadOnlyList<TenantGroupMember>>(new InvalidOperationException("down")));
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() => Assert.That(Figures(cut), Is.EqualTo(new[] { new[] { "-", "0", "0" } })));
    }

    [Test]
    public void Rendering_again_for_the_same_tenant_does_not_read_again()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage { Entries = [Group("ops")] });
        UseDirectory(directory);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("ops")));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "acme"));

        directory.Received(1).ListGroupsAsync(Arg.Any<string>(), Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_read_for_a_tenant_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var acme = new TaskCompletionSource<TenantGroupPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>()).Returns(acme.Task);
        directory.ListGroupsAsync("globex", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantGroupPage { Entries = [Group("finance")] });
        UseDirectory(directory);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls(), Is.Not.Empty));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("finance")));
        acme.SetResult(new TenantGroupPage { Entries = [Group("ops")] });

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains(">ops<", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public async Task A_read_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls(), Is.Not.Empty));
        var token = (CancellationToken)directory.ReceivedCalls().Single().GetArguments()[2]!;

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException(token));

        Assert.That(token.IsCancellationRequested, Is.True, "leaving the view cancels its read");
        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public async Task A_usage_read_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<IReadOnlyList<TenantGroupMember>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage { Entries = [Group("ops")] });
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("ops")));

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public async Task A_usage_read_for_a_tenant_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<IReadOnlyList<TenantGroupMember>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantGroupPage { Entries = [Group("ops")] });
        directory.ListGroupsAsync("globex", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantGroupPage());
        directory.ListGroupMembersAsync("acme", Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(pending.Task);
        UseDirectory(directory);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("ops")));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Tenant globex has no groups of its own yet.")));
        pending.SetResult([new TenantGroupMember { MemberId = "alice", Kind = TenantSubjectKind.User }]);

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains("ops", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public void A_second_load_more_while_one_is_in_flight_asks_for_nothing_more()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = PagedDirectory(null);
        directory.ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(pending.Task);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        var loadMore = AccessForms.Button(cut, "Load more groups");

        loadMore.Click();
        loadMore.Click();
        pending.SetResult(new TenantGroupPage { Entries = [Group("beta")] });

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));
        directory.Received(1).ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_next_page_for_a_tenant_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = PagedDirectory(null);
        directory.ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(pending.Task);
        directory.ListGroupsAsync("globex", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantGroupPage { Entries = [Group("finance")] });
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        AccessForms.Button(cut, "Load more groups").Click();

        cut.Render(parameters => parameters.Add(view => view.Tenant, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("finance")));
        pending.SetResult(new TenantGroupPage { Entries = [Group("beta")] });

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains(">beta<", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public async Task A_next_page_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = PagedDirectory(null);
        directory.ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(pending.Task);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        AccessForms.Button(cut, "Load more groups").Click();

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public void Create_with_an_empty_name_is_refused_beside_the_name()
    {
        TenantFacades.AsTenantAdmin();
        var cut = OpenCreate();

        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group name"), Is.Not.Null.And.Not.Empty);
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.UpsertGroupAsync)));
        });
    }

    [Test]
    public void Create_without_a_display_name_writes_none_and_says_so()
    {
        TenantFacades.AsTenantAdmin();
        var cut = OpenCreate();
        AccessForms.Type(cut, "Group name", "eng");
        AccessForms.Type(cut, "Display name (optional)", "   ");

        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            var created = TenantFacades.DirectoryFake.GetGroupAsync("acme", "eng").Result;
            Assert.That(created, Is.Not.Null);
            Assert.That(created!.DisplayName, Is.Null);
            Assert.That(ToastMessages, Does.Contain("Group eng created."));
            Assert.That(Navigation.Uri, Does.EndWith("/t/acme/access/groups/eng"));
        });
    }

    [Test]
    public void A_name_the_directory_finds_on_the_exact_look_up_is_refused()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage());
        directory.GetGroupAsync("acme", "eng", Arg.Any<CancellationToken>()).Returns(Group("eng"));
        UseDirectory(directory);
        var cut = OpenCreate();
        AccessForms.Type(cut, "Group name", "eng");

        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Group name"), Is.EqualTo("Tenant acme already has a group named eng.")));
        directory.DidNotReceiveWithAnyArgs().UpsertGroupAsync(default!, default!, default);
    }

    [Test]
    public void An_invalid_name_refused_by_the_directory_is_shown_beside_the_name()
    {
        TenantFacades.AsTenantAdmin();
        var directory = CreatingDirectory(new ArgumentException("The name is reserved.", "name"));
        var cut = OpenCreate();
        AccessForms.Type(cut, "Group name", "eng");

        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group name"), Is.Not.Null.And.Not.Empty);
            Assert.That(cut.FindAll("[data-lt-field=form]"), Is.Empty);
        });
        directory.ReceivedWithAnyArgs(1).UpsertGroupAsync(default!, default!, default);
    }

    [Test]
    public void Any_other_refusal_is_shown_under_the_form()
    {
        TenantFacades.AsTenantAdmin();
        CreatingDirectory(new InvalidOperationException("down"));
        var cut = OpenCreate();
        AccessForms.Type(cut, "Group name", "eng");

        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-field=form]").TextContent, Is.EqualTo("The group was not created: " + Unreachable)));
        Assert.That(cut.FindAll(".lt-dialog form.lt-access-form, form.lt-access-form"), Is.Not.Empty, "the dialog stays open");
    }

    [Test]
    public async Task A_create_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupDescriptor>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage());
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs((TenantGroupDescriptor?)null);
        directory.UpsertGroupAsync(default!, default!, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = Render<TenantGroupsView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.False));
        AccessForms.Button(cut, "New group").Click();
        AccessForms.Type(cut, "Group name", "eng");
        cut.Find("form.lt-access-form").Submit();
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.UpsertGroupAsync)), Is.True));

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
        Assert.That(Navigation.Uri, Does.Not.Contain("groups/eng"));
    }

    [Test]
    public void A_second_submit_while_a_create_is_in_flight_writes_once()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantGroupDescriptor>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage());
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs((TenantGroupDescriptor?)null);
        directory.UpsertGroupAsync(default!, default!, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = OpenCreate();
        AccessForms.Type(cut, "Group name", "eng");
        var form = cut.Find("form.lt-access-form");

        form.Submit();
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.UpsertGroupAsync)), Is.True));
        cut.Find("form.lt-access-form").Submit();
        pending.SetResult(Group("eng"));

        cut.WaitUntil(() => Assert.That(ToastMessages, Does.Contain("Group eng created.")));
        directory.ReceivedWithAnyArgs(1).UpsertGroupAsync(default!, default!, default);
    }

    [Test]
    public void Cancel_closes_the_create_dialog_without_a_write()
    {
        TenantFacades.AsTenantAdmin();
        var cut = OpenCreate();
        AccessForms.Type(cut, "Group name", "eng");

        AccessForms.Button(cut, "Cancel").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-access-form"), Is.Empty));
        Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.UpsertGroupAsync)));
    }

    [Test]
    public void Closing_the_delete_dialog_deletes_nothing()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(Names(cut), Is.EqualTo(new[] { "ops" })));
        cut.Find("button[aria-label='Delete ops']").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-confirm"), Is.Not.Empty));

        AccessForms.Button(cut, "Cancel").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-confirm"), Is.Empty));
        Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.RemoveGroupAsync)));
        Assert.That(Names(cut), Is.EqualTo(new[] { "ops" }));
    }

    private static TenantGroupDescriptor Group(string name) => new() { Name = name };

    private ILatticeTenantDirectoryAdmin PagedDirectory(TenantGroupPage? next)
    {
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == null), Arg.Any<CancellationToken>())
            .Returns(new TenantGroupPage { Entries = [Group("alpha")], NextPageToken = "p2" });
        directory.ListGroupsAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(next!));
        directory.ListGroupMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<IReadOnlyList<TenantGroupMember>>([]));
        UseDirectory(directory);
        return directory;
    }

    private ILatticeTenantDirectoryAdmin CreatingDirectory(Exception failure)
    {
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage());
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs((TenantGroupDescriptor?)null);
        directory.UpsertGroupAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromException<TenantGroupDescriptor>(failure));
        UseDirectory(directory);
        return directory;
    }

    private IRenderedComponent<AccessGroupsPage> OpenCreate()
    {
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.False));
        AccessForms.Button(cut, "New group").Click();
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Group name"), Is.True));
        return cut;
    }

    private static string Count(IRenderedComponent<AccessGroupsPage> cut) =>
        cut.Find("[data-lt-tenant-view=groups] .lt-access-count[role=status]").TextContent.Trim();

    private static List<string> Names(IRenderedComponent<AccessGroupsPage> cut) =>
        [.. cut.FindAll("[data-lt-tenant-view=groups] tbody tr").Select(row => row.QuerySelector("th, td")!.TextContent.Trim())];

    private static List<string[]> Figures(IRenderedComponent<AccessGroupsPage> cut) =>
        [.. cut.FindAll("[data-lt-tenant-view=groups] tbody tr").Select(row =>
            row.QuerySelectorAll("th, td").Skip(2).Take(3).Select(cell => cell.TextContent.Trim()).ToArray())];
}
