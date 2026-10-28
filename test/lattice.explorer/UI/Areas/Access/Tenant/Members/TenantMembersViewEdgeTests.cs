using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Members;

/// <summary>
/// The tenant member set's edges (<c>/t/acme/access/members</c>): paging with
/// "Load more members", a page that answers nothing, a denied read and its retry,
/// every refusal an add or a removal can meet and the notices a success raises,
/// the administrators' empty and pending states, and reads that answer after the
/// view moved to another tenant or was left.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantMembersViewEdgeTests : TenantAccessPagesTestContext
{
    private const string Unreachable = "The cluster could not be reached. Check the connection and try again.";

    [Test]
    public void Load_more_appends_the_next_page()
    {
        TenantFacades.AsTenantAdmin();
        var directory = PagedDirectory(new TenantMemberPage { Entries = [User("bob")] });

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" }));
            Assert.That(Count(cut), Is.EqualTo("1 member, more to load"));
        });

        AccessForms.Button(cut, "Load more members").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Is.EqualTo(new[] { "alice", "bob" }));
            Assert.That(Count(cut), Is.EqualTo("2 members"));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Load more members"));
        });
        directory.Received(1).ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_next_page_that_answers_nothing_ends_the_paging()
    {
        TenantFacades.AsTenantAdmin();
        PagedDirectory(next: null);

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(Count(cut), Is.EqualTo("1 member, more to load")));

        AccessForms.Button(cut, "Load more members").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" }));
            Assert.That(Count(cut), Is.EqualTo("1 member"));
        });
    }

    [Test]
    public void A_failed_next_page_is_told_in_a_notice_and_keeps_the_rows()
    {
        TenantFacades.AsTenantAdmin();
        var directory = PagedDirectory(new TenantMemberPage());
        directory.ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TenantMemberPage>(new InvalidOperationException("down")));

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" })));

        AccessForms.Button(cut, "Load more members").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain(Unreachable));
            Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" }));
            Assert.That(AccessForms.Button(cut, "Load more members"), Is.Not.Null, "the page can still be asked for again");
        });
    }

    [Test]
    public void A_read_that_answers_nothing_is_an_empty_member_set()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromResult<TenantMemberPage>(null!));
        UseDirectory(directory);

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=members]").TextContent, Does.Contain("No members yet"));
            Assert.That(Count(cut), Is.EqualTo("0 members"));
        });
    }

    [Test]
    public void A_denied_read_is_not_permitted_and_try_again_reads_again()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(
            _ => Task.FromException<TenantMemberPage>(new LatticeAuthorizationDeniedException("no")),
            _ => Task.FromResult(new TenantMemberPage { Entries = [User("alice")] }));
        UseDirectory(directory);

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=members] .lt-empty-state, [data-lt-tenant-view=members]").TextContent, Does.Contain("Not permitted"));
            Assert.That(cut.FindAll("[data-lt-tenant-view=members] .lt-access-count[role=status]"), Is.Empty);
        });

        AccessForms.Button(cut, "Try again").Click();

        cut.WaitUntil(() => Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" })));
    }

    [Test]
    public void Adding_no_member_asks_for_one()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));

        AccessForms.Type(cut, "Member", "   ");
        AddForm(cut).Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo("Choose the member.")));
        Assert.That(TenantFacades.DirectoryFake.ListMembersAsync("acme", new TenantAccessPageRequest()).Result.Entries, Is.Empty);
    }

    [Test]
    public void Adding_at_the_cap_adds_nothing()
    {
        TenantFacades.AsTenantAdmin();
        TenantFacades.PolicyFake.MemberSubjects = new TenantQuotaDimensionUsage { Usage = 5, Limit = 5 };
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-cap-reason=member-subjects]"), Has.Count.EqualTo(1)));

        AddForm(cut).Submit();

        Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.Null.Or.Empty);
        Assert.That(TenantFacades.DirectoryFake.ListMembersAsync("acme", new TenantAccessPageRequest()).Result.Entries, Is.Empty);
    }

    [Test]
    public void A_member_over_the_cap_is_refused_with_the_cap()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");

        TenantFacades.Gate.NextFailure = new LatticeQuotaExceededException("cap", string.Empty, "MaxMemberSubjects", 5, 5, "acme");
        AddForm(cut).Submit();

        cut.WaitUntil(() => Assert.That(
            AccessForms.ErrorOf(cut, "Member"),
            Is.EqualTo("Tenant acme is at its cap of members, so alice was not added.")));
        Assert.That(ToastMessages, Is.Empty);
    }

    [Test]
    public void A_nesting_refusal_is_shown_with_its_typed_reason()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");

        TenantFacades.Gate.NextFailure = new TenantAccessConfinementException(
            "acme", TenantAccessConfinementRule.GroupNesting, "nesting", "memberId");
        AddForm(cut).Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo(TenantGroupFormat.NestingMessage)));
    }

    [Test]
    public void A_failed_add_is_shown_under_the_member()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");

        TenantFacades.Gate.NextFailure = new InvalidOperationException("down");
        AddForm(cut).Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo(Unreachable));
            Assert.That(Rows(cut), Is.Empty);
        });
    }

    [Test]
    public void An_added_member_is_announced_and_listed()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));

        AccessForms.Type(cut, "Member", "alice");
        AddForm(cut).Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain("alice is now a member of tenant acme."));
            Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" }));
        });
    }

    [Test]
    public async Task A_failed_removal_is_told_in_a_notice_and_keeps_the_member()
    {
        TenantFacades.AsTenantAdmin();
        await SeedMemberAsync("alice");
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" })));

        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");
        TenantFacades.Gate.NextFailure = new InvalidOperationException("down");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain(Unreachable));
            Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" }));
        });
    }

    [Test]
    public async Task A_removal_is_announced()
    {
        TenantFacades.AsTenantAdmin();
        await SeedMemberAsync("alice");
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(Rows(cut), Is.EqualTo(new[] { "alice" })));

        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastMessages, Does.Contain("alice is no longer a member of tenant acme."));
            Assert.That(Rows(cut), Is.Empty);
        });
    }

    [Test]
    public void No_administrator_entries_are_said_to_be_none()
    {
        TenantFacades.AsTenantAdmin();
        TenantAccessAdmin.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(call =>
            new TenantAdminSubjectReport { TenantId = call.Arg<string>(), Subjects = [] });

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-implicit-members]").GetAttribute("data-lt-implicit-members"), Is.EqualTo("ready"));
            Assert.That(cut.Find("[data-lt-implicit-members]").TextContent, Does.Contain("No administrator entries are listed."));
        });
    }

    [Test]
    public void While_the_administrators_are_read_a_skeleton_is_shown()
    {
        TenantFacades.AsTenantAdmin();
        TenantAccessAdmin.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(new TaskCompletionSource<TenantAdminSubjectReport>().Task);

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-implicit-members]").GetAttribute("data-lt-implicit-members"), Is.EqualTo("loading"));
            Assert.That(cut.Find("[data-lt-implicit-members]").InnerHtml, Does.Contain("Loading administrators"));
        });
    }

    [Test]
    public void An_administrator_group_of_this_tenant_links_to_the_group()
    {
        TenantFacades.AsTenantAdmin();
        TenantAccessAdmin.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(call =>
            new TenantAdminSubjectReport { TenantId = call.Arg<string>(), Subjects = ["t/globex/eng", "t/acme/ops", "bob"] });

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(
                cut.FindAll("[data-lt-admin]").Select(item => item.GetAttribute("data-lt-admin")),
                Is.EqualTo(new[] { "bob", "t/acme/ops", "t/globex/eng" }));
            Assert.That(cut.FindAll("[data-lt-admin] a"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("[data-lt-admin='t/acme/ops'] a").GetAttribute("href"), Does.EndWith("t/acme/access/groups/ops"));
        });
    }

    [Test]
    public void Setting_the_same_tenant_again_reads_nothing_again()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(new TenantMemberPage { Entries = [User("alice")] });
        UseDirectory(directory);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "acme"));

        Assert.That(directory.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.ListMembersAsync)), Is.EqualTo(1));
    }

    [Test]
    public async Task A_member_set_read_for_a_tenant_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var acme = new TaskCompletionSource<TenantMemberPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>()).Returns(acme.Task);
        directory.ListMembersAsync("globex", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantMemberPage { Entries = [User("gina")] });
        UseDirectory(directory);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls(), Is.Not.Empty));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("gina")));
        acme.SetResult(new TenantMemberPage { Entries = [User("alice")] });

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains("alice", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public async Task An_administrators_read_for_a_tenant_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var acme = new TaskCompletionSource<TenantAdminSubjectReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        TenantAccessAdmin.ListAdminSubjectsAsync("acme", Arg.Any<CancellationToken>()).Returns(acme.Task);
        TenantAccessAdmin.ListAdminSubjectsAsync("globex", Arg.Any<CancellationToken>())
            .Returns(new TenantAdminSubjectReport { TenantId = "globex", Subjects = ["gina@example.com"] });
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(TenantAccessAdmin.ReceivedCalls(), Is.Not.Empty));

        cut.Render(parameters => parameters.Add(view => view.Tenant, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("gina@example.com")));
        acme.SetResult(new TenantAdminSubjectReport { TenantId = "acme", Subjects = ["old@example.com"] });

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains("old@example.com", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_member_set_read_that_answers_after_the_view_is_left_is_dropped(bool cancelled)
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMemberPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls(), Is.Not.Empty));
        var token = (CancellationToken)directory.ReceivedCalls().Single().GetArguments()[2]!;

        await DisposeComponentsAsync();
        if (cancelled)
        {
            pending.SetException(new OperationCanceledException(token));
        }
        else
        {
            pending.SetResult(new TenantMemberPage { Entries = [User("alice")] });
        }

        Assert.That(token.IsCancellationRequested, Is.True, "leaving the view cancels its read");
        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(ToastMessages, Is.Empty);
    }

    [Test]
    public async Task An_administrators_read_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantAdminSubjectReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        TenantAccessAdmin.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(pending.Task);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(TenantAccessAdmin.ReceivedCalls(), Is.Not.Empty));
        var token = (CancellationToken)TenantAccessAdmin.ReceivedCalls().Single().GetArguments()[1]!;

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException(token));

        Assert.That(token.IsCancellationRequested, Is.True);
        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(ToastMessages, Is.Empty);
    }

    [Test]
    public void A_second_load_more_while_one_is_in_flight_asks_for_nothing_more()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMemberPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = PagedDirectory(null);
        directory.ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(pending.Task);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        var loadMore = AccessForms.Button(cut, "Load more members");

        loadMore.Click();
        loadMore.Click();
        pending.SetResult(new TenantMemberPage { Entries = [User("bob")] });

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));
        directory.Received(1).ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_next_page_for_a_tenant_the_view_has_left_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMemberPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = PagedDirectory(null);
        directory.ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(pending.Task);
        directory.ListMembersAsync("globex", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantMemberPage { Entries = [User("gina")] });
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        AccessForms.Button(cut, "Load more members").Click();

        cut.Render(parameters => parameters.Add(view => view.Tenant, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("gina")));
        pending.SetResult(new TenantMemberPage { Entries = [User("bob")] });

        Assert.That(await TestPoll.TryUntilAsync(() => cut.Markup.Contains(">bob<", StringComparison.Ordinal), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public async Task A_next_page_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMemberPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = PagedDirectory(null);
        directory.ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(pending.Task);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        AccessForms.Button(cut, "Load more members").Click();

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        await pending.Task.ContinueWith(_ => { }, TaskScheduler.Default);
        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public async Task An_add_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMembershipChangeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(new TenantMemberPage());
        directory.AddMemberAsync(default!, default!, default, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));
        AccessForms.Type(cut, "Member", "alice");
        cut.Find("form.lt-access-form").Submit();
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.AddMemberAsync)), Is.True));

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public async Task A_removal_cancelled_by_leaving_the_view_is_dropped()
    {
        TenantFacades.AsTenantAdmin();
        var pending = new TaskCompletionSource<TenantMembershipChangeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(new TenantMemberPage { Entries = [User("alice")] });
        directory.RemoveMemberAsync(default!, default!, default, default).ReturnsForAnyArgs(pending.Task);
        UseDirectory(directory);
        var cut = Render<TenantMembersView>(parameters => parameters.Add(view => view.Tenant, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();
        cut.WaitUntil(() => Assert.That(directory.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(ILatticeTenantDirectoryAdmin.RemoveMemberAsync)), Is.True));

        await DisposeComponentsAsync();
        pending.SetException(new OperationCanceledException());

        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    private static TenantMemberEntry User(string subject) => new() { SubjectId = subject, Kind = TenantSubjectKind.User };

    private ILatticeTenantDirectoryAdmin PagedDirectory(TenantMemberPage? next)
    {
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == null), Arg.Any<CancellationToken>())
            .Returns(new TenantMemberPage { Entries = [User("alice")], NextPageToken = "p2" });
        directory.ListMembersAsync("acme", Arg.Is<TenantAccessPageRequest>(request => request.PageToken == "p2"), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(next!));
        UseDirectory(directory);
        return directory;
    }

    private async Task SeedMemberAsync(string subject)
    {
        var enabled = TenantFacades.Gate.Enabled;
        TenantFacades.Gate.Enabled = true;
        await TenantFacades.DirectoryFake.AddMemberAsync("acme", subject, TenantSubjectKind.User);
        TenantFacades.Gate.Enabled = enabled;
        TenantFacades.Gate.Calls.Clear();
    }

    private static AngleSharp.Dom.IElement AddForm(IRenderedComponent<AccessMembersPage> cut) => cut.Find("form.lt-access-form");

    private static string Count(IRenderedComponent<AccessMembersPage> cut) =>
        cut.Find("[data-lt-tenant-view=members] .lt-access-count[role=status]").TextContent.Trim();

    private static List<string> Rows(IRenderedComponent<AccessMembersPage> cut) =>
        [.. cut.FindAll("[data-lt-tenant-view=members] tbody tr").Select(row => row.QuerySelector("th, td")!.TextContent.Trim())];
}
