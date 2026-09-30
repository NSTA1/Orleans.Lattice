using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Framing;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Suggestions;

/// <summary>
/// The shared suggestion sources: each lists what the cluster already knows,
/// keys anything it remembers on the asserted tenant, and fails closed to an
/// unavailable answer with a note, never a throw.
/// </summary>
[TestFixture]
public sealed class SuggestionSourceTests
{
    [Test]
    public async Task Regions_list_this_region_first_then_its_peers_once_each()
    {
        var status = Status("eu-west", ("orders", "us-east"), ("billing", "us-east"), ("orders", "ap-south"));
        var source = new RegionSuggestionSource(status, null, new ManualTimeProvider());

        var answer = await source.SuggestAsync(string.Empty, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "eu-west", "ap-south", "us-east" }));
            Assert.That(answer.Items[0].Detail, Is.EqualTo(RegionSuggestionSource.LocalDetail));
            Assert.That(answer.Items[1].Detail, Is.EqualTo(RegionSuggestionSource.PeerDetail));
        });
    }

    [Test]
    public async Task Regions_are_read_once_per_tenant_and_freshness_window_and_never_served_across_tenants()
    {
        var status = Status("eu-west");
        var tenant = new FakeActiveTenantProvider("acme");
        var time = new ManualTimeProvider();
        var source = new RegionSuggestionSource(status, new ShellCaller(tenant: new ShellAssertedTenant(tenant)), time);

        await source.SuggestAsync("e", 5, CancellationToken.None);
        await source.SuggestAsync("eu", 5, CancellationToken.None);
        Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(1), "typing matches the remembered list");

        tenant.Set("globex");
        await source.SuggestAsync("eu", 5, CancellationToken.None);
        Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(2), "another tenant reads again");

        time.Advance(CachedSuggestionSource.Freshness);
        await source.SuggestAsync("eu", 5, CancellationToken.None);
        Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(3), "a stale list is read again");

        source.Invalidate();
        await source.SuggestAsync("eu", 5, CancellationToken.None);
        Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(4));
    }

    [Test]
    public async Task Regions_fail_closed_when_the_report_is_not_served_or_throws_and_remember_the_failure()
    {
        var missing = await new RegionSuggestionSource(null, null, null).SuggestAsync("eu", 5, CancellationToken.None);
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationPeerStatusPage>(_ => throw new InvalidOperationException("down"));
        var failing = new RegionSuggestionSource(status, null, new ManualTimeProvider());

        var first = await failing.SuggestAsync("eu", 5, CancellationToken.None);
        await failing.SuggestAsync("eu-", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(missing.IsAvailable, Is.False);
            Assert.That(first.UnavailableReason, Does.Contain("used as typed"));
            Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(1), "a failing facade is not asked again on every key");
        });
    }

    [Test]
    public void A_cancelled_query_is_not_remembered_as_a_failure()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationPeerStatusPage>(call => throw new OperationCanceledException(call.Arg<CancellationToken>()));
        var source = new RegionSuggestionSource(status, null, new ManualTimeProvider());
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(async () => await source.SuggestAsync("eu", 5, cancelled.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Directory_matches_put_an_exact_id_first_and_show_the_display_name_as_detail()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>()).Returns(new DirectorySearchResult
        {
            Available = true,
            Principals = [Principal("alice2", "Alice Two"), Principal("alice", "Alice")],
        });
        var source = new DirectorySuggestionSource(admin, DirectoryPrincipalKind.User);

        var answer = await source.SuggestAsync("alice", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "alice", "alice2" }));
            Assert.That(answer.Items[0].Detail, Is.EqualTo("Alice"));
            Assert.That(source.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
        });
        await admin.Received(1).SearchDirectoryAsync(Arg.Is<DirectorySearchRequest>(request => request.Term == "alice" && request.PageSize == 5 && request.Kind == DirectoryPrincipalKind.User), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_truncated_directory_page_resolves_the_exact_id_so_pick_existing_can_accept_it()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>()).Returns(new DirectorySearchResult
        {
            Available = true,
            Principals = [Principal("ann-1", "Ann 1"), Principal("ann-2", "Ann 2")],
            ContinuationToken = "more",
        });
        admin.ResolveDirectoryPrincipalAsync("ann", Arg.Any<CancellationToken>()).Returns(Principal("ann", "Ann"));
        var source = new DirectorySuggestionSource(admin, DirectoryPrincipalKind.User);

        var answer = await source.SuggestAsync("ann", 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "ann", "ann-1" }));
            Assert.That(answer.Truncated, Is.True);
        });
    }

    [Test]
    public async Task Without_a_directory_the_answer_is_unavailable_unless_stored_groups_may_stand_in()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>()).Returns(DirectorySearchResult.Unavailable);
        admin.ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new AuthGroupPage { Entries = [new AuthGroup { GroupId = "ops", DisplayName = "Operations" }, new AuthGroup { GroupId = "devs" }] });

        var users = await new DirectorySuggestionSource(admin, DirectoryPrincipalKind.User).SuggestAsync("o", 5, CancellationToken.None);
        var groups = await new DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group, listStoredGroups: true).SuggestAsync("o", 5, CancellationToken.None);
        var noFacade = await new DirectorySuggestionSource(null, null).SuggestAsync("o", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(users.UnavailableReason, Is.EqualTo(DirectorySuggestionSource.UnavailableReason));
            Assert.That(groups.Items.Select(item => item.Value), Is.EqualTo(new[] { "ops" }));
            Assert.That(groups.Items[0].Detail, Is.EqualTo("Operations"));
            Assert.That(noFacade.IsAvailable, Is.False);
        });
    }

    [Test]
    public async Task A_directory_that_throws_is_unavailable_not_a_crash()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>()).Returns(Task.FromException<DirectorySearchResult>(new TimeoutException()));

        var answer = await new DirectorySuggestionSource(admin, DirectoryPrincipalKind.Group).SuggestAsync("x", 5, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(DirectorySuggestionSource.UnavailableReason));
    }

    [Test]
    public async Task The_unavailable_source_always_says_why()
    {
        var answer = await UnavailableSuggestionSource.Instance.SuggestAsync("x", 5, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(UnavailableSuggestionSource.Reason));
    }

    private static ILatticeReplicationStatus Status(string local, params (string Tree, string Peer)[] links)
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationPeerStatusPage(
                local,
                [.. links.Select(link => new ReplicationPeerStatusEntry(link.Tree, link.Peer, ReplicationLinkDirection.Outbound, 0, 0, 0, TimeSpan.Zero, 0, ReplicationLinkHealth.Healthy))],
                null));
        return status;
    }

    private static DirectoryPrincipalDescriptor Principal(string id, string name) =>
        new() { Id = id, DisplayName = name, Kind = DirectoryPrincipalKind.User };
}
