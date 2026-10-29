using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>Cancellation and incomplete directory evidence must not silently misclassify admin subjects.</summary>
[TestFixture]
public sealed class TenancyAdminEntriesTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Classify_caller_cancels_a_read_propagates_the_same_token_and_stops(bool duringResolution)
    {
        using var cancellation = new CancellationTokenSource();
        var auth = Substitute.For<ILatticeAuthAdmin>();
        auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), cancellation.Token)
            .Returns(call =>
            {
                if (duringResolution)
                {
                    return Task.FromResult(new AuthGroupPage { Entries = [] });
                }

                cancellation.Cancel();
                return Task.FromCanceled<AuthGroupPage>(call.Arg<CancellationToken>());
            });
        auth.ResolveDirectoryPrincipalAsync("alice", cancellation.Token)
            .Returns(call =>
            {
                cancellation.Cancel();
                return Task.FromCanceled<DirectoryPrincipalDescriptor?>(call.Arg<CancellationToken>());
            });

        var exception = Assert.ThrowsAsync<TaskCanceledException>(async () =>
            await TenancyAdminEntries.ClassifyAsync(auth, "acme", ["t/acme/owners", "alice", "bob"], cancellation.Token));

        Assert.That(exception!.CancellationToken, Is.EqualTo(cancellation.Token));
        await auth.Received(1).ListGroupsAsync(
            Arg.Is<AuthPageRequest>(request => request.PageSize == AuthPageRequest.MaxPageSize), cancellation.Token);
        await auth.Received(duringResolution ? 1 : 0).ResolveDirectoryPrincipalAsync("alice", cancellation.Token);
        await auth.DidNotReceive().ResolveDirectoryPrincipalAsync("bob", Arg.Any<CancellationToken>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Classify_independent_resolution_cancellation_uses_only_available_group_evidence(
        bool groupsKnown)
    {
        var auth = Substitute.For<ILatticeAuthAdmin>();
        auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), CancellationToken.None)
            .Returns(groupsKnown
                ? Task.FromResult(new AuthGroupPage { Entries = [] })
                : Task.FromException<AuthGroupPage>(new OperationCanceledException("Upstream read cancelled.")));
        auth.ResolveDirectoryPrincipalAsync("alice", CancellationToken.None)
            .Returns(Task.FromException<DirectoryPrincipalDescriptor?>(new OperationCanceledException("Upstream read cancelled.")));

        var result = await TenancyAdminEntries.ClassifyAsync(auth, "acme", ["alice"], CancellationToken.None);

        Assert.That(result.Single().Kind, Is.EqualTo(groupsKnown ? TenancyAdminEntryKind.User : TenancyAdminEntryKind.Unknown));
        await auth.Received(1).ResolveDirectoryPrincipalAsync("alice", CancellationToken.None);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Classify_directory_returns_another_id_does_not_apply_its_group_kind(
        bool groupsKnown)
    {
        var auth = Substitute.For<ILatticeAuthAdmin>();
        auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), CancellationToken.None)
            .Returns(groupsKnown
                ? Task.FromResult(new AuthGroupPage { Entries = [] })
                : Task.FromException<AuthGroupPage>(new TimeoutException()));
        auth.ResolveDirectoryPrincipalAsync("alice", CancellationToken.None)
            .Returns(new DirectoryPrincipalDescriptor { Id = "Alice", DisplayName = "Another principal", Kind = DirectoryPrincipalKind.Group });

        var result = await TenancyAdminEntries.ClassifyAsync(auth, "acme", ["alice"], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Single().SubjectId, Is.EqualTo("alice"));
            Assert.That(result.Single().Kind, Is.EqualTo(groupsKnown ? TenancyAdminEntryKind.User : TenancyAdminEntryKind.Unknown),
                "directory ids are matched ordinally, not by case folding");
        });
    }

    [Test]
    public async Task Classify_mixed_subjects_preserves_order_reads_groups_once_and_skips_known_groups()
    {
        var auth = Substitute.For<ILatticeAuthAdmin>();
        auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), CancellationToken.None)
            .Returns(new AuthGroupPage { Entries = [new AuthGroup { GroupId = "ops" }] });
        auth.ResolveDirectoryPrincipalAsync("alice", CancellationToken.None)
            .Returns(new DirectoryPrincipalDescriptor { Id = "alice", DisplayName = "Alice", Kind = DirectoryPrincipalKind.User });
        auth.ResolveDirectoryPrincipalAsync("directory-group", CancellationToken.None)
            .Returns(new DirectoryPrincipalDescriptor { Id = "directory-group", DisplayName = "Directory group", Kind = DirectoryPrincipalKind.Group });
        string[] subjects = ["t/acme/owners", "ops", "alice", "t/other/owners", "directory-group"];

        var result = await TenancyAdminEntries.ClassifyAsync(auth, "acme", subjects, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Select(entry => entry.SubjectId), Is.EqualTo(subjects));
            Assert.That(result.Select(entry => entry.Kind), Is.EqualTo(new[]
            {
                TenancyAdminEntryKind.TenantGroup, TenancyAdminEntryKind.ClusterGroup,
                TenancyAdminEntryKind.User, TenancyAdminEntryKind.OtherTenantGroup, TenancyAdminEntryKind.ClusterGroup,
            }));
        });
        await auth.Received(1).ListGroupsAsync(Arg.Any<AuthPageRequest>(), CancellationToken.None);
        await auth.DidNotReceive().ResolveDirectoryPrincipalAsync("ops", Arg.Any<CancellationToken>());
        await auth.DidNotReceive().ResolveDirectoryPrincipalAsync("t/acme/owners", Arg.Any<CancellationToken>());
        await auth.DidNotReceive().ResolveDirectoryPrincipalAsync("t/other/owners", Arg.Any<CancellationToken>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Classify_absent_group_page_or_entries_treats_the_listing_as_empty(bool absentPage)
    {
        var auth = Substitute.For<ILatticeAuthAdmin>();
        auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), CancellationToken.None)
            .Returns(Task.FromResult(absentPage ? null! : new AuthGroupPage { Entries = null! }));
        auth.ResolveDirectoryPrincipalAsync("alice", CancellationToken.None)
            .Returns(Task.FromResult<DirectoryPrincipalDescriptor?>(null));

        var result = await TenancyAdminEntries.ClassifyAsync(auth, "acme", ["alice"], CancellationToken.None);

        Assert.That(result.Single(), Is.EqualTo(new TenancyAdminEntry("alice", TenancyAdminEntryKind.User)));
        await auth.Received(1).ResolveDirectoryPrincipalAsync("alice", CancellationToken.None);
    }

    [Test]
    public async Task Classify_null_group_or_id_does_not_hide_a_valid_group_in_the_same_page()
    {
        var auth = Substitute.For<ILatticeAuthAdmin>();
        auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), CancellationToken.None)
            .Returns(new AuthGroupPage
            {
                Entries = [null!, new AuthGroup { GroupId = null! }, new AuthGroup { GroupId = "ops" }],
            });

        var result = await TenancyAdminEntries.ClassifyAsync(auth, "acme", ["ops"], CancellationToken.None);

        Assert.That(result.Single(), Is.EqualTo(new TenancyAdminEntry("ops", TenancyAdminEntryKind.ClusterGroup)));
        await auth.DidNotReceive().ResolveDirectoryPrincipalAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
