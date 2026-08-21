using Grpc.Core;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Coverage-focused tests for <see cref="MembershipAdminService"/> that exercise
/// every action's success, server-denial, and transport-failure branch as well as
/// its argument guards, so the Access UI's degrade-cleanly contract is fully
/// verified. They build on the existing <see cref="FakeAuthAdminClient"/> and, for
/// the two lookups the fake cannot fault, a scripted NSubstitute client.
/// </summary>
[TestFixture]
public class MembershipAdminServiceCoverageTests
{
    private static readonly LatticeAuthorizationDeniedException Denied = new("nope");
    private static RpcException Failed() => new(new Status(StatusCode.Unavailable, "gone"));

    private static MembershipAdminService Create(IAuthAdminClient client) => new(client);

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new MembershipAdminService(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ListGroupsAsync_success_returns_entries()
    {
        var client = new FakeAuthAdminClient
        {
            GroupsResult = new AuthGroupPage
            {
                Entries = new[] { new AuthGroup { GroupId = "admins" } },
                NextPageToken = "next",
            },
        };

        var view = await Create(client).ListGroupsAsync(pageSize: 25, pageToken: "cursor");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Entries, Has.Count.EqualTo(1));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
            Assert.That(client.LastGroupsRequest!.PageSize, Is.EqualTo(25));
            Assert.That(client.LastGroupsRequest!.PageToken, Is.EqualTo("cursor"));
        });
    }

    [Test]
    public async Task ListGroupsAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Denied };

        var view = await Create(client).ListGroupsAsync();

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task ListGroupsAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Failed() };

        var view = await Create(client).ListGroupsAsync();

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task GetGroupAsync_success_returns_group()
    {
        var client = new FakeAuthAdminClient { GroupResult = new AuthGroup { GroupId = "admins" } };

        var group = await Create(client).GetGroupAsync("admins");

        Assert.That(group!.GroupId, Is.EqualTo("admins"));
    }

    [Test]
    public void GetGroupAsync_empty_id_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).GetGroupAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task GetGroupAsync_denied_returns_null()
    {
        var client = Substitute.For<IAuthAdminClient>();
        client.GetGroupAsync("admins", Arg.Any<CancellationToken>()).Returns<Task<AuthGroup?>>(_ => throw Denied);

        var group = await Create(client).GetGroupAsync("admins");

        Assert.That(group, Is.Null);
    }

    [Test]
    public async Task GetGroupAsync_transport_failure_returns_null()
    {
        var client = Substitute.For<IAuthAdminClient>();
        client.GetGroupAsync("admins", Arg.Any<CancellationToken>()).Returns<Task<AuthGroup?>>(_ => throw Failed());

        var group = await Create(client).GetGroupAsync("admins");

        Assert.That(group, Is.Null);
    }

    [Test]
    public async Task UpsertGroupAsync_success_reports_saved()
    {
        var client = new FakeAuthAdminClient();

        var result = await Create(client).UpsertGroupAsync(new AuthGroup { GroupId = "admins" });

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastUpsertedGroup!.GroupId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public void UpsertGroupAsync_null_group_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).UpsertGroupAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task UpsertGroupAsync_denied_returns_denied_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Denied };

        var result = await Create(client).UpsertGroupAsync(new AuthGroup { GroupId = "admins" });

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task UpsertGroupAsync_transport_failure_returns_failed_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Failed() };

        var result = await Create(client).UpsertGroupAsync(new AuthGroup { GroupId = "admins" });

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task DeleteGroupAsync_success_reports_deleted()
    {
        var result = await Create(new FakeAuthAdminClient()).DeleteGroupAsync("admins");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public void DeleteGroupAsync_empty_id_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).DeleteGroupAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task DeleteGroupAsync_denied_returns_denied_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Denied };

        var result = await Create(client).DeleteGroupAsync("admins");

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task AddMemberAsync_user_success_reports_added_user()
    {
        var client = new FakeAuthAdminClient();

        var result = await Create(client).AddMemberAsync("admins", "alice");

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("user"));
            Assert.That(client.LastAddedMemberKind, Is.EqualTo(MembershipMemberKind.User));
        });
    }

    [Test]
    public async Task AddMemberAsync_group_success_reports_added_group()
    {
        var client = new FakeAuthAdminClient();

        var result = await Create(client).AddMemberAsync("admins", "readers", MembershipMemberKind.Group);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("group"));
        });
    }

    [Test]
    public void AddMemberAsync_empty_group_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).AddMemberAsync(string.Empty, "alice"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void AddMemberAsync_empty_member_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).AddMemberAsync("admins", string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task AddMemberAsync_transport_failure_returns_failed_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Failed() };

        var result = await Create(client).AddMemberAsync("admins", "alice");

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task RemoveMemberAsync_success_reports_removed()
    {
        var result = await Create(new FakeAuthAdminClient()).RemoveMemberAsync("admins", "alice");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public void RemoveMemberAsync_empty_member_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).RemoveMemberAsync("admins", string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RemoveMemberAsync_denied_returns_denied_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Denied };

        var result = await Create(client).RemoveMemberAsync("admins", "alice");

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task ListDirectMembersAsync_success_returns_members()
    {
        var client = new FakeAuthAdminClient { MembersResult = new[] { "alice", "bob" } };

        var view = await Create(client).ListDirectMembersAsync("admins");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Entries, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void ListDirectMembersAsync_empty_group_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).ListDirectMembersAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ListDirectMembersAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Denied };

        var view = await Create(client).ListDirectMembersAsync("admins");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task ListDirectMembersAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Failed() };

        var view = await Create(client).ListDirectMembersAsync("admins");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task ListSubjectGroupsAsync_success_returns_groups()
    {
        var client = new FakeAuthAdminClient { SubjectGroupsResult = new[] { "admins" } };

        var view = await Create(client).ListSubjectGroupsAsync("alice");

        Assert.That(view.IsSuccess, Is.True);
    }

    [Test]
    public void ListSubjectGroupsAsync_empty_member_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).ListSubjectGroupsAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ListSubjectGroupsAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Denied };

        var view = await Create(client).ListSubjectGroupsAsync("alice");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task SearchDirectoryAsync_available_returns_principals()
    {
        var client = new FakeAuthAdminClient
        {
            DirectorySearchResult = new DirectorySearchResult
            {
                Available = true,
                Principals = new[] { new DirectoryPrincipalDescriptor { Id = "alice", DisplayName = "Alice" } },
                ContinuationToken = "next",
            },
        };

        var view = await Create(client).SearchDirectoryAsync("al", DirectoryPrincipalKind.User, 10, "cursor");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Available, Is.True);
            Assert.That(view.Principals, Has.Count.EqualTo(1));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
            Assert.That(client.LastDirectorySearchRequest!.Term, Is.EqualTo("al"));
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_unavailable_returns_unavailable_view()
    {
        var client = new FakeAuthAdminClient { DirectorySearchResult = DirectorySearchResult.Unavailable };

        var view = await Create(client).SearchDirectoryAsync("al");

        Assert.That(view.Available, Is.False);
    }

    [Test]
    public async Task SearchDirectoryAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { DirectoryThrows = Denied };

        var view = await Create(client).SearchDirectoryAsync("al");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task SearchDirectoryAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient { DirectoryThrows = Failed() };

        var view = await Create(client).SearchDirectoryAsync("al");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_success_returns_descriptor()
    {
        var client = new FakeAuthAdminClient
        {
            DirectoryPrincipalResult = new DirectoryPrincipalDescriptor { Id = "alice", DisplayName = "Alice" },
        };

        var descriptor = await Create(client).ResolveDirectoryPrincipalAsync("alice");

        Assert.That(descriptor!.Id, Is.EqualTo("alice"));
    }

    [Test]
    public void ResolveDirectoryPrincipalAsync_empty_id_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).ResolveDirectoryPrincipalAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_denied_returns_null()
    {
        var client = new FakeAuthAdminClient { DirectoryThrows = Denied };

        var descriptor = await Create(client).ResolveDirectoryPrincipalAsync("alice");

        Assert.That(descriptor, Is.Null);
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_transport_failure_returns_null()
    {
        var client = new FakeAuthAdminClient { DirectoryThrows = Failed() };

        var descriptor = await Create(client).ResolveDirectoryPrincipalAsync("alice");

        Assert.That(descriptor, Is.Null);
    }

    [Test]
    public async Task GetAccessModelAsync_success_returns_model()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelResult = new AccessModelDescriptor
            {
                DirectoryProviderId = "entra",
                DirectoryExplanation = "ok",
                RulesEnforced = true,
            },
        };

        var view = await Create(client).GetAccessModelAsync();

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Succeeded));
    }

    [Test]
    public async Task GetAccessModelAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { AccessModelThrows = Denied };

        var view = await Create(client).GetAccessModelAsync();

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task GetAccessModelAsync_transport_failure_returns_failed_message()
    {
        var client = new FakeAuthAdminClient { AccessModelThrows = Failed() };

        var view = await Create(client).GetAccessModelAsync();

        Assert.That(view.Message, Is.Not.Empty);
    }
}
