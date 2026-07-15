using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

[TestFixture]
public class MembershipAdminServiceTests
{
    private static MembershipAdminService Create(FakeAuthAdminClient client) => new(client);

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new MembershipAdminService(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ListUsersAsync_success_returns_entries_and_token()
    {
        var client = new FakeAuthAdminClient
        {
            UsersResult = new AuthUserPage
            {
                Entries = new[] { new AuthUser { UserId = "alice" }, new AuthUser { UserId = "bob" } },
                NextPageToken = "next",
            },
        };
        var service = Create(client);

        var view = await service.ListUsersAsync(pageSize: 50, pageToken: "cursor");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Entries, Has.Count.EqualTo(2));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
            Assert.That(client.LastUsersRequest!.PageSize, Is.EqualTo(50));
            Assert.That(client.LastUsersRequest!.PageToken, Is.EqualTo("cursor"));
        });
    }

    [Test]
    public async Task ListUsersAsync_empty_token_is_normalized_to_null()
    {
        var client = new FakeAuthAdminClient();
        var service = Create(client);

        await service.ListUsersAsync(pageToken: string.Empty);

        Assert.That(client.LastUsersRequest!.PageToken, Is.Null);
    }

    [Test]
    public async Task ListUsersAsync_denied_returns_denied_view_with_message()
    {
        var client = new FakeAuthAdminClient { ListUsersThrows = new LatticeAuthorizationDeniedException("no admin") };
        var service = Create(client);

        var view = await service.ListUsersAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
            Assert.That(view.Entries, Is.Empty);
            Assert.That(view.Message, Is.Not.Empty);
        });
    }

    [Test]
    public async Task ListUsersAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient
        {
            ListUsersThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var service = Create(client);

        var view = await service.ListUsersAsync();

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task UpsertUserAsync_forwards_and_succeeds()
    {
        var client = new FakeAuthAdminClient();
        var service = Create(client);
        var user = new AuthUser { UserId = "carol", DisplayName = "Carol" };

        var result = await service.UpsertUserAsync(user);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastUpsertedUser, Is.SameAs(user));
        });
    }

    [Test]
    public void UpsertUserAsync_null_user_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.UpsertUserAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task UpsertUserAsync_denied_folds_into_denied_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var result = await service.UpsertUserAsync(new AuthUser { UserId = "carol" });

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public void DeleteUserAsync_empty_id_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.DeleteUserAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task AddMemberAsync_forwards_group_kind()
    {
        var client = new FakeAuthAdminClient();
        var service = Create(client);

        var result = await service.AddMemberAsync("admins", "operators", MembershipMemberKind.Group);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastAddedGroupId, Is.EqualTo("admins"));
            Assert.That(client.LastAddedMemberId, Is.EqualTo("operators"));
            Assert.That(client.LastAddedMemberKind, Is.EqualTo(MembershipMemberKind.Group));
        });
    }

    [Test]
    public void AddMemberAsync_empty_group_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.AddMemberAsync(string.Empty, "m"), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void AddMemberAsync_empty_member_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.AddMemberAsync("g", string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ListDirectMembersAsync_success_returns_members()
    {
        var client = new FakeAuthAdminClient { MembersResult = new[] { "alice", "operators" } };
        var service = Create(client);

        var view = await service.ListDirectMembersAsync("admins");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Entries, Is.EqualTo(new[] { "alice", "operators" }));
        });
    }

    [Test]
    public async Task ListSubjectGroupsAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.ListSubjectGroupsAsync("alice");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public void ListSubjectGroupsAsync_empty_member_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.ListSubjectGroupsAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task GetUserAsync_denied_returns_null()
    {
        var client = new FakeAuthAdminClient { UserResult = new AuthUser { UserId = "x" } };
        var service = Create(client);

        var user = await service.GetUserAsync("x");

        Assert.That(user, Is.Not.Null);
    }
}
