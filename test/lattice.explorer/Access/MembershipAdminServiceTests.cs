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
    public async Task SearchDirectoryAsync_available_returns_principals_and_token()
    {
        var client = new FakeAuthAdminClient
        {
            DirectorySearchResult = new DirectorySearchResult
            {
                Principals = new[]
                {
                    new DirectoryPrincipalDescriptor { Id = "alice", DisplayName = "Alice", Kind = DirectoryPrincipalKind.User },
                },
                ContinuationToken = "next",
                Available = true,
            },
        };
        var service = Create(client);

        var view = await service.SearchDirectoryAsync("al", DirectoryPrincipalKind.User, pageSize: 10, pageToken: "cursor");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Available, Is.True);
            Assert.That(view.Principals, Has.Count.EqualTo(1));
            Assert.That(view.Principals[0].Id, Is.EqualTo("alice"));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
            Assert.That(client.LastDirectorySearchRequest!.Term, Is.EqualTo("al"));
            Assert.That(client.LastDirectorySearchRequest!.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(client.LastDirectorySearchRequest!.PageSize, Is.EqualTo(10));
            Assert.That(client.LastDirectorySearchRequest!.ContinuationToken, Is.EqualTo("cursor"));
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_empty_token_is_normalized_to_null()
    {
        var client = new FakeAuthAdminClient { DirectorySearchResult = new DirectorySearchResult { Available = true } };
        var service = Create(client);

        await service.SearchDirectoryAsync("al", pageToken: string.Empty);

        Assert.That(client.LastDirectorySearchRequest!.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task SearchDirectoryAsync_no_directory_folds_into_unavailable_view()
    {
        var client = new FakeAuthAdminClient { DirectorySearchResult = DirectorySearchResult.Unavailable };
        var service = Create(client);

        var view = await service.SearchDirectoryAsync("al");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True, "a missing directory is a clean state, not a failure");
            Assert.That(view.Available, Is.False);
            Assert.That(view.Principals, Is.Empty);
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_denied_folds_into_denied_view_without_throwing()
    {
        var client = new FakeAuthAdminClient { DirectoryThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.SearchDirectoryAsync("al");

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
            Assert.That(view.Available, Is.False);
            Assert.That(view.Principals, Is.Empty);
            Assert.That(view.Message, Is.Not.Empty);
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_transport_failure_folds_into_failed_view_without_throwing()
    {
        var client = new FakeAuthAdminClient
        {
            DirectoryThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var service = Create(client);

        var view = await service.SearchDirectoryAsync("al");

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
            Assert.That(view.Available, Is.False);
            Assert.That(view.Principals, Is.Empty);
        });
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_success_returns_descriptor()
    {
        var client = new FakeAuthAdminClient
        {
            DirectoryPrincipalResult = new DirectoryPrincipalDescriptor { Id = "g-1", DisplayName = "Group One", Kind = DirectoryPrincipalKind.Group },
        };
        var service = Create(client);

        var principal = await service.ResolveDirectoryPrincipalAsync("g-1");

        Assert.Multiple(() =>
        {
            Assert.That(principal, Is.Not.Null);
            Assert.That(principal!.Id, Is.EqualTo("g-1"));
            Assert.That(client.LastResolvedPrincipalId, Is.EqualTo("g-1"));
        });
    }

    [Test]
    public void ResolveDirectoryPrincipalAsync_empty_id_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.ResolveDirectoryPrincipalAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_denied_folds_into_null_without_throwing()
    {
        var client = new FakeAuthAdminClient { DirectoryThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var principal = await service.ResolveDirectoryPrincipalAsync("g-1");

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_transport_failure_folds_into_null_without_throwing()
    {
        var client = new FakeAuthAdminClient
        {
            DirectoryThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var service = Create(client);

        var principal = await service.ResolveDirectoryPrincipalAsync("g-1");

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task GetAccessModelAsync_success_maps_descriptor()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Claims,
                RulesEnforced = true,
                DirectoryAvailable = true,
                DirectoryProviderId = "entra",
                DirectoryExplanation = "Use the object id.",
            },
        };
        var service = Create(client);

        var view = await service.GetAccessModelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Claims));
            Assert.That(view.RulesEnforced, Is.True);
            Assert.That(view.DirectoryAvailable, Is.True);
            Assert.That(view.DirectoryProviderId, Is.EqualTo("entra"));
            Assert.That(view.DirectoryExplanation, Is.EqualTo("Use the object id."));
        });
    }

    [Test]
    public async Task GetAccessModelAsync_denied_folds_into_safe_snapshot_without_throwing()
    {
        var client = new FakeAuthAdminClient { AccessModelThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.GetAccessModelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
            Assert.That(view.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Unknown));
            Assert.That(view.DirectoryAvailable, Is.False);
            Assert.That(view.Message, Is.Not.Empty);
        });
    }

    [Test]
    public async Task GetAccessModelAsync_transport_failure_folds_into_safe_snapshot_without_throwing()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var service = Create(client);

        var view = await service.GetAccessModelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
            Assert.That(view.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Unknown));
            Assert.That(view.DirectoryAvailable, Is.False);
        });
    }
}
