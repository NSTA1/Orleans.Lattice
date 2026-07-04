using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// End-to-end coverage for the membership-administration half of the control
/// facade, routed through the live membership directory under an administrator
/// caller. Proves user, group, and membership-edge CRUD round-trips through the
/// directory and that the list endpoints page deterministically.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthAdminMembershipTests
{
    private AuthAdminClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthAdminClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private static IDisposable AsAdmin() => AuthAdminClusterFixture.AsSubject(AuthAdminClusterFixture.BootstrapAdmin);

    [Test]
    public async Task user_crud_round_trips_through_the_directory()
    {
        using (AsAdmin())
        {
            await _fixture.Admin.UpsertUserAsync(new AuthUser
            {
                UserId = "u-crud",
                DisplayName = "Crud User",
                Claims = new Dictionary<string, string> { ["team"] = "ops" },
            });

            var read = await _fixture.Admin.GetUserAsync("u-crud");
            Assert.Multiple(() =>
            {
                Assert.That(read, Is.Not.Null);
                Assert.That(read!.DisplayName, Is.EqualTo("Crud User"));
                Assert.That(read.Claims!["team"], Is.EqualTo("ops"));
            });

            // Replace, then delete.
            await _fixture.Admin.UpsertUserAsync(new AuthUser { UserId = "u-crud", DisplayName = "Renamed" });
            var renamed = await _fixture.Admin.GetUserAsync("u-crud");
            Assert.That(renamed!.DisplayName, Is.EqualTo("Renamed"));

            await _fixture.Admin.RemoveUserAsync("u-crud");
            Assert.That(await _fixture.Admin.GetUserAsync("u-crud"), Is.Null);
        }
    }

    [Test]
    public async Task group_crud_round_trips_through_the_directory()
    {
        using (AsAdmin())
        {
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "g-crud", DisplayName = "Crud Group" });

            var read = await _fixture.Admin.GetGroupAsync("g-crud");
            Assert.Multiple(() =>
            {
                Assert.That(read, Is.Not.Null);
                Assert.That(read!.DisplayName, Is.EqualTo("Crud Group"));
            });

            await _fixture.Admin.RemoveGroupAsync("g-crud");
            Assert.That(await _fixture.Admin.GetGroupAsync("g-crud"), Is.Null);
        }
    }

    [Test]
    public async Task get_of_a_missing_user_or_group_returns_null()
    {
        using (AsAdmin())
        {
            Assert.Multiple(async () =>
            {
                Assert.That(await _fixture.Admin.GetUserAsync("nope"), Is.Null);
                Assert.That(await _fixture.Admin.GetGroupAsync("nope"), Is.Null);
            });
        }
    }

    [Test]
    public async Task membership_edges_round_trip_and_resolve_transitively()
    {
        using (AsAdmin())
        {
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "team" });
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "org" });
            await _fixture.Admin.UpsertUserAsync(new AuthUser { UserId = "alice" });

            // alice in team, team nested in org.
            await _fixture.Admin.AddMemberAsync("team", "alice");
            await _fixture.Admin.AddMemberAsync("org", "team", MembershipMemberKind.Group);

            var teamMembers = await _fixture.Admin.ListGroupMembersAsync("team");
            var aliceGroups = await _fixture.Admin.ListSubjectGroupsAsync("alice");

            Assert.Multiple(() =>
            {
                Assert.That(teamMembers, Does.Contain("alice"));
                Assert.That(aliceGroups, Does.Contain("team"));
                Assert.That(aliceGroups, Does.Contain("org"), "nested group membership resolves transitively");
            });

            // Removing the direct edge drops alice from both groups.
            await _fixture.Admin.RemoveMemberAsync("team", "alice");
            var afterRemoval = await _fixture.Admin.ListSubjectGroupsAsync("alice");
            Assert.That(afterRemoval, Does.Not.Contain("team"));
        }
    }

    [Test]
    public async Task list_users_pages_deterministically_with_a_continuation_token()
    {
        using (AsAdmin())
        {
            for (var i = 0; i < 5; i++)
            {
                await _fixture.Admin.UpsertUserAsync(new AuthUser { UserId = $"page-user-{i}" });
            }

            var first = await _fixture.Admin.ListUsersAsync(new AuthPageRequest { PageSize = 2 });
            Assert.Multiple(() =>
            {
                Assert.That(first.Entries, Has.Count.EqualTo(2));
                Assert.That(first.NextPageToken, Is.Not.Null);
            });

            // Walk every page and assert the union covers the seeded users with no
            // duplicates and a strictly ascending order.
            var seen = new List<string>(first.Entries.Select(e => e.UserId));
            var token = first.NextPageToken;
            while (token is not null)
            {
                var next = await _fixture.Admin.ListUsersAsync(new AuthPageRequest { PageSize = 2, PageToken = token });
                seen.AddRange(next.Entries.Select(e => e.UserId));
                token = next.NextPageToken;
            }

            var pageUsers = seen.Where(id => id.StartsWith("page-user-", StringComparison.Ordinal)).ToList();
            Assert.Multiple(() =>
            {
                Assert.That(pageUsers, Is.Unique);
                Assert.That(pageUsers, Is.SupersetOf(new[]
                {
                    "page-user-0", "page-user-1", "page-user-2", "page-user-3", "page-user-4",
                }));
                Assert.That(pageUsers, Is.Ordered.Using<string>(StringComparer.Ordinal));
            });
        }
    }

    [Test]
    public async Task list_groups_returns_upserted_groups()
    {
        using (AsAdmin())
        {
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "list-group-a" });
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "list-group-b" });

            var all = new List<string>();
            string? token = null;
            do
            {
                var page = await _fixture.Admin.ListGroupsAsync(new AuthPageRequest { PageSize = 100, PageToken = token });
                all.AddRange(page.Entries.Select(g => g.GroupId));
                token = page.NextPageToken;
            }
            while (token is not null);

            Assert.That(all, Is.SupersetOf(new[] { "list-group-a", "list-group-b" }));
        }
    }

    [Test]
    public void upsert_null_user_or_group_throws()
    {
        using (AsAdmin())
        {
            Assert.Multiple(() =>
            {
                Assert.ThrowsAsync<ArgumentNullException>(async () => await _fixture.Admin.UpsertUserAsync(null!));
                Assert.ThrowsAsync<ArgumentNullException>(async () => await _fixture.Admin.UpsertGroupAsync(null!));
                Assert.ThrowsAsync<ArgumentException>(
                    async () => await _fixture.Admin.UpsertUserAsync(new AuthUser { UserId = "" }));
            });
        }
    }
}
