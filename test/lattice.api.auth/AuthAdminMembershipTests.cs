using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// End-to-end coverage for the membership-administration half of the control
/// facade, routed through the live membership directory under an administrator
/// caller. Proves group and membership-edge CRUD round-trips through the
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
    public async Task get_of_a_missing_group_returns_null()
    {
        using (AsAdmin())
        {
            Assert.That(await _fixture.Admin.GetGroupAsync("nope"), Is.Null);
        }
    }

    [Test]
    public async Task membership_edges_round_trip_and_resolve_transitively()
    {
        using (AsAdmin())
        {
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "team" });
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "org" });

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
    public async Task list_groups_emits_a_continuation_token_when_more_groups_remain()
    {
        // A page that fills exactly to the requested size has to resume from the
        // last group it returned, which is the only path that evaluates the page
        // key selector; a page-size of one forces it deterministically.
        using (AsAdmin())
        {
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "page-group-a" });
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = "page-group-b" });

            var first = await _fixture.Admin.ListGroupsAsync(new AuthPageRequest { PageSize = 1 });

            Assert.Multiple(() =>
            {
                Assert.That(first.Entries, Has.Count.EqualTo(1));
                Assert.That(first.NextPageToken, Is.Not.Null);
                Assert.That(first.NextPageToken, Is.EqualTo(first.Entries[0].GroupId));
            });

            var second = await _fixture.Admin.ListGroupsAsync(
                new AuthPageRequest { PageSize = 1, PageToken = first.NextPageToken });

            Assert.Multiple(() =>
            {
                Assert.That(second.Entries, Has.Count.EqualTo(1));
                Assert.That(
                    second.Entries[0].GroupId,
                    Is.Not.EqualTo(first.Entries[0].GroupId),
                    "the continuation token must resume strictly after the previous page");
            });
        }
    }

    [Test]
    public void upsert_null_group_throws()
    {
        using (AsAdmin())
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () => await _fixture.Admin.UpsertGroupAsync(null!));
        }
    }
}
