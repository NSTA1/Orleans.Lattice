namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Integration coverage for the read and removal halves of
/// <see cref="ILatticeMembershipDirectory"/> against a live single-silo cluster.
/// The existing resolution suite drives the write path
/// (<c>UpsertGroupAsync</c> / <c>AddMemberAsync</c>) and the forward
/// <c>GroupsOfAsync</c> closure; this fixture covers the group read and
/// enumeration surface, the reverse <c>MembersOfAsync</c> prefix scan, and the
/// two removal operations that unpick both edge rows - so a regression in the
/// reverse index or in edge deletion is caught rather than silently passing.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeMembershipDirectoryIntegrationTests
{
    private MembershipClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new MembershipClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeMembershipDirectory Directory => _fixture.Directory;

    [Test]
    public async Task GetGroupAsync_returns_the_stored_group_record()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("get-team", "Get Team"));

        var group = await Directory.GetGroupAsync("get-team");

        Assert.That(group, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(group!.GroupId, Is.EqualTo("get-team"));
            Assert.That(group.DisplayName, Is.EqualTo("Get Team"));
        });
    }

    [Test]
    public async Task GetGroupAsync_returns_null_for_an_unknown_group()
    {
        Assert.That(await Directory.GetGroupAsync("no-such-group-at-all"), Is.Null);
    }

    [Test]
    public async Task ListGroupsAsync_enumerates_the_stored_groups()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("list-alpha"));
        await Directory.UpsertGroupAsync(new MembershipGroup("list-beta"));

        var seen = new List<string>();
        await foreach (var group in Directory.ListGroupsAsync())
        {
            seen.Add(group.GroupId);
        }

        Assert.That(seen, Is.SupersetOf(new[] { "list-alpha", "list-beta" }));
    }

    [Test]
    public async Task RemoveGroupAsync_deletes_the_group_record()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("doomed-group"));
        Assert.That(await Directory.GetGroupAsync("doomed-group"), Is.Not.Null);

        await Directory.RemoveGroupAsync("doomed-group");

        Assert.That(await Directory.GetGroupAsync("doomed-group"), Is.Null);
    }

    [Test]
    public async Task RemoveGroupAsync_is_a_no_op_for_an_unknown_group()
    {
        Assert.That(async () => await Directory.RemoveGroupAsync("never-existed"), Throws.Nothing);
    }

    [Test]
    public async Task MembersOfAsync_returns_the_direct_members_via_the_reverse_index()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("mem-team"));
        await Directory.AddMemberAsync("mem-team", "mem-alice", MembershipMemberKind.User);
        await Directory.AddMemberAsync("mem-team", "mem-bob", MembershipMemberKind.User);
        await Directory.AddMemberAsync("mem-team", "mem-nested", MembershipMemberKind.Group);

        var members = await Directory.MembersOfAsync("mem-team");

        Assert.That(members, Is.EquivalentTo(new[] { "mem-alice", "mem-bob", "mem-nested" }),
            "the reverse edge row must list every direct member regardless of kind");
    }

    [Test]
    public async Task MembersOfAsync_returns_empty_for_a_group_with_no_members()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("empty-team"));

        Assert.That(await Directory.MembersOfAsync("empty-team"), Is.Empty);
    }

    [Test]
    public async Task MembersOfAsync_does_not_leak_members_of_a_prefix_sharing_group()
    {
        // "sib" is a strict prefix of "sib-extended": the scan's upper bound must
        // stop before the longer group's reverse rows.
        await Directory.UpsertGroupAsync(new MembershipGroup("sib"));
        await Directory.UpsertGroupAsync(new MembershipGroup("sib-extended"));
        await Directory.AddMemberAsync("sib", "sib-member");
        await Directory.AddMemberAsync("sib-extended", "extended-member");

        var members = await Directory.MembersOfAsync("sib");

        Assert.That(members, Is.EqualTo(new[] { "sib-member" }),
            "a prefix-sharing sibling group's members must not bleed into the scan");
    }

    [Test]
    public async Task RemoveMemberAsync_unpicks_both_the_forward_and_the_reverse_edge()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("rm-team"));
        await Directory.AddMemberAsync("rm-team", "rm-alice");
        await Directory.AddMemberAsync("rm-team", "rm-bob");

        await Directory.RemoveMemberAsync("rm-team", "rm-alice");

        var members = await Directory.MembersOfAsync("rm-team");
        var groups = await Directory.GroupsOfAsync("rm-alice");

        Assert.Multiple(() =>
        {
            Assert.That(members, Is.EqualTo(new[] { "rm-bob" }),
                "the reverse edge must be gone");
            Assert.That(groups, Does.Not.Contain("rm-team"),
                "the forward edge must be gone too, or the closure would still resolve");
        });
    }

    [Test]
    public async Task RemoveMemberAsync_is_a_no_op_for_an_edge_that_was_never_added()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("noop-team"));

        Assert.That(
            async () => await Directory.RemoveMemberAsync("noop-team", "never-a-member"),
            Throws.Nothing);
    }

    [Test]
    public async Task ExpandGroupsAsync_returns_the_seeds_plus_their_ancestors()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("exp-child"));
        await Directory.UpsertGroupAsync(new MembershipGroup("exp-parent"));
        await Directory.AddMemberAsync("exp-parent", "exp-child", MembershipMemberKind.Group);

        var closure = await Directory.ExpandGroupsAsync(["exp-child"]);

        Assert.That(closure, Is.SupersetOf(new[] { "exp-child", "exp-parent" }),
            "the seed group is itself part of the closure, unlike a member seed");
    }

    [Test]
    public async Task ExpandGroupsAsync_short_circuits_on_an_empty_seed_set()
    {
        Assert.That(await Directory.ExpandGroupsAsync([]), Is.Empty);
    }

    [Test]
    public async Task ExpandGroupsAsync_deduplicates_repeated_seeds()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("dup-seed"));

        var closure = await Directory.ExpandGroupsAsync(["dup-seed", "dup-seed"]);

        Assert.That(closure, Is.EqualTo(new[] { "dup-seed" }));
    }

    [Test]
    public void The_directory_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await Directory.UpsertGroupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.GetGroupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.RemoveGroupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.AddMemberAsync(null!, "m"), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.AddMemberAsync("g", null!), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.RemoveMemberAsync(null!, "m"), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.RemoveMemberAsync("g", null!), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.GroupsOfAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.MembersOfAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await Directory.ExpandGroupsAsync(null!), Throws.ArgumentNullException);
        });
    }
}
