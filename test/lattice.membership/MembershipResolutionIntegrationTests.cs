using System.Diagnostics;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Integration tests for the membership resolution pipeline against a live
/// single-silo <see cref="Orleans.TestingHost.TestCluster"/>. Covers the issue's
/// acceptance criteria: transitively-expanded subject resolution, anonymity
/// without a credential, introspectability through the standard scan surface,
/// change-feed cache invalidation without restart, durable history on by
/// default, and nested-group cycle detection.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class MembershipResolutionIntegrationTests
{
    private MembershipClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new MembershipClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task ResolveCurrentAsync_without_a_credential_returns_anonymous()
    {
        var subject = await _fixture.Context.ResolveCurrentAsync();

        Assert.That(subject.IsAnonymous, Is.True);
    }

    [Test]
    public async Task ResolveCurrentAsync_returns_the_subject_with_transitively_expanded_groups()
    {
        var directory = _fixture.Directory;
        await directory.UpsertUserAsync(new MembershipUser("res-alice", "Alice"));
        await directory.UpsertGroupAsync(new MembershipGroup("res-team"));
        await directory.UpsertGroupAsync(new MembershipGroup("res-org"));
        await directory.AddMemberAsync("res-team", "res-alice", MembershipMemberKind.User);
        // Nested: the team is itself a member of the org.
        await directory.AddMemberAsync("res-org", "res-team", MembershipMemberKind.Group);

        var token = MembershipClusterFixture.MintToken("res-alice");
        LatticeSubject subject;
        using (LatticeCredentialContext.Use(token, scheme: "Bearer"))
        {
            subject = await _fixture.Context.ResolveCurrentAsync();
        }

        Assert.That(subject.SubjectId, Is.EqualTo("res-alice"));
        Assert.That(subject.GroupIds, Is.SupersetOf(new[] { "res-team", "res-org" }),
            "the resolved subject must carry the full transitive group closure");
    }

    [Test]
    public async Task Membership_state_is_readable_through_the_standard_scan_surface()
    {
        var directory = _fixture.Directory;
        await directory.UpsertUserAsync(new MembershipUser("scan-user", "Scannable"));

        var usersTree = _fixture.Cluster.GrainFactory.GetGrain<ILattice>(MembershipConstants.UsersTree);
        var seen = new List<string>();
        await foreach (var entry in usersTree.EntriesAsync<MembershipUser>(cancellationToken: default))
        {
            if (entry.Value is { } user)
            {
                seen.Add(user.UserId);
            }
        }

        Assert.That(seen, Does.Contain("scan-user"),
            "membership records must be introspectable through the ordinary ILattice scan surface");
    }

    [Test]
    public async Task Membership_mutation_is_reflected_after_change_feed_invalidation_without_restart()
    {
        var directory = _fixture.Directory;
        await directory.UpsertUserAsync(new MembershipUser("inv-user"));
        await directory.UpsertGroupAsync(new MembershipGroup("inv-initial"));
        await directory.AddMemberAsync("inv-initial", "inv-user");

        var token = MembershipClusterFixture.MintToken("inv-user");

        using (LatticeCredentialContext.Use(token, scheme: "Bearer"))
        {
            // Warm the cache.
            var warm = await _fixture.Context.ResolveCurrentAsync();
            Assert.That(warm.GroupIds, Does.Contain("inv-initial"));

            // Mutate membership: the change-feed observer must flush the cache.
            await directory.UpsertGroupAsync(new MembershipGroup("inv-added"));
            await directory.AddMemberAsync("inv-added", "inv-user");

            var reflected = await PollUntilAsync(async () =>
            {
                var subject = await _fixture.Context.ResolveCurrentAsync();
                return subject.GroupIds.Contains("inv-added");
            });

            Assert.That(reflected, Is.True,
                "a membership change must be reflected without a process restart");
        }
    }

    [Test]
    public async Task Group_changes_produce_durable_history_with_no_extra_configuration()
    {
        var directory = _fixture.Directory;
        await directory.UpsertGroupAsync(new MembershipGroup("hist-group", "v1"));
        await directory.UpsertGroupAsync(new MembershipGroup("hist-group", "v2"));

        var groupsTree = _fixture.Cluster.GrainFactory.GetGrain<ILattice>(MembershipConstants.GroupsTree);

        var retention = await groupsTree.GetHistoryRetentionAsync();
        Assert.That(retention.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly),
            "membership trees must have durable history retention enabled by default");

        var page = await PollAsync(async () =>
        {
            var history = await groupsTree.ScanEntryHistoryAsync("hist-group", null, null, 100, null);
            return history.Revisions.Count > 0 ? history : null;
        });

        Assert.That(page, Is.Not.Null);
        Assert.That(page!.Revisions, Is.Not.Empty,
            "successive group upserts must leave a durable revision timeline");
    }

    [Test]
    public async Task Nested_group_cycle_detection_terminates_and_yields_the_closure()
    {
        var directory = _fixture.Directory;
        await directory.UpsertGroupAsync(new MembershipGroup("cycle-a"));
        await directory.UpsertGroupAsync(new MembershipGroup("cycle-b"));
        // A in B, B in A: a cycle that must not loop forever.
        await directory.AddMemberAsync("cycle-b", "cycle-a", MembershipMemberKind.Group);
        await directory.AddMemberAsync("cycle-a", "cycle-b", MembershipMemberKind.Group);

        var groups = await directory.GroupsOfAsync("cycle-a");

        Assert.That(groups, Does.Contain("cycle-b"),
            "the transitive closure must include the reachable group even through a cycle");
    }

    private static async Task<T> PollAsync<T>(Func<Task<T?>> probe, int timeoutMs = 5000)
        where T : class
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            var result = await probe();
            if (result is not null)
            {
                return result;
            }

            await Task.Delay(50);
        }

        return await probe() ?? throw new TimeoutException("Condition not met within the poll timeout.");
    }

    private static async Task<bool> PollUntilAsync(Func<Task<bool>> condition, int timeoutMs = 5000)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(50);
        }

        return await condition();
    }
}
