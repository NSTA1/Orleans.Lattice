using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppSubscriptionRouteTests
{
    private static AppSubscriptionRoute Route(string? keyPrefix, long revision = 1)
    {
        var manifest = Manifest(Subscription("feed", "docs", keyPrefix: keyPrefix));
        var context = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling()).Subscriptions.Single();
        return new AppSubscriptionRoute(context, new RecordingChangeFeedHandler(), revision);
    }

    [Test]
    public void Route_exposes_its_context_handler_and_revision()
    {
        var route = Route(null, revision: 7);

        Assert.That(route.Context.Name, Is.EqualTo("feed"));
        Assert.That(route.Handler, Is.InstanceOf<RecordingChangeFeedHandler>());
        Assert.That(route.Revision, Is.EqualTo(7));
    }

    [Test]
    public void Whole_tree_route_matches_every_mutation()
    {
        var route = Route(null);

        Assert.That(route.Matches(Set("a/notes/docs", "anything")), Is.True);
        Assert.That(route.Matches(new LatticeMutation { TreeId = "a/notes/docs", Kind = MutationKind.DeleteRange, Key = "x", EndExclusiveKey = "y" }), Is.True);
    }

    [TestCase("log/1", true)]
    [TestCase("log/", true)]
    [TestCase("lo", false)]
    [TestCase("other", false)]
    public void Prefix_route_matches_single_key_mutations_by_ordinal_prefix(string key, bool expected)
    {
        Assert.That(Route("log/").Matches(Set("a/notes/docs", key)), Is.EqualTo(expected));
    }

    [Test]
    public void Prefix_route_does_not_match_a_single_key_mutation_without_a_key()
    {
        Assert.That(Route("log/").Matches(new LatticeMutation { TreeId = "a/notes/docs", Kind = MutationKind.Delete }), Is.False);
    }

    [Test]
    public void Prefix_route_matches_a_predicate_range_delete_by_its_matched_keys()
    {
        var route = Route("log/");
        LatticeMutation Range(params string[] matched) =>
            new() { TreeId = "a/notes/docs", Kind = MutationKind.DeleteRange, Key = "a", EndExclusiveKey = "z", MatchedKeys = matched };

        Assert.That(route.Matches(Range("other", "log/1")), Is.True);
        Assert.That(route.Matches(Range("other")), Is.False);
    }

    [TestCase("a", "z", true)]
    [TestCase("a", "log/", false)]
    [TestCase("a", "log/0", true)]
    [TestCase("log/5", "log/6", true)]
    [TestCase("log0", "z", false)]
    [TestCase("m", "z", false)]
    [TestCase(null, null, true)]
    [TestCase(null, "a", false)]
    [TestCase("log/", null, true)]
    public void RangeIntersectsPrefix_is_exact_for_the_prefix_interval(string? start, string? end, bool expected)
    {
        Assert.That(AppSubscriptionRoute.RangeIntersectsPrefix(start, end, "log/"), Is.EqualTo(expected));
    }

    [Test]
    public void IsStillEnabled_requires_the_same_enabled_revision()
    {
        var route = Route(null, revision: 2);

        Assert.That(route.IsStillEnabled(CompiledAppRegistrySnapshot.Compile([Record(Notes, revision: 2)], 1)), Is.True);
        Assert.That(route.IsStillEnabled(CompiledAppRegistrySnapshot.Compile([Record(Notes, revision: 3)], 1)), Is.False);
        Assert.That(route.IsStillEnabled(CompiledAppRegistrySnapshot.Compile([Record(Notes, AppRegistryLifecycleState.Disabled, revision: 2)], 1)), Is.False);
        Assert.That(route.IsStillEnabled(CompiledAppRegistrySnapshot.Empty), Is.False);
    }

    [Test]
    public void Empty_routing_table_matches_no_snapshot_and_routes_nothing()
    {
        var table = AppSubscriptionRoutingTable.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(table.Snapshot, Is.Null);
            Assert.That(table.Epoch, Is.EqualTo(-1));
            Assert.That(table.TreeCount, Is.Zero);
            Assert.That(table.TryGetRoutes(null, out _), Is.False);
            Assert.That(table.TryGetRoutes("a/notes/docs", out _), Is.False);
            Assert.That(table.TryGetFailure(TenantId.Default, Notes, out _), Is.False);
        });
    }

    [Test]
    public void Routing_table_reports_routes_failures_and_its_snapshot_epoch()
    {
        var snapshot = CompiledAppRegistrySnapshot.Compile([Record(Notes)], 4);
        var route = Route(null);
        var table = new AppSubscriptionRoutingTable(
            snapshot,
            new(StringComparer.Ordinal) { ["a/notes/docs"] = [route] },
            new() { [(TenantId.Default, Billing)] = ["nope"] });

        Assert.That(table.Epoch, Is.EqualTo(4));
        Assert.That(table.TreeCount, Is.EqualTo(1));
        Assert.That(table.TryGetRoutes("a/notes/docs", out var routes), Is.True);
        Assert.That(routes, Is.EqualTo(new[] { route }));
        Assert.That(table.TryGetFailure(TenantId.Default, Billing, out var reasons), Is.True);
        Assert.That(reasons, Is.EqualTo(new[] { "nope" }));
    }
}
