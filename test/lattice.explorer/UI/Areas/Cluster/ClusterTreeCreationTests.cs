using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// G4 (issue #5368): the one service every create-tree path drives - who may
/// create trees at all (failing closed), the name and size rules the form checks,
/// the review's pre-flight for one name, and the create with each of its
/// refusals told in one sentence.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTreeCreationTests : ClusterTestContext
{
    private static readonly ClusterTreeCreationRequest Request = new("factory-orders", 4, 256, 64);

    private ClusterTreeCreation Creation => Services.GetRequiredService<ClusterTreeCreation>();

    [TestCase("factory-orders")]
    [TestCase("factory/orders")]
    [TestCase("  factory-orders  ")]
    [TestCase("a/b/c/d/e/f")]
    public void A_plain_name_is_accepted(string name)
    {
        Assert.That(ClusterTreeCreation.ValidateTreeId(name), Is.Null);
    }

    [TestCase(null, "Name the tree.")]
    [TestCase("   ", "Name the tree.")]
    [TestCase("_lattice_trees", "Tree id '_lattice_trees' is reserved: the '_lattice_' namespace is managed by the library.")]
    [TestCase("t/acme/orders", "Tree id 't/acme/orders' is reserved: the 't/' namespace is the structural tenant namespace.")]
    [TestCase("factory//orders", "Each part of a tree name, between slashes, needs at least one character.")]
    [TestCase("/orders", "Each part of a tree name, between slashes, needs at least one character.")]
    [TestCase("a/b/c/d/e/f/g/h", "The Explorer cannot address a tree by that name.")]
    public void A_reserved_empty_or_unaddressable_name_is_refused_with_the_reason(string? name, string reason)
    {
        Assert.That(ClusterTreeCreation.ValidateTreeId(name), Does.StartWith(reason));
    }

    [TestCase("4", 1, 4096, true, 4)]
    [TestCase(" 4096 ", 1, 4096, true, 4096)]
    [TestCase("0", 1, 4096, false, 0)]
    [TestCase("4097", 1, 4096, false, 4097)]
    [TestCase("-3", 1, 4096, false, 0)]
    [TestCase("2.5", 2, int.MaxValue, false, 0)]
    [TestCase("", 2, int.MaxValue, false, 0)]
    [TestCase(null, 2, int.MaxValue, false, 0)]
    public void A_size_is_a_whole_number_within_its_bounds(string? text, int minimum, int maximum, bool accepted, int expected)
    {
        var parsed = ClusterTreeCreation.TryParseSize(text, minimum, maximum, out var value);

        Assert.That(parsed, Is.EqualTo(accepted));
        if (accepted)
        {
            Assert.That(value, Is.EqualTo(expected));
        }
    }

    [Test]
    public async Task A_caller_administering_every_tree_may_create_trees()
    {
        Assert.That(await Creation.CanCreateAsync(), Is.True);
        await Admin.Received(1).ProbeCapabilitiesAsync(ClusterTreeCreation.ProbeTreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_caller_administering_only_a_listed_tree_may_create_trees()
    {
        UseTrees(Tree("lines"), Tree("orders"));
        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Capabilities(call.Arg<string>(), admin: call.Arg<string>() == "orders"));

        Assert.That(await Creation.CanCreateAsync(), Is.True);
    }

    [Test]
    public async Task A_caller_administering_nothing_may_not_create_trees_and_no_more_than_the_bound_of_trees_is_probed()
    {
        UseTrees([.. Enumerable.Range(0, ClusterTreeCreation.MaximumListedProbes + 4).Select(i => Tree("tree-" + i))]);
        Granted = Grants.Read;

        Assert.That(await Creation.CanCreateAsync(), Is.False);
        Assert.That(
            Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.ProbeCapabilitiesAsync)),
            Is.EqualTo(1 + ClusterTreeCreation.MaximumListedProbes));
    }

    [Test]
    public async Task A_refused_or_failing_probe_fails_closed()
    {
        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no"));
        Assert.That(await Creation.CanCreateAsync(), Is.False);
    }

    [Test]
    public async Task A_deleted_listed_tree_is_never_asked_about()
    {
        UseTrees(Tree("orders") with { Lifecycle = TreeLifecycleState.SoftDeleted });
        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Capabilities(call.Arg<string>(), admin: call.Arg<string>() == "orders"));

        Assert.That(await Creation.CanCreateAsync(), Is.False);
        await Admin.DidNotReceive().ProbeCapabilitiesAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_answer_is_remembered_while_fresh_and_asked_again_after()
    {
        Assert.That(await Creation.CanCreateAsync(), Is.True);
        Granted = Grants.None;
        Assert.That(await Creation.CanCreateAsync(), Is.True, "remembered while fresh");

        Time.Advance(ClusterTreeCreation.Freshness);

        Assert.That(await Creation.CanCreateAsync(), Is.False, "asked again once stale");
    }

    [Test]
    public async Task The_review_passes_a_free_name_the_caller_administers()
    {
        Admin.CheckTreeExistsAsync("factory-orders", Arg.Any<CancellationToken>()).Returns(new TreeExistenceResult { TreeId = "factory-orders" });

        Assert.That(await Creation.CheckAsync(Request, scope: null), Is.Null);
    }

    [Test]
    public async Task The_review_refuses_a_name_the_caller_does_not_administer()
    {
        Granted = Grants.Read;

        Assert.That(
            await Creation.CheckAsync(Request, scope: null),
            Is.EqualTo("You do not hold the administration grant over a tree named factory-orders, so the cluster would refuse to create it."));
    }

    [Test]
    public async Task The_review_refuses_a_name_already_taken()
    {
        Admin.CheckTreeExistsAsync("factory-orders", Arg.Any<CancellationToken>()).Returns(new TreeExistenceResult { TreeId = "factory-orders", Exists = true });

        Assert.That(
            await Creation.CheckAsync(Request, scope: null),
            Is.EqualTo("A tree named factory-orders already exists. Creating it again would change nothing; choose another name."));
    }

    [Test]
    public async Task When_the_cluster_will_not_say_the_review_reads_the_listed_trees()
    {
        UseTrees(Tree("factory-orders"));
        Admin.CheckTreeExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("no read"));

        Assert.That(await Creation.CheckAsync(Request, scope: null), Does.StartWith("A tree named factory-orders already exists."));
        Assert.That(await Creation.CheckAsync(Request with { TreeId = "factory-lines" }, scope: null), Is.Null);
    }

    [Test]
    public async Task Under_a_tenant_at_its_tree_quota_the_review_says_so_in_quota_terms()
    {
        var quota = UseQuota(new TenantQuotaDimensionUsage { Usage = 3, Limit = 3, BurstLimit = 3 });
        Admin.CheckTreeExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new TreeExistenceResult { TreeId = "x" });

        Assert.That(
            await Creation.CheckAsync(Request, scope: "acme"),
            Is.EqualTo("Tenant acme is at its tree quota: 3 of 3 trees. Delete a tree, or ask a platform operator to raise the quota."));
        await quota.Received(1).GetQuotaUsageAsync("acme", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Burst_headroom_or_an_unbounded_or_unreadable_quota_does_not_stop_the_review()
    {
        Admin.CheckTreeExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new TreeExistenceResult { TreeId = "x" });
        var quota = UseQuota(new TenantQuotaDimensionUsage { Usage = 3, Limit = 3, BurstLimit = 4 });
        Assert.That(await Creation.CheckAsync(Request, scope: "acme"), Is.Null, "the burst ceiling has room");

        quota.GetQuotaUsageAsync("acme", Arg.Any<CancellationToken>()).Returns(Report(TenantQuotaDimensionUsage.Unbounded));
        Assert.That(await Creation.CheckAsync(Request, scope: "acme"), Is.Null, "an unbounded tenant");

        quota.GetQuotaUsageAsync("acme", Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("no"));
        Assert.That(await Creation.CheckAsync(Request, scope: "acme"), Is.Null, "the cluster still charges the quota at the create");

        Assert.That(await Creation.CheckAsync(Request, scope: "default"), Is.Null, "the default tenant is unbounded");
        await quota.DidNotReceive().GetQuotaUsageAsync("default", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_created_tree_is_reported_remembered_once_for_its_page_and_forgets_both_tree_lists()
    {
        UseTrees(Tree("lines"));
        Admin.CreateTreeAsync("factory-orders", 4, 256, 64, Arg.Any<CancellationToken>())
            .Returns(new TreeCreationResult { TreeId = "factory-orders", Created = true, ShardCount = 4, MaxLeafKeys = 256, MaxInternalChildren = 64 });
        var probe = await TreeListProbe.PrimeAsync(Services);

        var outcome = await Creation.CreateAsync(Request, scope: null);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.IsCreated, Is.True);
            Assert.That(outcome.Refusal, Is.Null);
            Assert.That(outcome.Result!.ShardCount, Is.EqualTo(4));
            Assert.That(Creation.TryTakeCreated("other", out _), Is.False, "another tree's page is not told");
            Assert.That(Creation.TryTakeCreated("factory-orders", out var created), Is.True);
            Assert.That(created!.MaxLeafKeys, Is.EqualTo(256));
            Assert.That(Creation.TryTakeCreated("factory-orders", out _), Is.False, "taken once");
        });
        await probe.AssertBothForgottenAsync();
    }

    [Test]
    public async Task A_name_the_cluster_already_had_is_refused_and_nothing_is_forgotten()
    {
        UseTrees(Tree("factory-orders"));
        Admin.CreateTreeAsync("factory-orders", 4, 256, 64, Arg.Any<CancellationToken>())
            .Returns(new TreeCreationResult { TreeId = "factory-orders", Created = false, ShardCount = 64 });
        var probe = await TreeListProbe.PrimeAsync(Services);

        var outcome = await Creation.CreateAsync(Request, scope: null);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.IsCreated, Is.False);
            Assert.That(outcome.Result, Is.Null);
            Assert.That(outcome.Refusal, Is.EqualTo("The cluster already has a tree named factory-orders, so nothing was created and its configuration is unchanged."));
            Assert.That(Creation.TryTakeCreated("factory-orders", out _), Is.False);
        });
        await probe.AssertNeitherForgottenAsync();
    }

    [Test]
    public async Task A_tree_count_quota_refusal_is_told_in_quota_terms()
    {
        Admin.CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeQuotaExceededException("Tenant 'acme' exceeded its trees quota.", "t/acme/factory-orders", "trees", 4, 3, "acme"));

        var outcome = await Creation.CreateAsync(Request, scope: "acme");

        Assert.That(outcome.Refusal, Is.EqualTo("Tenant acme is at its tree quota: 3 of 3 trees. Delete a tree, or ask a platform operator to raise the quota."));
    }

    [Test]
    public async Task Another_quota_dimension_a_denial_and_a_fault_each_say_what_happened()
    {
        Admin.CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeQuotaExceededException("Tenant 'acme' is over its request rate.", "x", "ops-per-second", 0, 0, "acme"));
        Assert.That((await Creation.CreateAsync(Request, scope: "acme")).Refusal, Is.EqualTo("Tenant 'acme' is over its request rate."));

        Admin.CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no"));
        Assert.That((await Creation.CreateAsync(Request, scope: null)).Refusal, Is.EqualTo("You do not have permission to create a tree named factory-orders."));

        Admin.CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new ArgumentException("Tree id 'view-x' is refused by the cluster."));
        Assert.That((await Creation.CreateAsync(Request, scope: null)).Refusal, Is.EqualTo("Tree id 'view-x' is refused by the cluster."));
    }

    [Test]
    public void The_quota_sentence_names_the_tenant_when_it_can_and_never_more_than_the_ceiling()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterTreeCreation.QuotaRefusal(null, null, null), Is.EqualTo("This tenant is at its tree quota. Delete a tree, or ask a platform operator to raise the quota."));
            Assert.That(ClusterTreeCreation.QuotaRefusal("globex", 7, 1), Does.StartWith("Tenant globex is at its tree quota: 1 of 1 tree."));
        });
    }

    [Test]
    public void The_tenant_a_tree_lands_in_is_the_scope_else_the_asserted_tenant_and_never_the_default()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Creation.TenantFor("acme"), Is.EqualTo("acme"));
            Assert.That(Creation.TenantFor("default"), Is.Null);
            Assert.That(Creation.TenantFor(null), Is.Null);
        });
    }

    [Test]
    public void The_outcome_factories_carry_one_side_each()
    {
        var created = ClusterTreeCreationOutcome.Created(new TreeCreationResult { TreeId = "x", Created = true });
        var refused = ClusterTreeCreationOutcome.Refused("no");

        Assert.Multiple(() =>
        {
            Assert.That(created.IsCreated, Is.True);
            Assert.That(created.Refusal, Is.Null);
            Assert.That(refused.IsCreated, Is.False);
            Assert.That(refused.Result, Is.Null);
            Assert.That(refused.Refusal, Is.EqualTo("no"));
        });
    }

    private ILatticeTenantQuotaUsage UseQuota(TenantQuotaDimensionUsage trees)
    {
        var quota = Substitute.For<ILatticeTenantQuotaUsage>();
        quota.GetQuotaUsageAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(trees));
        Services.AddKeyedSingleton(ShellFacades.Key, quota);
        return quota;
    }

    private static TenantQuotaUsageReport Report(TenantQuotaDimensionUsage trees) => new()
    {
        TenantId = "acme",
        HasUsage = true,
        TreeCount = trees,
    };

    private static LatticeTreeAdminCapabilities Capabilities(string treeId, bool admin) => new()
    {
        TreeId = treeId,
        Schema = new Orleans.Lattice.Api.Schema.LatticeSchemaCapabilities { TreeId = treeId },
        CanAdministerTree = admin,
        CanViewDiagnostics = true,
    };
}
