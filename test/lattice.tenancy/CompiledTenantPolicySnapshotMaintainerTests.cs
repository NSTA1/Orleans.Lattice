using Microsoft.Extensions.Logging.Abstractions;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledTenantPolicySnapshotMaintainer"/> over a
/// substituted in-memory registry (no cluster). Covers the warm-up build, the
/// monotonic epoch, change-feed-driven rebuilds filtered to the reserved
/// registry tree, and that the swapped snapshot reflects added and removed admin
/// subjects and grants. Every change-feed rebuild is drained deterministically by
/// awaiting the maintainer's captured background task, so no test polls or sleeps.
/// </summary>
[TestFixture]
public sealed class CompiledTenantPolicySnapshotMaintainerTests
{
    private static CompiledTenantPolicySnapshotMaintainer CreateMaintainer(FakeTenantRegistry registry) =>
        new(registry, NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);

    private static LatticeMutation RegistryMutation() =>
        new() { TreeId = TenantTreeNames.RegistryTree };

    [Test]
    public void Constructor_null_registry_throws()
    {
        Assert.That(
            () => new CompiledTenantPolicySnapshotMaintainer(null!, NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_logger_throws()
    {
        Assert.That(
            () => new CompiledTenantPolicySnapshotMaintainer(new FakeTenantRegistry(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Fresh_maintainer_starts_at_epoch_zero_with_the_empty_snapshot()
    {
        var maintainer = CreateMaintainer(new FakeTenantRegistry());

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(0));
            Assert.That(maintainer.Current, Is.SameAs(CompiledTenantPolicy.Empty));
        });
    }

    [Test]
    public async Task EnsureWarmAsync_builds_the_snapshot_once_and_is_idempotent()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = CreateMaintainer(registry);

        await maintainer.EnsureWarmAsync();
        var epochAfterFirst = maintainer.CurrentEpoch;

        await maintainer.EnsureWarmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(epochAfterFirst, Is.EqualTo(1), "warming a cold maintainer advances the epoch to 1");
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1), "warming an already-warm maintainer does not rebuild");
            Assert.That(maintainer.Current.TenantCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RebuildNowAsync_advances_the_epoch_monotonically_on_each_call()
    {
        var maintainer = CreateMaintainer(new FakeTenantRegistry());

        var first = await maintainer.RebuildNowAsync();
        var second = await maintainer.RebuildNowAsync();
        var third = await maintainer.RebuildNowAsync();

        Assert.That(new[] { first, second, third }, Is.EqualTo(new long[] { 1, 2, 3 }));
    }

    [Test]
    public void IsRegistryMutation_is_true_only_for_the_reserved_registry_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                CompiledTenantPolicySnapshotMaintainer.IsRegistryMutation(new LatticeMutation { TreeId = TenantTreeNames.RegistryTree }),
                Is.True);
            Assert.That(
                CompiledTenantPolicySnapshotMaintainer.IsRegistryMutation(new LatticeMutation { TreeId = "some-app-tree" }),
                Is.False);
        });
    }

    [Test]
    public async Task OnMutationAsync_on_the_registry_tree_rebuilds_the_snapshot()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = CreateMaintainer(registry);
        var epochBefore = maintainer.CurrentEpoch;

        await maintainer.OnMutationAsync(RegistryMutation(), CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.GreaterThan(epochBefore), "a registry-tree mutation triggers a rebuild");
            Assert.That(maintainer.Current.TenantCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task OnMutationAsync_on_a_non_registry_tree_does_not_rebuild()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = CreateMaintainer(registry);
        var epochBefore = maintainer.CurrentEpoch;

        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = "some-app-tree" }, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(epochBefore), "a non-registry mutation must not rebuild");
            Assert.That(maintainer.Current, Is.SameAs(CompiledTenantPolicy.Empty), "the snapshot stays cold");
        });
    }

    [Test]
    public async Task Rebuild_reflects_a_newly_added_admin_subject()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = CreateMaintainer(registry);
        await maintainer.EnsureWarmAsync();

        registry.Records.Clear();
        registry.Records.Add(Record("acme", admins: ["alice", "bob"]));
        await maintainer.RebuildNowAsync();

        Assert.That(maintainer.Current.TryGetTenant("acme", out var tenant), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(tenant!.IsAdmin("bob"), Is.True, "the newly-added admin is visible after refresh");
            Assert.That(maintainer.Current.ResolveAllowedTenants("bob"), Is.EqualTo(new[] { TenantId.Parse("acme") }));
        });
    }

    [Test]
    public async Task Rebuild_reflects_a_removed_admin_subject()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice", "bob"]));
        var maintainer = CreateMaintainer(registry);
        await maintainer.EnsureWarmAsync();

        registry.Records.Clear();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        await maintainer.RebuildNowAsync();

        Assert.That(maintainer.Current.TryGetTenant("acme", out var tenant), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(tenant!.IsAdmin("bob"), Is.False, "the removed admin is gone after refresh");
            Assert.That(maintainer.Current.ResolveAllowedTenants("bob"), Is.Empty);
        });
    }

    [Test]
    public async Task Rebuild_reflects_a_newly_added_grant()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = CreateMaintainer(registry);
        await maintainer.EnsureWarmAsync();

        registry.Records.Clear();
        registry.Records.Add(Record(
            "acme",
            admins: ["alice"],
            grants: [TenantGrant("beta", "orders", TenantGrantOperations.Read)]));
        await maintainer.RebuildNowAsync();

        Assert.That(maintainer.Current.TryGetTenant("acme", out var tenant), Is.True);
        Assert.That(tenant!.TryGetTenantGrants("beta", out var grants), Is.True);
        Assert.That(grants!, Has.Length.EqualTo(1));
    }
}
