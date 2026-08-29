using Microsoft.Extensions.DependencyInjection;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// End-to-end integration tests for cross-cluster usage-fold convergence over the
/// real dogfooded <c>sys-tenant-usage</c> tree. Two clusters publishing into the
/// same tenant's usage record are simulated by publishing records that each carry a
/// distinct <c>clusterId</c> slot; the store's CRDT merge is expected to
/// converge them so the global <see cref="TenantUsageRecord.Fold"/> sums every
/// cluster's slot, a slower cluster's stale slot never regresses a fresher one, and
/// re-publishing a slot is idempotent. Convergence is asserted by writing
/// hand-stamped records directly and reading back the merged result - never by
/// timing, ordering, or delays.
/// </summary>
/// <remarks>
/// This fixture is written by T8 but is run by the epic coordinator, not in the T8
/// unit loop. It documents the bounded-overshoot contract: because each cluster
/// writes only its own slot and admission reads an eventually-consistent snapshot,
/// concurrent cross-cluster writes can transiently overshoot the global quota until
/// the fold converges - the converged fold below is the steady state that bounds it.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class TenantUsageCrossClusterFoldConvergenceTests
{
    private readonly TenancyClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private ITenantUsageStore Store => _fixture.SiloServices.GetRequiredService<ITenantUsageStore>();

    private static TenantUsageRecord Slot(string tenant, string cluster, LocalUsageSample sample, long clock)
    {
        var record = TenantUsageRecord.Create(TenantId.Parse(tenant));
        record.SetLocalSample(cluster, sample, Clock(clock), cluster);
        return record;
    }

    [Test]
    public async Task Publishing_two_cluster_slots_converges_to_a_summed_global_fold()
    {
        var tenant = TenantId.Parse("acme");

        await Store.PublishAsync(Slot("acme", "east", Sample(100, 1, 10, 1), 10));
        await Store.PublishAsync(Slot("acme", "west", Sample(200, 2, 20, 1), 10));

        var converged = await Store.GetAsync(tenant);

        Assert.That(converged, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(converged!.ClusterCount, Is.EqualTo(2), "both cluster slots survive the merge");
            Assert.That(converged.Fold(), Is.EqualTo(Sample(300, 3, 30, 2)), "the global fold sums both clusters' slots");
        });
    }

    [Test]
    public async Task A_stale_slot_republish_does_not_regress_a_fresher_one()
    {
        var tenant = TenantId.Parse("beta");

        await Store.PublishAsync(Slot("beta", "east", Sample(500, 5, 50, 1), 50));
        await Store.PublishAsync(Slot("beta", "east", Sample(100, 1, 10, 1), 10));

        var converged = await Store.GetAsync(tenant);

        Assert.That(converged, Is.Not.Null);
        Assert.That(converged!.LocalSample("east"), Is.EqualTo(Sample(500, 5, 50, 1)),
            "the older east-slot write must not regress the newer one");
    }

    [Test]
    public async Task Concurrent_cross_cluster_publishes_all_survive()
    {
        var tenant = TenantId.Parse("gamma");

        // Three clusters publish into the same tenant's usage record concurrently.
        // The store's optimistic-concurrency merge forces the losers of the version
        // race to re-read and re-merge, so every slot survives in every interleaving
        // - the outcome is interleaving-independent, never timing-dependent.
        await Task.WhenAll(
            Store.PublishAsync(Slot("gamma", "east", Sample(100, 1, 10, 1), 10)),
            Store.PublishAsync(Slot("gamma", "west", Sample(200, 2, 20, 1), 10)),
            Store.PublishAsync(Slot("gamma", "north", Sample(300, 3, 30, 1), 10)));

        var converged = await Store.GetAsync(tenant);

        Assert.That(converged, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(converged!.ClusterCount, Is.EqualTo(3), "every cluster's slot survives the concurrent merge");
            Assert.That(converged.Fold(), Is.EqualTo(Sample(600, 6, 60, 3)), "the converged fold sums all three slots");
        });
    }

    [Test]
    public async Task Re_publishing_the_same_slot_is_idempotent()
    {
        var tenant = TenantId.Parse("delta");
        var slot = Sample(100, 1, 10, 1);

        await Store.PublishAsync(Slot("delta", "east", slot, 10));
        await Store.PublishAsync(Slot("delta", "east", slot, 10));

        var converged = await Store.GetAsync(tenant);

        Assert.That(converged, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(converged!.ClusterCount, Is.EqualTo(1));
            Assert.That(converged.Fold(), Is.EqualTo(slot), "re-publishing the same slot does not double-count");
        });
    }
}
