using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// End-to-end cross-cluster replication coverage. Asserts that mutations
/// authored on Site A converge on Site B (and vice-versa) via the
/// canonical
/// <see cref="IChangeFeed"/> -> <see cref="IReplicationBatchEncoder"/> -> 
/// <see cref="IReplicationTransport"/> -> <see cref="IReplicationApplier"/>
/// pipeline, and that the cycle-break invariants documented on
/// <see cref="WalRecord.OriginClusterId"/> hold so cluster pairs do not
/// ping-pong each other's writes.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public async Task SetAsync_on_site_a_replicates_to_site_b()
    {
        var treeId = NextTreeId("set-a-to-b");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k1", Bytes("v1"));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k1")) == "v1",
            $"Site B should see Site A's write to '{treeId}/k1'.");

        var observed = await treeOnB.GetAsync("k1");
        Assert.That(Str(observed), Is.EqualTo("v1"));
    }

    [Test]
    public async Task SetAsync_on_site_b_replicates_to_site_a()
    {
        var treeId = NextTreeId("set-b-to-a");
        await CreateReplicatedTreeAsync(treeId);
        var treeOnA = _fixture.TreeOnA(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnB.SetAsync("k1", Bytes("from-b"));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnA.GetAsync("k1")) == "from-b",
            $"Site A should see Site B's write to '{treeId}/k1'.");

        Assert.That(Str(await treeOnA.GetAsync("k1")), Is.EqualTo("from-b"));
    }

    [Test]
    public async Task DeleteAsync_on_site_a_replicates_to_site_b()
    {
        var treeId = NextTreeId("delete-a-to-b");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k1", Bytes("v1"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k1")) == "v1",
            "initial replication");

        await treeOnA.DeleteAsync("k1");

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await treeOnB.GetAsync("k1") is null,
            $"Site B should observe the delete of '{treeId}/k1'.");

        Assert.That(await treeOnB.GetAsync("k1"), Is.Null);
    }

    [Test]
    public async Task DeleteRangeAsync_on_site_a_replicates_to_site_b()
    {
        var treeId = NextTreeId("delete-range-a-to-b");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("a", Bytes("a"));
        await treeOnA.SetAsync("b", Bytes("b"));
        await treeOnA.SetAsync("c", Bytes("c"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("a")) == "a"
                     && Str(await treeOnB.GetAsync("b")) == "b"
                     && Str(await treeOnB.GetAsync("c")) == "c",
            "initial seed of 3 keys");

        var deleted = await treeOnA.DeleteRangeAsync("a", "c"); // deletes a, b - c excluded
        Assert.That(deleted, Is.EqualTo(2));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await treeOnB.GetAsync("a") is null
                     && await treeOnB.GetAsync("b") is null
                     && Str(await treeOnB.GetAsync("c")) == "c",
            $"Site B should observe the [a,c) range delete.");

        Assert.That(await treeOnB.GetAsync("a"), Is.Null);
        Assert.That(await treeOnB.GetAsync("b"), Is.Null);
        Assert.That(Str(await treeOnB.GetAsync("c")), Is.EqualTo("c"));
    }

    [Test]
    public async Task Bidirectional_writes_converge_under_lww_register()
    {
        var treeId = NextTreeId("bidir");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        // Distinct keys per site so neither site's write beats the
        // other under LWW (LWW would collapse same-key concurrent
        // writes to one). The contract claim is that both clusters
        // see each other's authored writes.
        await treeOnA.SetAsync("from-a", Bytes("v-a"));
        await treeOnB.SetAsync("from-b", Bytes("v-b"));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnA.GetAsync("from-a")) == "v-a"
                     && Str(await treeOnA.GetAsync("from-b")) == "v-b"
                     && Str(await treeOnB.GetAsync("from-a")) == "v-a"
                     && Str(await treeOnB.GetAsync("from-b")) == "v-b",
            "Both sites see both authored writes.");
    }

    [Test]
    public async Task Replicated_write_does_not_loop_back_to_origin_cluster()
    {
        var treeId = NextTreeId("cycle-break");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k1", Bytes("v1"));

        // Wait for B to see the write so we know the apply path ran.
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k1")) == "v1",
            "initial A->B replication");

        // Allow the shipper a generous window to pump anything it
        // intended to send. The contract claim is that B's
        // applier-installed entry does not enter B's WAL (the change
        // feed is locally-authored only, and the per-origin HWM check
        // suppresses re-delivery either way), so B never re-ships
        // that entry back to A. Counting the deliveries with origin
        // == site-b after the wait must be zero, even though A keeps
        // shipping its single authored entry on every shipper tick.
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var siteBOriginatedDeliveries = LoopbackDeliveringTransport
            .DeliveredBatches
            .Where(d => d.OriginClusterId == PublicReplicationApiClusterFixture.SiteBClusterId)
            .Where(d => d.TreeName == treeId)
            .ToList();

        Assert.That(siteBOriginatedDeliveries, Is.Empty,
            "Site B must not echo Site A's write back to Site A.");
    }

    /// <summary>
    /// Regression for issue #894: the explorer / state API "Data"
    /// catalog lists trees via
    /// <see cref="ILatticeRegistry.GetAllTreeIdsAsync"/>. A tree authored
    /// only in the peer cluster - never locally registered or written on
    /// the receiver - must still appear in the receiver's registry catalog
    /// once the replication apply path materialises it, otherwise the
    /// explorer shows an empty catalog (as though cross-cluster data never
    /// arrived) even though a point read on the receiver returns the value.
    /// </summary>
    [Test]
    public async Task Replicated_tree_is_listed_in_receiver_registry_catalog_without_local_registration()
    {
        var treeId = NextTreeId("receiver-catalog");

        var treeOnA = await _fixture.CreateReplicatedTreeOnSiteAOnlyAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var registryB = ClientB.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Note: the fixture helper registers the tree on Site A ONLY, so
        // Site B never receives a local RegisterAsync call. Any catalog
        // visibility on Site B is therefore owed purely to the receiver
        // apply path (shipper delivery + ShardRootGrain self-registration).
        // We deliberately do NOT assert Site B is empty up front: once the
        // A->B shipper activates it can deliver - and thus self-register the
        // tree on B - asynchronously, even before the explicit write below.

        await treeOnA.SetAsync("k1", Bytes("v1"));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k1")) == "v1",
            $"Site B should apply Site A's write to '{treeId}/k1'.");

        // The point read converged; the tree must now be discoverable via
        // the same registry catalog the state API / explorer enumerate -
        // despite Site B never having been locally registered.
        var catalogB = await registryB.GetAllTreeIdsAsync();
        Assert.That(catalogB, Does.Contain(treeId),
            "The replication apply path must register the tree in the receiver's "
            + "registry so the explorer / state API catalog lists it.");
    }
}
