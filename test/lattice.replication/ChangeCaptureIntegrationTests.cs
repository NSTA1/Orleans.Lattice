using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration tests for the replication change-capture path registered by
/// <c>AddLatticeReplication</c>. The durable change-feed record now ships
/// from the per-tree leaf write-ahead log, written by the foreground
/// commit-log writer; the producer-side observer's only remaining job is
/// to nudge the background shipper. These tests therefore assert field
/// fidelity against the real shipped records read back from the leaf WAL
/// (via <see cref="LeafWalReader"/>), and assert the commit-time gating
/// rules (mode / maintenance / site isolation) against the observer
/// nudges captured by <see cref="RecordingReplogSink"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ChangeCaptureIntegrationTests
{
    private TwoSiteClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new TwoSiteClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task SetAsync_emits_wal_record_with_value_and_local_origin()
    {
        const string tree = "ccap-set";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 1, 2, 3 });

        var records = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree,
            e => e.Key == "k" && e.Op == MutationKind.Set);
        Assert.That(records, Is.Not.Empty);
        var entry = records[^1];
        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(MutationKind.Set));
            Assert.That(entry.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteAClusterId));
            Assert.That(entry.Timestamp.WallClockTicks, Is.GreaterThan(0L));
        });
    }

    [Test]
    public async Task DeleteAsync_emits_tombstone_wal_record()
    {
        const string tree = "ccap-del";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("gone", new byte[] { 9 });
        await lattice.DeleteAsync("gone");

        var deletes = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree,
            e => e.Key == "gone" && e.Op == MutationKind.Delete);
        Assert.That(deletes, Is.Not.Empty);
        Assert.Multiple(() =>
        {
            Assert.That(deletes[^1].IsTombstone, Is.True);
            Assert.That(deletes[^1].Value, Is.Null);
            Assert.That(deletes[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteAClusterId));
        });
    }

    [Test]
    public async Task DeleteRangeAsync_emits_range_wal_record()
    {
        const string tree = "ccap-range";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });

        await lattice.DeleteRangeAsync("a", "z");

        var ranges = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree,
            e => e.Op == MutationKind.DeleteRange);
        Assert.That(ranges, Is.Not.Empty);
        Assert.Multiple(() =>
        {
            Assert.That(ranges[^1].Key, Is.EqualTo("a"));
            Assert.That(ranges[^1].EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(ranges[^1].IsTombstone, Is.True);
            Assert.That(ranges[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteAClusterId));
        });
    }

    [Test]
    public async Task SetAsync_with_ttl_preserves_expires_at_ticks_on_wal_record()
    {
        const string tree = "ccap-ttl";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("kttl", new byte[] { 1 }, TimeSpan.FromMinutes(5));

        var records = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree,
            e => e.Key == "kttl" && e.Op == MutationKind.Set);
        Assert.That(records, Is.Not.Empty);
        Assert.That(records[^1].ExpiresAtTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task Site_b_is_not_nudged_for_site_a_mutations()
    {
        const string tree = "ccap-iso";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var beforeB = _fixture.SiteBSink.NudgeCount(tree);
        await lattice.SetAsync("local-only", new byte[] { 7 });

        // Site B is not wired to Site A by any transport in the fixture, so its
        // observer must never nudge for Site A's writes.
        Assert.That(_fixture.SiteBSink.NudgeCount(tree), Is.EqualTo(beforeB));
    }

    [Test]
    public async Task SetAsync_under_remote_origin_context_preserves_remote_origin_on_wal_record()
    {
        // End-to-end cycle-break check: a caller forwarding a remote write
        // wraps the lattice call in LatticeOriginContext.With(peerClusterId)
        // and expects the shipped WAL record to carry the peer's id verbatim,
        // never the local cluster id. Without this guarantee the remote
        // write would loop back out as if it were locally authored.
        const string tree = "ccap-remote-origin";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        using (LatticeOriginContext.With(TwoSiteClusterFixture.SiteBClusterId))
        {
            await lattice.SetAsync("k", new byte[] { 4, 2 });
        }

        var records = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree,
            e => e.Key == "k" && e.Op == MutationKind.Set);
        Assert.That(records, Is.Not.Empty);
        Assert.That(records[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteBClusterId));
    }

    [Test]
    public async Task DeleteAsync_under_remote_origin_context_preserves_remote_origin_on_tombstone_record()
    {
        const string tree = "ccap-remote-origin-del";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("gone", new byte[] { 9 });
        using (LatticeOriginContext.With(TwoSiteClusterFixture.SiteBClusterId))
        {
            await lattice.DeleteAsync("gone");
        }

        var deletes = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree,
            e => e.Key == "gone" && e.Op == MutationKind.Delete
                && e.OriginClusterId == TwoSiteClusterFixture.SiteBClusterId);
        Assert.That(deletes, Is.Not.Empty);
        Assert.That(deletes[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteBClusterId));
    }

    [Test]
    public async Task DeleteRangeAsync_under_remote_origin_context_preserves_remote_origin_on_range_record()
    {
        const string tree = "ccap-remote-origin-range";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });

        using (LatticeOriginContext.With(TwoSiteClusterFixture.SiteBClusterId))
        {
            await lattice.DeleteRangeAsync("a", "z");
        }

        var ranges = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree,
            e => e.Op == MutationKind.DeleteRange
                && e.OriginClusterId == TwoSiteClusterFixture.SiteBClusterId);
        Assert.That(ranges, Is.Not.Empty);
        Assert.That(ranges[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteBClusterId));
    }

    // ------------------------------------------------------------------
    // MutationCategory.Maintenance writes are not shipped end-to-end.
    //
    // A maintenance-scoped write still commits to the tree and still
    // lands in the leaf WAL (it is a real state mutation that must be
    // crash-recoverable), but it is stamped MutationCategory.Maintenance
    // and the producer-side observer skips the shipper nudge for it.
    // These tests assert the commit-time gate by checking that no nudge
    // was raised for the maintenance write, and that the underlying tree
    // mutation still committed locally.
    // ------------------------------------------------------------------

    [Test]
    public async Task SetAsync_under_maintenance_scope_does_not_nudge_shipper()
    {
        const string tree = "ccap-maint-set";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.NudgeCount(tree);
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.SetAsync("k-maint", new byte[] { 1, 2, 3 });
        }

        // The maintenance write raised no shipper nudge.
        Assert.That(_fixture.SiteASink.NudgeCount(tree), Is.EqualTo(before));

        // The local tree still committed the value - maintenance is a
        // skip on the *replication* ship, not on the underlying write.
        var stored = await lattice.GetAsync("k-maint");
        Assert.That(stored, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task DeleteAsync_under_maintenance_scope_does_not_nudge_shipper()
    {
        const string tree = "ccap-maint-del";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("gone-maint", new byte[] { 9 });

        var before = _fixture.SiteASink.NudgeCount(tree);
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.DeleteAsync("gone-maint");
        }

        Assert.That(_fixture.SiteASink.NudgeCount(tree), Is.EqualTo(before));

        // The underlying tombstone still committed locally - the
        // maintenance gate is on replication only, never on the write.
        var stored = await lattice.GetAsync("gone-maint");
        Assert.That(stored, Is.Null);
    }

    [Test]
    public async Task User_write_after_maintenance_scope_disposes_nudges_shipper()
    {
        // The maintenance scope is per-call: a follow-up write outside
        // the scope reverts to the User default and nudges normally.
        const string tree = "ccap-maint-revert";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.NudgeCount(tree);
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.SetAsync("inside", new byte[] { 1 });
        }
        await lattice.SetAsync("outside", new byte[] { 2 });

        // Exactly one nudge: the maintenance-scoped write was gated out,
        // the User-default write that followed nudged.
        Assert.That(_fixture.SiteASink.NudgeCount(tree), Is.EqualTo(before + 1));

        // Both writes committed to the leaf WAL, but only the User write
        // is shippable: the maintenance write is stamped
        // MutationCategory.Maintenance and the shipper skips it.
        var inside = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree, e => e.Key == "inside");
        var outside = await LeafWalReader.WaitForRecordsAsync(
            _fixture.SiteA.Client, tree, e => e.Key == "outside");
        Assert.Multiple(() =>
        {
            Assert.That(inside[^1].Category, Is.EqualTo(MutationCategory.Maintenance));
            Assert.That(outside[^1].Category, Is.EqualTo(MutationCategory.User));
        });
    }

    [Test]
    public async Task DeleteRangeAsync_under_maintenance_scope_does_not_nudge_shipper()
    {
        const string tree = "ccap-maint-range";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("ma", new byte[] { 1 });
        await lattice.SetAsync("mb", new byte[] { 2 });

        var before = _fixture.SiteASink.NudgeCount(tree);
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.DeleteRangeAsync("ma", "mz");
        }

        Assert.That(_fixture.SiteASink.NudgeCount(tree), Is.EqualTo(before));

        // The underlying range tombstones still committed locally - both
        // keys are gone after the maintenance-scoped range delete returns.
        var maAfter = await lattice.GetAsync("ma");
        var mbAfter = await lattice.GetAsync("mb");
        Assert.Multiple(() =>
        {
            Assert.That(maAfter, Is.Null);
            Assert.That(mbAfter, Is.Null);
        });
    }
}
