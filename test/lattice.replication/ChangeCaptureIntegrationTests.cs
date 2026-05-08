using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration tests for the change-feed observer registered by
/// <c>AddLatticeReplication</c>: every committed mutation must surface a
/// fully-formed <see cref="WalRecord"/> on the registered
/// <see cref="IReplogSink"/> before the originating grain call returns.
/// </summary>
[TestFixture]
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
    public async Task SetAsync_emits_replog_entry_with_value_and_local_origin()
    {
        const string tree = "ccap-set";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.Entries.Count;
        await lattice.SetAsync("k", new byte[] { 1, 2, 3 });

        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Key == "k").ToArray();
        Assert.That(entries, Is.Not.Empty);
        var entry = entries[^1];
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
    public async Task DeleteAsync_emits_tombstone_entry()
    {
        const string tree = "ccap-del";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("gone", new byte[] { 9 });
        var beforeDelete = _fixture.SiteASink.Entries.Count;
        await lattice.DeleteAsync("gone");

        var deletes = _fixture.SiteASink.Entries.Skip(beforeDelete)
            .Where(e => e.TreeId == tree && e.Key == "gone" && e.Op == MutationKind.Delete)
            .ToArray();
        Assert.That(deletes, Is.Not.Empty);
        Assert.Multiple(() =>
        {
            Assert.That(deletes[^1].IsTombstone, Is.True);
            Assert.That(deletes[^1].Value, Is.Null);
            Assert.That(deletes[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteAClusterId));
        });
    }

    [Test]
    public async Task DeleteRangeAsync_emits_range_entry_per_shard()
    {
        const string tree = "ccap-range";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });

        var before = _fixture.SiteASink.Entries.Count;
        await lattice.DeleteRangeAsync("a", "z");

        var ranges = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Op == MutationKind.DeleteRange)
            .ToArray();
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
    public async Task SetAsync_with_ttl_preserves_expires_at_ticks_on_replog_entry()
    {
        const string tree = "ccap-ttl";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.Entries.Count;
        await lattice.SetAsync("kttl", new byte[] { 1 }, TimeSpan.FromMinutes(5));

        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Key == "kttl").ToArray();
        Assert.That(entries, Is.Not.Empty);
        Assert.That(entries[^1].ExpiresAtTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task Site_b_does_not_observe_site_a_mutations()
    {
        const string tree = "ccap-iso";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var beforeB = _fixture.SiteBSink.Entries.Count;
        await lattice.SetAsync("local-only", new byte[] { 7 });

        // Site B is not wired to Site A by any transport in the fixture, so its
        // sink must not see Site A's writes.
        Assert.That(
            _fixture.SiteBSink.Entries.Skip(beforeB).Any(e => e.TreeId == tree),
            Is.False);
    }

    [Test]
    public async Task SetAsync_under_remote_origin_context_preserves_remote_origin_on_replog_entry()
    {
        // End-to-end cycle-break check: a caller forwarding a remote write
        // wraps the lattice call in LatticeOriginContext.With(peerClusterId)
        // and expects the replog entry to carry the peer's id verbatim,
        // never the local cluster id. Without this guarantee the remote
        // write would loop back out as if it were locally authored.
        const string tree = "ccap-remote-origin";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.Entries.Count;
        using (LatticeOriginContext.With(TwoSiteClusterFixture.SiteBClusterId))
        {
            await lattice.SetAsync("k", new byte[] { 4, 2 });
        }

        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Key == "k").ToArray();
        Assert.That(entries, Is.Not.Empty);
        Assert.That(entries[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteBClusterId));
    }

    [Test]
    public async Task DeleteAsync_under_remote_origin_context_preserves_remote_origin_on_tombstone_entry()
    {
        const string tree = "ccap-remote-origin-del";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("gone", new byte[] { 9 });
        var beforeDelete = _fixture.SiteASink.Entries.Count;
        using (LatticeOriginContext.With(TwoSiteClusterFixture.SiteBClusterId))
        {
            await lattice.DeleteAsync("gone");
        }

        var deletes = _fixture.SiteASink.Entries.Skip(beforeDelete)
            .Where(e => e.TreeId == tree && e.Key == "gone" && e.Op == MutationKind.Delete)
            .ToArray();
        Assert.That(deletes, Is.Not.Empty);
        Assert.That(deletes[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteBClusterId));
    }

    [Test]
    public async Task DeleteRangeAsync_under_remote_origin_context_preserves_remote_origin_on_range_entry()
    {
        const string tree = "ccap-remote-origin-range";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });

        var before = _fixture.SiteASink.Entries.Count;
        using (LatticeOriginContext.With(TwoSiteClusterFixture.SiteBClusterId))
        {
            await lattice.DeleteRangeAsync("a", "z");
        }

        var ranges = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Op == MutationKind.DeleteRange)
            .ToArray();
        Assert.That(ranges, Is.Not.Empty);
        Assert.That(ranges[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteBClusterId));
    }

    // ------------------------------------------------------------------
    // R-090 - MutationCategory.Maintenance writes are skipped end-to-end
    // ------------------------------------------------------------------

    [Test]
    public async Task SetAsync_under_maintenance_scope_does_not_emit_replog_entry()
    {
        // A write authored inside LatticeMaintenanceContext.BeginScope()
        // is stamped with MutationCategory.Maintenance by the leaf grain's
        // mutation publisher, and the replication observer skips the WAL
        // append entirely. The originating lattice call still commits the
        // value to the local tree (maintenance writes are real mutations
        // of state); they just don't cross cluster boundaries.
        const string tree = "ccap-maint-set";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.Entries.Count;
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.SetAsync("k-maint", new byte[] { 1, 2, 3 });
        }

        // Source-site sink saw zero entries for the maintenance write.
        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Key == "k-maint").ToArray();
        Assert.That(entries, Is.Empty);

        // The local tree still committed the value - maintenance is a
        // skip on the *replication* WAL, not on the underlying write.
        var stored = await lattice.GetAsync("k-maint");
        Assert.That(stored, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task DeleteAsync_under_maintenance_scope_does_not_emit_replog_entry()
    {
        // Same contract for tombstones: the leaf still commits the
        // tombstone (subsequent GetAsync returns null) but the
        // replication WAL is silent.
        const string tree = "ccap-maint-del";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("gone-maint", new byte[] { 9 });

        var before = _fixture.SiteASink.Entries.Count;
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.DeleteAsync("gone-maint");
        }

        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Key == "gone-maint").ToArray();
        Assert.That(entries, Is.Empty);

        // Pin that the underlying tombstone still committed locally -
        // the maintenance gate is on replication only, never on the
        // underlying write path.
        var stored = await lattice.GetAsync("gone-maint");
        Assert.That(stored, Is.Null);
    }

    [Test]
    public async Task User_write_after_maintenance_scope_disposes_emits_replog_entry()
    {
        // The maintenance scope is per-call: a follow-up write outside
        // the scope reverts to the User default and emits normally.
        const string tree = "ccap-maint-revert";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.Entries.Count;
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.SetAsync("inside", new byte[] { 1 });
        }
        await lattice.SetAsync("outside", new byte[] { 2 });

        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree).ToArray();
        Assert.That(entries.Select(e => e.Key).ToArray(), Is.EqualTo(new[] { "outside" }));
    }

    [Test]
    public async Task DeleteRangeAsync_under_maintenance_scope_does_not_emit_replog_entries()
    {
        // Range deletes fan out to one emit per shard inside the range.
        // The maintenance gate must apply uniformly to every per-shard
        // emit, leaving the source sink with no DeleteRange entries for
        // the suppressed range.
        const string tree = "ccap-maint-range";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("ma", new byte[] { 1 });
        await lattice.SetAsync("mb", new byte[] { 2 });

        var before = _fixture.SiteASink.Entries.Count;
        using (LatticeMaintenanceContext.BeginScope())
        {
            await lattice.DeleteRangeAsync("ma", "mz");
        }

        var ranges = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Op == MutationKind.DeleteRange)
            .ToArray();
        Assert.That(ranges, Is.Empty);

        // Pin that the underlying range tombstones still committed
        // locally - both keys are gone after the maintenance-scoped
        // range delete returns.
        var maAfter = await lattice.GetAsync("ma");
        var mbAfter = await lattice.GetAsync("mb");
        Assert.Multiple(() =>
        {
            Assert.That(maAfter, Is.Null);
            Assert.That(mbAfter, Is.Null);
        });
    }
}
