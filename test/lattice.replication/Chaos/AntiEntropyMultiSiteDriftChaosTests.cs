using System.Linq;
using System.Text;
using NUnit.Framework;
using Orleans.Lattice;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// End-to-end chaos coverage of the cross-cluster anti-entropy chain over a
/// real three-site cluster. Each test injects one controlled drift fault
/// mode against site C while sites A and B stay healthy, asserts that the
/// drift is observable as a real divergence in replicated state (the diverged
/// site is missing entries the authoring site holds), and - for the
/// recoverable modes - that the production shipper closes the gap so the
/// diverged site converges back to the authoring site's state after the fault
/// is cleared.
/// <para>
/// The three fault modes are: skipped writes (the outbound replication edge
/// to C is dropped so C never sees A's writes), corrupted apply (C's
/// receiver-side applier throws on every inbound batch so the writes are
/// rejected mid-apply), and partition-then-heal (both directions of the C
/// edge are cut, both sides take divergent writes, then the edge heals and
/// the two write sets reconcile under last-writer-wins).
/// </para>
/// <para>
/// Convergence and divergence are asserted on observable replicated key state
/// rather than on the leaf projection-digest hash: that hash deliberately
/// folds in each silo's local applied-prefix checkpoint offset, so two silos
/// holding identical logical entries still report distinct hashes. Key-level
/// presence is the cross-site-stable signal the production shipper actually
/// converges. Digest-probe detection is pinned deterministically by the
/// companion guard-chain tests.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class AntiEntropyMultiSiteDriftChaosTests
{
    private const string TreeName = "chaos-anti-entropy";

    /// <summary>
    /// Shared convergence ceiling for every <c>WaitForKeysAsync</c> in this
    /// fixture. The cold-start baseline (full silo startup + replication
    /// bootstrap + first ship cadence) is strictly more expensive than the
    /// warm post-heal drain, so both share one generous budget rather than
    /// giving the baseline a tighter window that blows first under CI load.
    /// </summary>
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(45);

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Polls <paramref name="peer"/> until every key in <paramref name="keys"/>
    /// is present, or fails on timeout. This is the cross-site convergence
    /// signal the production shipper drives.
    /// </summary>
    private static async Task WaitForKeysAsync(ILattice peer, IEnumerable<string> keys, TimeSpan timeout)
    {
        var keysArr = keys.ToArray();
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var allPresent = true;
            foreach (var k in keysArr)
            {
                if (await peer.GetAsync(k) is null)
                {
                    allPresent = false;
                    break;
                }
            }
            if (allPresent)
            {
                return;
            }
            await Task.Delay(100);
        }
        Assert.Fail($"Peer did not converge on {keysArr.Length} keys within {timeout.TotalSeconds}s.");
    }

    /// <summary>
    /// Confirms a real divergence: the authoring site holds every key in
    /// <paramref name="keys"/> while <paramref name="diverged"/> is missing at
    /// least one of them, and that this divergence is stable for a short
    /// settling window (so an in-flight delivery is not mistaken for a closed
    /// gap). Fails if the diverged site silently catches up.
    /// </summary>
    private static async Task AssertDivergedAsync(
        ILattice author, ILattice diverged, IReadOnlyList<string> keys, TimeSpan settle)
    {
        foreach (var k in keys)
        {
            Assert.That(await author.GetAsync(k), Is.Not.Null, $"author should hold '{k}'");
        }

        var deadline = DateTime.UtcNow + settle;
        while (DateTime.UtcNow < deadline)
        {
            var missing = false;
            foreach (var k in keys)
            {
                if (await diverged.GetAsync(k) is null)
                {
                    missing = true;
                    break;
                }
            }
            Assert.That(missing, Is.True, "diverged site unexpectedly holds every drifted key");
            await Task.Delay(100);
        }
    }

    [Test]
    public async Task Skipped_writes_to_the_isolated_site_diverge_and_heal_on_reconnect()
    {
        await using var fixture = new ProductionShipperFixture(TreeName, siteCount: 3);
        await fixture.InitializeAsync();

        var siteCId = fixture.ClusterIds[2];
        var a = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var c = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        // Establish a converged baseline so every site shares the same state.
        for (var i = 0; i < 4; i++)
        {
            await a.SetAsync($"base-{i}", V($"base-{i}"));
        }
        await WaitForKeysAsync(c, Enumerable.Range(0, 4).Select(i => $"base-{i}"), ConvergenceTimeout);

        // Skip writes at C: drop A's outbound edge to C, then author writes
        // that C never observes. A and C must diverge.
        fixture.TransportOf(0).IsolateSite(siteCId);
        var drifted = new[] { "skip-0", "skip-1", "skip-2" };
        foreach (var k in drifted)
        {
            await a.SetAsync(k, V(k));
        }
        await AssertDivergedAsync(a, c, drifted, TimeSpan.FromMilliseconds(200));

        // Heal the edge: the production shipper resumes from its stationary
        // per-peer cursor and ships the skipped backlog. C converges.
        fixture.TransportOf(0).HealSite(siteCId);
        await WaitForKeysAsync(c, drifted, ConvergenceTimeout);
    }

    [Test]
    public async Task Corrupted_apply_at_the_diverged_site_is_detected_and_heals_when_the_fault_clears()
    {
        await using var fixture = new ProductionShipperFixture(TreeName, siteCount: 3);
        await fixture.InitializeAsync();

        var a = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var c = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        for (var i = 0; i < 4; i++)
        {
            await a.SetAsync($"base-{i}", V($"base-{i}"));
        }
        await WaitForKeysAsync(c, Enumerable.Range(0, 4).Select(i => $"base-{i}"), ConvergenceTimeout);

        // Corrupt the apply path at C: every inbound batch throws mid-apply,
        // so C rejects the writes and falls behind. A and C diverge.
        fixture.ApplierOf(2).FailEveryNthCall = 1;
        var drifted = new[] { "corrupt-0", "corrupt-1", "corrupt-2" };
        foreach (var k in drifted)
        {
            await a.SetAsync(k, V(k));
        }
        await AssertDivergedAsync(a, c, drifted, TimeSpan.FromMilliseconds(200));
        Assert.That(fixture.ApplierOf(2).InjectedFailures, Is.GreaterThan(0));

        // Clear the apply fault: the shipper's retry path re-delivers the
        // rejected batches, C applies them, and converges.
        fixture.ApplierOf(2).FailEveryNthCall = 0;
        await WaitForKeysAsync(c, drifted, ConvergenceTimeout);
    }

    [Test]
    public async Task Partition_then_heal_diverges_both_directions_and_reconciles_under_last_writer_wins()
    {
        await using var fixture = new ProductionShipperFixture(TreeName, siteCount: 3);
        await fixture.InitializeAsync();

        var siteAId = fixture.ClusterIds[0];
        var siteCId = fixture.ClusterIds[2];
        var a = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var c = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        for (var i = 0; i < 2; i++)
        {
            await a.SetAsync($"base-{i}", V($"base-{i}"));
        }
        await WaitForKeysAsync(c, Enumerable.Range(0, 2).Select(i => $"base-{i}"), ConvergenceTimeout);

        // Full partition: cut both directions of the A-C edge, then take
        // divergent writes on each side of the split (distinct keys so the
        // post-heal union is well defined and stays inside a single leaf).
        fixture.TransportOf(0).IsolateSite(siteCId);
        fixture.TransportOf(2).IsolateSite(siteAId);

        var aSide = new[] { "a-side-0", "a-side-1" };
        var cSide = new[] { "c-side-0", "c-side-1" };
        foreach (var k in aSide)
        {
            await a.SetAsync(k, V(k));
        }
        foreach (var k in cSide)
        {
            await c.SetAsync(k, V(k));
        }
        // Each side holds its own writes and is missing the other's. The
        // divergence window is kept short (well inside the fixture's
        // maintenance-GC horizon) so the un-acked backlog survives in each
        // site's WAL and the production shipper can re-ship it on heal. A
        // partition that outlives GC reaping is the GC'd-divergence case
        // covered by the bootstrap-fallback path, exercised separately.
        await AssertDivergedAsync(a, c, aSide, TimeSpan.FromMilliseconds(200));
        await AssertDivergedAsync(c, a, cSide, TimeSpan.FromMilliseconds(200));

        // Heal both directions: each site ships its accumulated backlog to
        // the other and the two write sets reconcile under last-writer-wins.
        fixture.TransportOf(0).HealSite(siteCId);
        fixture.TransportOf(2).HealSite(siteAId);

        var union = aSide.Concat(cSide).ToArray();
        await WaitForKeysAsync(c, union, ConvergenceTimeout);
        await WaitForKeysAsync(a, union, ConvergenceTimeout);
    }
}
