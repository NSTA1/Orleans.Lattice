using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Tests.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4768: a just-live import must not re-seed itself. The receiver's
/// first lineage after a shipper only stepped past filtered entries is not a
/// gap, and the lineage the settling import installed - reported on the same
/// acknowledgement as its echo - is that import's own replacement, not a new
/// one. A lineage-less echo settles nothing once lineages are reported, and a
/// later replacement still forces a re-seed.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private static WalRecord PeerOriginSet(string tree, string key, HybridLogicalClock stamp) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = stamp,
        OriginClusterId = TwoSiteClusterFixture.SiteBClusterId,
    };

    private static async Task<(Orleans.Lattice.Replication.Grains.ReplicationShipperGrain Shipper,
        ReplicationShipperGrainTests.StubReplogShardGrain[] Feeds, FrontierTransport Transport)> ForcedReseedAsync(string tree, long ticks)
    {
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50));
        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        transport.Lineage = Guid.NewGuid();
        feeds[1].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: a replacement after acknowledgements forces a re-seed");
        return (shipper, feeds, transport);
    }

    [Test]
    public async Task A_first_lineage_after_the_cursor_passed_only_filtered_entries_does_not_re_seed()
    {
        const string tree = "ccv-reseed-lineage-filtered";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, state) = FrontierShipper(tree, Hlc(ticks, 50));

        // Only the peer's own writes, which are never shipped back to it.
        feeds[0].Append(PeerOriginSet(tree, "peer", Hlc(ticks, 5)));
        await PumpAsync(shipper, ticks: 2);
        Assert.That(state.State.PartitionCursors.Values, Has.Some.GreaterThan(0), "precondition: the cursor stepped past the filtered entry");
        Assert.That(state.State.Frontier.LineageObserved, Is.False);

        transport.Lineage = Guid.NewGuid();
        feeds[1].Append(LocalSet(tree, "a", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);

        Assert.That(shipper.ReseedRequired, Is.False,
            "the peer was never sent data, so its first lineage identifies contents nothing was acknowledged against");
    }

    [Test]
    public async Task The_lineage_the_settling_import_installed_does_not_re_seed_again()
    {
        const string tree = "ccv-reseed-lineage-settled";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport) = await ForcedReseedAsync(tree, ticks);

        // The import replaces the peer's contents under a new lineage and
        // reports it on the acknowledgement that echoes the completion.
        transport.Lineage = Guid.NewGuid();
        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        var reforced = new List<int>();
        for (var tick = 0; tick < 6; tick++)
        {
            if (tick == 3)
            {
                feeds[0].Append(LocalSet(tree, "d", Hlc(ticks, 62)));
            }

            await PumpAsync(shipper, ticks: 1);
            if (shipper.ReseedRequired)
            {
                reforced.Add(tick);
            }
        }

        // Checked after every tick: the harness keeps echoing the same epoch, so a
        // re-forced re-seed would settle again a tick later and hide the restart.
        Assert.That(reforced, Is.Empty, "the settling import's own lineage is not a further replacement");
    }

    [Test]
    public async Task A_replacement_after_the_re_seed_settled_still_re_seeds()
    {
        const string tree = "ccv-reseed-lineage-later";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport) = await ForcedReseedAsync(tree, ticks);
        transport.Lineage = Guid.NewGuid();
        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 6);
        Assert.That(shipper.ReseedRequired, Is.False, "precondition: the re-seed settled");

        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "d", Hlc(ticks, 62)));
        await PumpAsync(shipper, ticks: 1);

        Assert.That(shipper.ReseedRequired, Is.True, "a replacement no import settled is a gap");
    }

    [Test]
    public async Task An_echo_without_a_lineage_settles_nothing_once_the_peer_reports_lineages()
    {
        const string tree = "ccv-reseed-lineage-unbound-echo";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport) = await ForcedReseedAsync(tree, ticks);
        var current = transport.Lineage;

        transport.Lineage = null;
        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 6);
        var unbound = shipper.ReseedRequired;

        transport.Lineage = current;
        feeds[0].Append(LocalSet(tree, "d", Hlc(ticks, 62)));
        await PumpAsync(shipper, ticks: 6);

        Assert.Multiple(() =>
        {
            Assert.That(unbound, Is.True, "an echo the receiver could not bind to its contents vouches for nothing");
            Assert.That(shipper.ReseedRequired, Is.False, "the same echo bound to the current lineage settles it");
        });
    }
}
