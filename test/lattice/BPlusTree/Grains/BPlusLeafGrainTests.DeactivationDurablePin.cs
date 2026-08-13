using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #1453 ("fall off the log"): on a
/// snapshot-less, replication-less host the durable materialiser pin is the
/// only thing that stops the shared per-partition WAL being trimmed past a
/// dormant write-once leaf's durable checkpoint. Before this fix the leaf only
/// ever mirrored its frontier fire-and-forget/coalesced, so a leaf could
/// checkpoint, go dormant, and - if the coalesced/birth write was lost across a
/// container restart - leave <b>no</b> durable floor, after which the GC trimmed
/// past its checkpoint and the next cold activation fell off the log.
/// <para>
/// The fix makes the durable pin an authoritative retention barrier on two cold
/// paths: (A) the first time a leaf crosses from its Zero block pin to a real
/// frontier it <b>awaits</b> the durable pin write, and (B) on graceful
/// deactivation it <b>awaits</b> a durable pin write of its final frontier.
/// These tests drive the real leaf through the real
/// <see cref="LeafCursorReporter"/> and <see cref="WalMaterialiserPinGrain"/>
/// (reusing the <c>CreateLeafWithDurablePinStore</c> rig) and assert the pin is
/// durable, then run the real <see cref="LatticeWalGc"/> with the in-memory
/// registry wiped (simulating a restart) and assert the WAL is not trimmed past
/// the leaf's checkpoint.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static async Task CheckpointLeafAsync(BPlusLeafGrain leaf, string key, long hlcPhysical, long offset)
    {
        var projection = AsProjection(leaf);
        projection.Apply(BuildSet(key, Encoding.UTF8.GetBytes("v"), hlcPhysical: hlcPhysical, treeId: PinSeamTreeId));
        await projection.SetCheckpointOffsetAsync(offset, default);
        await projection.FlushCheckpointAsync(default);
    }

    [Test]
    public async Task First_real_frontier_checkpoint_awaits_durable_pin_write()
    {
        // Barrier A: a leaf that has checkpointed once must already have a
        // durable real-frontier pin, without relying on the debounced
        // fire-and-forget mirror landing (which an ungraceful crash could lose).
        var (leaf, pinGrain, _, _) =
            CreateLeafWithDurablePinStore(leafKey: "deact-pin-leaf-A", treeId: PinSeamTreeId);

        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 100, offset: 1);

        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins, Is.Not.Empty,
            "The first real-frontier checkpoint must synchronously (awaited) persist a durable pin.");
        Assert.That(pins.Values, Has.All.GreaterThan(HybridLogicalClock.Zero),
            "The durable pin must record the leaf's real checkpoint frontier, not a Zero block pin.");
    }

    [Test]
    public async Task Deactivation_flushes_final_frontier_to_durable_pin_store()
    {
        // Barrier B: after a graceful deactivation the durable pin must reflect
        // the leaf's final checkpoint frontier.
        var (leaf, pinGrain, _, _) =
            CreateLeafWithDurablePinStore(leafKey: "deact-pin-leaf-B", treeId: PinSeamTreeId);

        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 200, offset: 1);

        // The first-frontier barrier has already persisted the real frontier;
        // capture it so we can assert deactivation preserves (does not lose or
        // roll back) the durable floor.
        var before = await pinGrain.GetPinsAsync();
        Assert.That(before, Is.Not.Empty, "precondition: the checkpoint must have seeded a durable pin.");
        var frontier = before.Values.First();

        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins, Is.Not.Empty,
            "Graceful deactivation must leave a durable retention pin behind.");
        Assert.That(pins.Values, Has.All.EqualTo(frontier),
            "The deactivation flush must persist the leaf's final checkpoint frontier.");
    }

    [Test]
    public async Task Dormant_leaf_pin_survives_registry_wipe_and_blocks_trim_past_checkpoint()
    {
        // End-to-end "fall off the log" regression: a leaf checkpoints at HLC
        // 20 and deactivates; the in-memory registry is then wiped (simulating a
        // full container restart); a forward consumer (shipper) sits at the WAL
        // head (40). Without the deactivation durable-pin barrier the GC would
        // floor only under the shipper and trim past the leaf's checkpoint,
        // discarding entries the next cold activation must replay. The durable
        // pin at 20 must floor the trim so every entry above the checkpoint
        // survives.
        var (leaf, _, registry, factory) =
            CreateLeafWithDurablePinStore(leafKey: "deact-pin-leaf-C", treeId: PinSeamTreeId);

        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 20, offset: 1);
        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // Simulate the restart: the process-local registry is wiped, so the
        // dormant leaf is ABSENT from it and only its durable pin remains.
        var freshRegistry = new InMemoryWalCursorRegistry();

        // WAL holds four entries; the leaf checkpointed at HLC 20 so it still
        // needs to replay the entries above 20 (offsets 2 and 3).
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            PinSeamTreeId,
            0,
            new[]
            {
                PinWalEntry(0, PinHlc(10)),
                PinWalEntry(1, PinHlc(20)),
                PinWalEntry(2, PinHlc(30)),
                PinWalEntry(3, PinHlc(40)),
            },
            CancellationToken.None);

        // A forward consumer sits at the WAL head - on baseline it would drag
        // the trim floor to 40 and discard offsets 2 and 3.
        await freshRegistry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(40));

        var gcServices = new ServiceCollection();
        gcServices.AddSingleton<IWalStorageProvider>(provider);
        gcServices.AddSingleton(factory);
        var gc = new LatticeWalGc(gcServices.BuildServiceProvider(), freshRegistry, PinOptionsMonitor());

        var report = await gc.RunOnceAsync(PinSeamTreeId);

        Assert.That(report.MinCursor, Is.EqualTo(PinHlc(20)),
            "The dormant leaf's durable pin (20) must floor the trim under the forward consumer (40).");

        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Does.Contain(2L).And.Contain(3L),
            "Every WAL entry above the leaf's durable checkpoint must survive the restart GC pass.");
    }
}
