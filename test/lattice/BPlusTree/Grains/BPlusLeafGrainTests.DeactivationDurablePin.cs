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
        // REWRITTEN for the coverage-gated durable pin (residual cold-restart
        // prefix-loss fix). The ORIGINAL #1453 assertion required the first
        // checkpoint to persist a durable pin at the leaf's REAL frontier
        // (Has.All.GreaterThan(Zero)). That assertion encoded the UNSAFE
        // "trim-at-checkpoint-without-snapshot-coverage" mechanism: a real
        // frontier pin authorises the WAL GC to trim the checkpointed prefix
        // [0, checkpoint], but this rig wires NO snapshot store, so the
        // checkpointed prefix has NO durable copy other than the WAL itself.
        // Trimming it is exactly the residual loss - on the next cold rebuild
        // the empty cache replays from -1 over a WAL whose prefix is gone.
        // Under the fix a data-bearing, checkpointed-but-uncovered partition
        // retains its Zero BLOCK pin (a retention barrier), which is the
        // STRENGTHENED no-loss contract: the durable pin still lands (barrier
        // A still awaits the write), but it pins the WAL rather than licensing
        // its truncation. The block lifts to a real frontier only once a
        // durable snapshot covers the prefix (proved by the invariant-(b)
        // test in BPlusLeafGrainTests.ColdRestartResidualPin.cs).
        var (leaf, pinGrain, _, _) =
            CreateLeafWithDurablePinStore(leafKey: "deact-pin-leaf-A", treeId: PinSeamTreeId);

        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 100, offset: 1);

        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins, Is.Not.Empty,
            "The first real-frontier checkpoint must synchronously (awaited) persist a durable pin.");
        Assert.That(pins.Values, Has.All.EqualTo(HybridLogicalClock.Zero),
            "With no snapshot covering the checkpointed prefix, the durable pin must be a Zero block "
            + "pin that RETAINS the WAL - not a real frontier that would authorise trimming an "
            + "unrecoverable prefix.");
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
        // REWRITTEN for the coverage-gated durable pin (residual cold-restart
        // prefix-loss fix). The ORIGINAL #1453 assertion expected the dormant
        // leaf to floor the trim at its real checkpoint frontier (MinCursor ==
        // HLC 20) and retain only the entries ABOVE the checkpoint (offsets 2
        // and 3), i.e. it assumed the checkpointed prefix [0, 1] was safe to
        // trim. That assumption is the residual defect: this rig wires NO
        // snapshot store, so the checkpointed prefix has no durable copy and a
        // cold rebuild (empty cache -> replay from -1) would silently lose it.
        // Under the fix the data-bearing, uncovered leaf reports a Zero BLOCK
        // pin, which disables the WAL GC cursor trim entirely (MinCursor null)
        // and retains the WHOLE WAL - the strengthened, provably-lossless
        // contract. Trimming resumes only once a durable snapshot covers the
        // prefix (invariant (b), see BPlusLeafGrainTests.ColdRestartResidualPin.cs).
        var (leaf, _, registry, factory) =
            CreateLeafWithDurablePinStore(leafKey: "deact-pin-leaf-C", treeId: PinSeamTreeId);

        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 20, offset: 1);
        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // Simulate the restart: the process-local registry is wiped, so the
        // dormant leaf is ABSENT from it and only its durable pin remains.
        var freshRegistry = new InMemoryWalCursorRegistry();

        // WAL holds four entries; the leaf checkpointed at HLC 20 but, with no
        // snapshot covering the prefix, its Zero block pin protects EVERY entry.
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

        Assert.That(report.MinCursor, Is.Null,
            "The dormant leaf's Zero block pin (no snapshot coverage) must DISABLE the cursor trim "
            + "entirely, not floor it at the checkpoint - the checkpointed prefix is unrecoverable.");

        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Does.Contain(0L).And.Contain(1L).And.Contain(2L).And.Contain(3L),
            "With no snapshot covering the prefix, EVERY WAL entry - including the checkpointed "
            + "prefix [0, 1] - must survive the restart GC pass so a cold rebuild can replay from 0.");
    }
}
