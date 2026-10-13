using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task GetVisibleRawEntries_committed_pending_value_and_version_come_from_one_row(bool equalBytes)
    {
        var leaf = CreateGrain();
        await leaf.SetAsync("k", [1]);
        var before = (await leaf.GetRawEntriesAsync(["k"]))[0]!.Value;
        var tx = Guid.NewGuid();
        byte[] prepared = [equalBytes ? (byte)1 : (byte)2];
        await PreparePendingSetAsync(leaf, tx, "k", prepared);

        using var decision = Committed(tx);
        var rows = await leaf.GetVisibleRawEntriesAsync(["missing", "k", "k"]);
        var raw = await leaf.GetRawEntriesAsync(["k"]);

        Assert.Multiple(() =>
        {
            Assert.That(rows[0], Is.Null);
            Assert.That(rows[1]?.Value, Is.EqualTo(prepared));
            Assert.That(rows[1]?.IsTombstone, Is.False);
            Assert.That(rows[1]!.Value.Timestamp.CompareTo(before.Timestamp), Is.GreaterThan(0));
            Assert.That(rows[2], Is.EqualTo(rows[1]));
            Assert.That(raw[0], Is.EqualTo(before), "Raw prepare capture must not acquire logical read semantics.");
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetVisibleRawEntries_committed_pending_tombstone_and_expiry_metadata_survive(bool expired)
    {
        var leaf = CreateGrain();
        await leaf.SetAsync("k", [1]);
        var before = (await leaf.GetRawEntriesAsync(["k"]))[0]!.Value;
        var tx = Guid.NewGuid();
        if (expired)
        {
            LatticeTransactionContext.Set(tx);
            using (LatticePreparedContext.BeginScope())
            {
                await leaf.SetAsync("k", [2], expiresAtTicks: 1);
            }
            LatticeTransactionContext.Set(Guid.Empty);
        }
        else
        {
            await PreparePendingDeleteAsync(leaf, tx, "k");
        }

        using var decision = Committed(tx);
        var row = (await leaf.GetVisibleRawEntriesAsync(["k"]))[0];
        var live = await leaf.GetManyAsync(["k"]);
        Assert.Multiple(() =>
        {
            Assert.That(row, Is.Not.Null);
            Assert.That(row?.IsTombstone, Is.EqualTo(!expired));
            Assert.That(row?.ExpiresAtTicks, Is.EqualTo(expired ? 1L : 0L));
            Assert.That(row!.Value.Timestamp.CompareTo(before.Timestamp), Is.GreaterThan(0));
            Assert.That(live, Is.Empty, "A hidden live-value read must not erase the logical row's metadata.");
        });
    }

    [TestCase((int)TxStatus.InFlight)]
    [TestCase((int)TxStatus.Aborted)]
    [TestCase((int)TxStatus.Indeterminate)]
    public async Task GetVisibleRawEntries_pending_decision_controls_the_entire_row(int outcome)
    {
        var status = (TxStatus)outcome;
        var leaf = CreateGrain();
        await leaf.SetAsync("k", [1]);
        var before = (await leaf.GetRawEntriesAsync(["k"]))[0];
        var tx = Guid.NewGuid();
        await PreparePendingSetAsync(leaf, tx, "k", [2]);
        using var decision = LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [tx] = status });

        var rows = await leaf.GetVisibleRawEntriesAsync(["k"]);

        Assert.That(rows[0], Is.EqualTo(status == TxStatus.Indeterminate ? null : before));
    }

    [Test]
    public async Task GetVisibleRawEntries_root_preserves_pending_row_alignment_and_logical_versions()
    {
        var leaf = CreateGrain();
        await leaf.SetAsync("k", [1]);
        var before = (await leaf.GetRawEntriesAsync(["k"]))[0]!.Value;
        var tx = Guid.NewGuid();
        await PreparePendingSetAsync(leaf, tx, "k", [1]);
        await PreparePendingDeleteAsync(leaf, tx, "gone");
        var root = CreateVisibleReadRoot(leaf);
        using var decision = Committed(tx);

        var logical = await root.GetVisibleRawEntriesAsync(["gone", "k", "missing", "k"]);
        var versions = await root.GetRawVersionsAsync(["gone", "k", "missing", "k"]);
        var materialised = await root.GetRawEntriesIncludingTombstonesAsync(["gone", "k", "missing", "k"]);

        Assert.Multiple(() =>
        {
            Assert.That(logical.Select(row => row?.Key), Is.EqualTo(new string?[] { "gone", "k", null, "k" }));
            Assert.That(logical[0]?.IsTombstone, Is.True);
            Assert.That(logical[1]?.Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(logical[1]!.Value.Timestamp.CompareTo(before.Timestamp), Is.GreaterThan(0));
            Assert.That(versions, Is.EqualTo(logical.Select(row => row?.Timestamp ?? HybridLogicalClock.Zero)));
            Assert.That(materialised[0], Is.Null);
            Assert.That(materialised[1], Is.EqualTo(before));
            Assert.That(materialised[3], Is.EqualTo(before));
        });
    }

    private static ShardRootGrain CreateVisibleReadRoot(IBPlusLeafGrain leaf)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", "visible-source/0"));
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = GrainId.Create("leaf", "visible-leaf");
        state.State.RootIsLeaf = true;
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        return new ShardRootGrain(
            context, state, factory, TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory),
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());
    }
}
