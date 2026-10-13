using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(null, false)]
    [TestCase(0, false)]
    [TestCase(7, false)]
    [TestCase(7, true)]
    public async Task DeleteAsync_persists_owning_shard_in_encoded_WAL(int? shard, bool tracked)
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new OrleansBinaryWalRecordEncoder(services.GetRequiredService<Serializer<WalRecord>>());
        var persisted = new List<WalRecord>();
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            var record = call.Arg<WalRecord>();
            var bytes = new ArrayBufferWriter<byte>();
            encoder.Encode(in record, bytes);
            persisted.Add(encoder.Decode(bytes.WrittenSpan, MaterialiserTreeId, LatticeMergeMode.LwwRegister));
            return Task.FromResult((long)persisted.Count);
        });
        var grain = CreateGrain(new FakePersistentState<LeafNodeState>
        {
            State = new LeafNodeState { TreeId = MaterialiserTreeId },
        }, commitLog: writer);
        if (shard is { } index)
            await grain.SetShardIndexAsync(index);
        await grain.SetAsync("deleted", [1, 2, 3]);

        var deleted = tracked
            ? (await grain.DeleteTrackedAsync("deleted")).Deleted
            : await grain.DeleteAsync("deleted");
        Assert.That(deleted, Is.True);

        var deletion = persisted[^1];
        Assert.Multiple(() =>
        {
            Assert.That(deletion.Op, Is.EqualTo(MutationKind.Delete));
            Assert.That(deletion.ShardIndex, Is.EqualTo(shard ?? 0));
            Assert.That(deletion.IsTombstone, Is.True);
            Assert.That(deletion.Value, Is.Null);
            Assert.That(deletion.Timestamp, Is.GreaterThan(persisted[0].Timestamp));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Cold_replay_of_encoded_acknowledged_writes_preserves_delete_and_sibling_isolation(bool withMap)
    {
        const int shard = 7;
        var map = ShardMap.CreateDefault(64, 8);
        var keys = Enumerable.Range(0, 256).Select(i => $"key-{i:D3}")
            .Where(k => map.Resolve(k) == shard).Take(3).ToArray();
        Assert.That(keys, Has.Length.EqualTo(3));
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new OrleansBinaryWalRecordEncoder(services.GetRequiredService<Serializer<WalRecord>>());
        var persisted = new List<CommitLogSliceEntry>();
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            var record = call.Arg<WalRecord>();
            var bytes = new ArrayBufferWriter<byte>();
            encoder.Encode(in record, bytes);
            var decoded = encoder.Decode(bytes.WrittenSpan, MaterialiserTreeId, LatticeMergeMode.LwwRegister);
            persisted.Add(new CommitLogSliceEntry(persisted.Count + 1, WalRecordConverter.FromWalRecord(decoded)));
            return Task.FromResult((long)persisted.Count);
        });
        var foreground = CreateGrain(new FakePersistentState<LeafNodeState>
        {
            State = new LeafNodeState { TreeId = MaterialiserTreeId },
        }, commitLog: writer);
        await foreground.SetShardIndexAsync(shard);
        await foreground.SetAsync(keys[0], [1]);
        await foreground.SetAsync(keys[1], [2]);
        await foreground.SetAsync(keys[1], [3]);
        await foreground.SetAsync(keys[2], [4]);
        Assert.That(await foreground.DeleteAsync(keys[2]), Is.True);
        Assert.That(await foreground.GetAsync(keys[2]), Is.Null);
        var overwriteVersion = await foreground.GetWithVersionAsync(keys[1]);
        var deleteStamp = persisted[^1].Mutation.Timestamp;
        var foreignKey = Enumerable.Range(0, 256).Select(i => $"foreign-{i:D3}")
            .First(k => map.Resolve(k) != shard);
        persisted.Add(new CommitLogSliceEntry(persisted.Count + 1,
            BuildCommittedSet(foreignKey, [9]) with { ShardIndex = map.Resolve(foreignKey) }));
        var coordinator = BuildCoordinator(persisted.Count + 1, persisted.ToArray());
        var (cold, state, _, _) = CreateGrainWithMaterialiser(coordinator,
            seedState: s => s.ShardIndex = shard, registryShardMap: withMap ? map : null);
        Assert.That(cold.EntriesForTest, Is.Empty, "A new activation must rebuild from WAL, not a warm projection.");

        await ActivateAsync(cold);

        var deletedVersion = await cold.GetWithVersionAsync(keys[2]);
        var recoveredOverwrite = await cold.GetWithVersionAsync(keys[1]);
        var many = await cold.GetManyAsync([.. keys, foreignKey]);
        Assert.Multiple(() =>
        {
            Assert.That(cold.EntriesForTest[keys[2]].IsTombstone, Is.True);
            Assert.That(cold.EntriesForTest[keys[2]].Timestamp, Is.EqualTo(deleteStamp));
            Assert.That(deletedVersion.Value, Is.Null);
            Assert.That(deletedVersion.Version, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(recoveredOverwrite, Is.EqualTo(overwriteVersion));
            Assert.That(many.Keys, Is.EquivalentTo(keys.Take(2)));
            Assert.That(many[keys[0]], Is.EqualTo(new byte[] { 1 }));
            Assert.That(many[keys[1]], Is.EqualTo(new byte[] { 3 }));
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(persisted.Count));
        });
        Assert.That(await cold.GetAsync(keys[2]), Is.Null);
        Assert.That(await cold.GetAsync(foreignKey), Is.Null);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Cold_replay_of_legacy_zero_stamped_delete_requires_authoritative_map(bool withMap)
    {
        const int shard = 7;
        var map = ShardMap.CreateDefault(64, 8);
        var key = Enumerable.Range(0, 256).Select(i => $"legacy-{i:D3}").First(k => map.Resolve(k) == shard);
        var set = BuildCommittedSet(key, [1]) with { ShardIndex = shard };
        var legacyDelete = BuildDelete(key, treeId: MaterialiserTreeId) with { ShardIndex = 0 };
        var coordinator = BuildCoordinator(3,
            new CommitLogSliceEntry(1, set), new CommitLogSliceEntry(2, legacyDelete));
        var (cold, _, _, _) = CreateGrainWithMaterialiser(coordinator,
            seedState: s => s.ShardIndex = shard, registryShardMap: withMap ? map : null);

        await ActivateAsync(cold);

        // A default-0 record cannot identify a nonzero owner without a map.
        Assert.That(cold.EntriesForTest[key].IsTombstone, Is.EqualTo(withMap));
        Assert.That(await cold.GetAsync(key), withMap ? Is.Null : Is.EqualTo(new byte[] { 1 }));
    }
}
