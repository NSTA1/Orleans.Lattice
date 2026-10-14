using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Historical_delete_recovery_preserves_consumed_evidence_and_rebuilds_only_with_prior_map(
        bool laterRecreation)
    {
        const int shard = 7;
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var stateSerializer = services.GetRequiredService<Serializer<LeafNodeState>>();
        var map = ShardMap.CreateDefault(64, 8);
        var keys = Enumerable.Range(0, 1024).Select(i => $"historical-{i:D4}")
            .Where(k => map.Resolve(k) == shard).Take(2).ToArray();
        Assert.That(keys, Has.Length.EqualTo(2));
        var foreignKey = Enumerable.Range(0, 1024).Select(i => $"foreign-{i:D4}")
            .First(k => map.Resolve(k) != shard);
        var deletion = BuildDelete(keys[0], hlcPhysical: 200, treeId: ResumableTreeId) with { ShardIndex = 0 };
        var evidence = new List<CommitLogSliceEntry>
        {
            new(1, BuildCommittedSet(keys[0], [1], treeId: ResumableTreeId) with { ShardIndex = shard }),
            new(2, BuildCommittedSet(keys[1], [2], treeId: ResumableTreeId) with { ShardIndex = shard }),
            new(3, deletion),
            new(4, BuildCommittedSet(foreignKey, [9], treeId: ResumableTreeId) with
                { ShardIndex = map.Resolve(foreignKey) }),
        };
        if (laterRecreation)
            evidence.Add(new(5, BuildCommittedSet(keys[0], [3], hlcPhysical: 300, treeId: ResumableTreeId) with
                { ShardIndex = shard }));

        var coordinator = BuildCoordinator(evidence.Count + 1, evidence.ToArray());
        var originalStore = new InMemorySnapshotStore();
        var originalState = NewResumableState();
        originalState.State.ShardIndex = shard;
        var (consumed, _) = BuildResumableLeaf(originalState, coordinator, originalStore.Stub, reclassifyEveryN: 1);
        await ActivateAsync(consumed);
        Assert.That(await consumed.GetAsync(keys[0]), Is.EqualTo(new byte[] { laterRecreation ? (byte)3 : (byte)1 }));
        Assert.That(originalState.State.ProjectionCheckpointOffset, Is.EqualTo(evidence.Count));
        Assert.That(originalStore.Latest, Is.Not.Null);
        var originalSnapshot = originalStore.Latest!;
        var originalSaveCount = originalStore.SaveCount;
        var originalStateBytes = stateSerializer.SerializeToArray(originalState.State);
        Assert.That(originalSnapshot.ScalarOffsetOrSentinel(), Is.EqualTo(evidence.Count));

        // Rehydrate a separate clone from the already-consumed snapshot, now with a map.
        var cloneStore = new InMemorySnapshotStore(originalSnapshot);
        var cloneState = NewResumableState();
        cloneState.State = stateSerializer.Deserialize(originalStateBytes);
        var (lateMap, _) = BuildResumableLeaf(cloneState, coordinator, cloneStore.Stub,
            reclassifyEveryN: 1, registryShardMap: map);
        await ActivateAsync(lateMap);
        Assert.That(await lateMap.GetAsync(keys[0]), Is.EqualTo(new byte[] { laterRecreation ? (byte)3 : (byte)1 }),
            "A map added after snapshot coverage cannot recover the skipped tombstone.");

        // Build a NEW isolated projection from the complete evidence, not by resetting the original.
        var rebuiltStore = new InMemorySnapshotStore();
        var rebuiltState = NewResumableState();
        rebuiltState.State.ShardIndex = shard;
        var (rebuilt, _) = BuildResumableLeaf(rebuiltState, coordinator, rebuiltStore.Stub,
            reclassifyEveryN: 1, registryShardMap: map);
        await ActivateAsync(rebuilt);
        var delta = await rebuilt.GetDeltaSinceAsync(new VersionVector());
        var many = await rebuilt.GetManyAsync([.. keys, foreignKey]);
        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries.Keys, Is.EquivalentTo(keys));
            Assert.That(delta.Entries[keys[0]].IsTombstone, Is.EqualTo(!laterRecreation));
            Assert.That(delta.Entries[keys[0]].Timestamp.WallClockTicks, Is.EqualTo(laterRecreation ? 300 : 200));
            Assert.That(many.Keys, Is.EquivalentTo(laterRecreation ? keys : keys.Skip(1)));
            Assert.That(many[keys[1]], Is.EqualTo(new byte[] { 2 }));
            Assert.That(originalStore.Latest, Is.SameAs(originalSnapshot));
            Assert.That(originalStore.SaveCount, Is.EqualTo(originalSaveCount));
            Assert.That(originalState.State.ProjectionCheckpointOffset, Is.EqualTo(evidence.Count));
            Assert.That(stateSerializer.SerializeToArray(originalState.State), Is.EqualTo(originalStateBytes));
            Assert.That(evidence[2].Mutation.ShardIndex, Is.Zero);
            Assert.That(evidence[2].Mutation.Timestamp, Is.EqualTo(deletion.Timestamp));
        });
        Assert.That(await rebuilt.GetAsync(keys[0]), laterRecreation ? Is.EqualTo(new byte[] { 3 }) : Is.Null);
        var versioned = await rebuilt.GetWithVersionAsync(keys[0]);
        Assert.That(versioned.Version.WallClockTicks, Is.EqualTo(laterRecreation ? 300 : 0));
        Assert.That(await rebuilt.GetAsync(foreignKey), Is.Null);
        var (foreign, _, _, _) = CreateGrainWithMaterialiser(coordinator, treeId: ResumableTreeId,
            seedState: s => s.ShardIndex = map.Resolve(foreignKey), registryShardMap: map);
        await ActivateAsync(foreign);
        Assert.That(await foreign.GetAsync(foreignKey), Is.EqualTo(new byte[] { 9 }));
        Assert.That(await foreign.GetAsync(keys[0]), Is.Null);
        Assert.That(rebuiltStore.Latest, Is.Not.Null);
        Assert.That(rebuiltStore.Latest!.ScalarOffsetOrSentinel(), Is.EqualTo(evidence.Count));
        var verifiedStore = new InMemorySnapshotStore(rebuiltStore.Latest);
        var verifiedState = NewResumableState();
        verifiedState.State = stateSerializer.Deserialize(stateSerializer.SerializeToArray(rebuiltState.State));
        var snapshotOnly = BuildChunkingCoordinator(evidence.Count + 1, sliceSize: 8, tail: evidence.Count + 1);
        var (reopened, _) = BuildResumableLeaf(verifiedState, snapshotOnly, verifiedStore.Stub,
            reclassifyEveryN: 1, registryShardMap: map);
        await ActivateAsync(reopened);
        Assert.That(await reopened.GetManyAsync([.. keys, foreignKey]), Is.EquivalentTo(many));
        Assert.That(await reopened.GetAsync(keys[0]), laterRecreation ? Is.EqualTo(new byte[] { 3 }) : Is.Null);
        var reopenedDelta = await reopened.GetDeltaSinceAsync(new VersionVector());
        var reopenedEntry = reopenedDelta.Entries[keys[0]];
        Assert.Multiple(() =>
        {
            Assert.That(reopenedEntry.Timestamp, Is.EqualTo(delta.Entries[keys[0]].Timestamp));
            Assert.That(reopenedEntry.IsTombstone, Is.EqualTo(!laterRecreation));
            Assert.That(reopenedEntry.Value, laterRecreation ? Is.EqualTo(new byte[] { 3 }) : Is.Null);
        });
    }
}
