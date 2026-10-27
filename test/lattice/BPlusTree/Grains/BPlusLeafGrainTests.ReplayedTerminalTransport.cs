using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Replayed_commit_persists_typed_prepares_before_apply_and_deduplicates_delivery(
        bool tombstone, bool marked)
    {
        var writer = Substitute.For<ICommitLogWriter>();
        var appended = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        WalRecord[] records = [];
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                records = call.Arg<IReadOnlyList<WalRecord>>().ToArray();
                appended.TrySetResult();
                await release.Task;
                return (IReadOnlyList<long>)Enumerable.Range(0, records.Length).Select(i => (long)i).ToArray();
            });
        var state = new FakePersistentState<LeafNodeState> { State = { ShardIndex = 7 } };
        var grain = CreateGrain(state, commitLog: writer);
        var txid = Guid.NewGuid();
        var stamp = new HybridLogicalClock { WallClockTicks = 9000, Counter = 7 };
        var vector = new VersionVector();
        vector.Entries["origin"] = stamp;
        var prepare = new PendingMutationSnapshot
        {
            TransactionId = txid,
            Key = "k",
            Value = tombstone ? null : Encoding.UTF8.GetBytes("committed"),
            Timestamp = stamp,
            IsTombstone = tombstone,
            ExpiresAtTicks = tombstone ? 0 : 1234567,
            OriginClusterId = "origin",
            VectorClock = vector,
            AtomicBatchSize = 4,
            AtomicBatchIndex = 2,
            StampIsOriginal = marked,
        };
        var delivery = grain.ApplyPreparedReplayCommitAsync(txid, [prepare], stamp);
        await appended.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            Assert.That(grain.EntriesForTest, Is.Empty, "No committed row may precede the destination WAL.");
            Assert.That(await grain.GetPendingKeysAsync(), Is.Empty);
        }
        finally
        {
            release.TrySetResult();
            await delivery;
        }
        await grain.ApplyPreparedReplayCommitAsync(txid, [prepare], stamp);

        var row = grain.EntriesForTest["k"];
        Assert.Multiple(() =>
        {
            Assert.That(records, Has.Length.EqualTo(2));
            Assert.That(records[0].Op, Is.EqualTo(tombstone ? MutationKind.Delete : MutationKind.Set));
            Assert.That(records[0].IsPrepared, Is.True);
            Assert.That(records[0].Timestamp, Is.EqualTo(stamp));
            Assert.That(records[0].PrepareStampOriginal, Is.EqualTo(marked));
            Assert.That(records[0].ExpiresAtTicks, Is.EqualTo(prepare.ExpiresAtTicks));
            Assert.That(records[0].OriginClusterId, Is.EqualTo("origin"));
            Assert.That(records[0].VectorClock!.Entries, Is.EquivalentTo(vector.Entries));
            Assert.That(records[0].AtomicBatchSize, Is.EqualTo(4));
            Assert.That(records[0].AtomicBatchIndex, Is.EqualTo(2));
            Assert.That(records[1].Op, Is.EqualTo(MutationKind.TxCommit));
            Assert.That(records[1].Key, Is.EqualTo("7"), "Terminals route by numeric shard, not point-key hash.");
            Assert.That(records.All(record => record.ShardIndex == 7), Is.True);
            Assert.That(records.All(record => record.TransactionId == txid), Is.True);
            Assert.That(row.IsTombstone, Is.EqualTo(tombstone));
            Assert.That(row.ExpiresAtTicks, Is.EqualTo(prepare.ExpiresAtTicks));
            Assert.That(row.Timestamp, Is.EqualTo(stamp), "Cross-origin prepared stamps remain unchanged.");
            Assert.That(row.OriginClusterId, Is.EqualTo("origin"));
            Assert.That(row.VectorClock!.Entries, Is.EquivalentTo(vector.Entries));
        });
        await writer.Received(1).AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Replayed_commit_retries_a_failed_WAL_append_without_installing_a_partial_bucket()
    {
        var writer = Substitute.For<ICommitLogWriter>();
        var attempts = 0;
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(call => ++attempts == 1
                ? Task.FromException<IReadOnlyList<long>>(new IOException("append unavailable"))
                : Task.FromResult<IReadOnlyList<long>>([0, 1]));
        var grain = CreateGrain(commitLog: writer);
        var txid = Guid.NewGuid();
        var stamp = new HybridLogicalClock { WallClockTicks = 9000 };
        var prepare = new PendingMutationSnapshot
        {
            TransactionId = txid, Key = "k", Value = [42], Timestamp = stamp,
        };
        Assert.ThrowsAsync<IOException>(() => grain.ApplyPreparedReplayCommitAsync(txid, [prepare], stamp));
        Assert.That(grain.EntriesForTest, Is.Empty);
        Assert.That(await grain.GetPendingKeysAsync(), Is.Empty);

        await grain.ApplyPreparedReplayCommitAsync(txid, [prepare], stamp);
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 42 }));
        Assert.That(attempts, Is.EqualTo(2));
    }

    [Test]
    public async Task Replayed_commit_keeps_CRDT_delta_and_rebuilds_the_same_state_from_its_WAL()
    {
        var writer = Substitute.For<ICommitLogWriter>();
        WalRecord[] records = [];
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                records = call.Arg<IReadOnlyList<WalRecord>>().ToArray();
                return Task.FromResult<IReadOnlyList<long>>(Enumerable.Range(0, records.Length).Select(i => (long)i).ToArray());
            });
        var grain = CreateBackstopJoinGrain(LatticeMergeMode.GCounter, writer);
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, GCounterDeltaBytes("B", 2));
        var existingStamp = grain.EntriesForTest["k"].Timestamp;
        var stamp = new HybridLogicalClock { WallClockTicks = 9000 };
        var prepare = new PendingMutationSnapshot
        {
            TransactionId = Guid.NewGuid(), Key = "k", Timestamp = stamp,
            Value = GCounterState(("A", 3)), Delta = GCounterDeltaBytes("A", 3),
            Mode = LatticeMergeMode.GCounter,
        };
        await grain.ApplyPreparedReplayCommitAsync(prepare.TransactionId, [prepare], stamp);
        var committed = (GCounter)CrdtShape.ForGCounter().DeserializeState(grain.EntriesForTest["k"].Value!);
        Assert.That(committed.Value, Is.EqualTo(5));
        Assert.That(grain.EntriesForTest["k"].Timestamp, Is.EqualTo(existingStamp with { Counter = existingStamp.Counter + 1 }),
            "The CRDT fold must dominate the recipient row even when its prepare stamp is older.");
        Assert.That(grain.CacheForTest.GetMergeMode("k"), Is.EqualTo(LatticeMergeMode.GCounter));
        Assert.That(records[0].Delta, Is.EqualTo(prepare.Delta));
        Assert.That(records[0].Mode, Is.EqualTo(LatticeMergeMode.GCounter));

        var recovered = CreateBackstopJoinGrain(LatticeMergeMode.GCounter);
        await recovered.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, GCounterDeltaBytes("B", 2));
        foreach (var record in records)
            ((ILeafProjection)recovered).Apply(WalRecordConverter.FromWalRecord(record));
        var rebuilt = (GCounter)CrdtShape.ForGCounter().DeserializeState(recovered.EntriesForTest["k"].Value!);
        Assert.That(rebuilt.Value, Is.EqualTo(5));
        await grain.ApplyPreparedReplayCommitAsync(prepare.TransactionId, [prepare], stamp);
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(recovered.EntriesForTest["k"].Value),
            "A repeated commit does not fold the same delta twice.");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Replayed_commit_follows_a_span_narrowed_during_its_WAL_append(bool exhaustBound)
    {
        var writer = Substitute.For<ICommitLogWriter>();
        var appended = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                appended.TrySetResult();
                await release.Task;
                return (IReadOnlyList<long>)new long[] { 0, 1 };
            });
        var successor = CreateGrain();
        var target = Substitute.For<IBPlusLeafGrain>();
        target.ApplyPreparedReplayCommitAsync(Arg.Any<Guid>(), Arg.Any<List<PendingMutationSnapshot>>(), Arg.Any<HybridLogicalClock>(), Arg.Any<int>())
            .Returns(call => successor.ApplyPreparedReplayCommitAsync(
                call.Arg<Guid>(), call.Arg<List<PendingMutationSnapshot>>(), call.Arg<HybridLogicalClock>(), call.Arg<int>()));
        var state = new FakePersistentState<LeafNodeState>
        {
            State = { NextSibling = GrainId.Create("leaf", "replayed-terminal-successor") },
        };
        var grain = CreateGrain(state, commitLog: writer, siblingStub: target);
        var stamp = new HybridLogicalClock { WallClockTicks = 9000 };
        var prepare = new PendingMutationSnapshot
        {
            TransactionId = Guid.NewGuid(), Key = "z", Timestamp = stamp, Value = [42],
        };
        var delivery = grain.ApplyPreparedReplayCommitAsync(
            prepare.TransactionId, [prepare], stamp, remainingForwards: exhaustBound ? 0 : 64);
        await appended.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            await grain.SetKeyRangeAsync(null, "m");
        }
        finally
        {
            release.TrySetResult();
            if (exhaustBound)
                Assert.ThrowsAsync<InvalidOperationException>(async () => await delivery);
            else
                await delivery;
        }
        if (exhaustBound)
        {
            Assert.That(await grain.GetPendingKeysAsync(), Is.EquivalentTo(new[] { "z" }));
            await target.DidNotReceive().ApplyPreparedReplayCommitAsync(
                Arg.Any<Guid>(), Arg.Any<List<PendingMutationSnapshot>>(), Arg.Any<HybridLogicalClock>(), Arg.Any<int>());
            await grain.ApplyPreparedReplayCommitAsync(prepare.TransactionId, [prepare], stamp);
        }
        Assert.That(grain.EntriesForTest, Is.Empty);
        Assert.That(successor.EntriesForTest["z"].Value, Is.EqualTo(new byte[] { 42 }));
        await target.Received(1).ApplyPreparedReplayCommitAsync(
            prepare.TransactionId, Arg.Any<List<PendingMutationSnapshot>>(), stamp, 63);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Replayed_commit_keeps_a_local_original_stamp_and_does_not_replace_a_newer_row(bool newerRow)
    {
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<long>>([0, 1]));
        var grain = CreateGrain(commitLog: writer);
        var stamp = new HybridLogicalClock { WallClockTicks = 9000, Counter = 7 };
        if (newerRow)
            ((ILeafProjection)grain).Apply(new LatticeMutation
            {
                Kind = MutationKind.Set, Key = "k", Value = [99], Timestamp = stamp with { Counter = 8 },
            });
        var prepare = new PendingMutationSnapshot
        {
            TransactionId = Guid.NewGuid(), Key = "k", Timestamp = stamp, Value = [42],
            ExpiresAtTicks = 1234567, StampIsOriginal = true,
        };
        await grain.ApplyPreparedReplayCommitAsync(prepare.TransactionId, [prepare], stamp);
        await grain.ApplyPreparedReplayCommitAsync(prepare.TransactionId, [prepare], stamp);
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { newerRow ? (byte)99 : (byte)42 }));
        Assert.That(grain.EntriesForTest["k"].Timestamp, Is.EqualTo(newerRow ? stamp with { Counter = 8 } : stamp));
        if (newerRow)
            await writer.DidNotReceive().AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
        else
        {
            Assert.That(grain.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(prepare.ExpiresAtTicks));
            await writer.Received(1).AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
        }
    }

    [Test]
    public async Task Replayed_commit_hydrates_the_durable_per_key_witness_before_accepting_redelivery()
    {
        var sidecar = new FakeLeafTerminalWitnessGrain();
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<long>>([0, 1]));
        var state = new FakePersistentState<LeafNodeState>();
        var replica = Guid.NewGuid().ToString("N");
        BPlusLeafGrain NewActivation() => CreateGrain(
            state, replicaId: replica, commitLog: writer,
            configureGrainFactory: factory => factory
                .GetGrain<ILeafTerminalWitnessGrain>(Arg.Any<Guid>(), Arg.Any<string?>()).Returns(sidecar));
        var first = NewActivation();
        var prepare = new PendingMutationSnapshot
        {
            TransactionId = Guid.NewGuid(), Key = "k",
            Timestamp = new HybridLogicalClock { WallClockTicks = 9000 }, Value = [42],
        };
        await first.ApplyPreparedReplayCommitAsync(prepare.TransactionId, [prepare], prepare.Timestamp);
        await first.FlushTerminalWitnessAsync();
        Assert.That(sidecar.Holds(prepare.TransactionId, "k"), Is.True);

        var recovered = NewActivation();
        recovered.EntriesForTest["k"] = first.EntriesForTest["k"];
        await recovered.ApplyPreparedReplayCommitAsync(prepare.TransactionId, [prepare], prepare.Timestamp);
        Assert.That(recovered.EntriesForTest["k"], Is.EqualTo(first.EntriesForTest["k"]));
        await writer.Received(1).AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Replayed_commit_keeps_a_moved_prepare_when_no_sibling_route_is_available()
    {
        var state = new FakePersistentState<LeafNodeState> { State = { HighKeyExclusive = "m" } };
        var grain = CreateGrain(state);
        var prepare = new PendingMutationSnapshot
        {
            TransactionId = Guid.NewGuid(), Key = "z", Value = [42],
            Timestamp = new HybridLogicalClock { WallClockTicks = 9000 },
        };
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ApplyPreparedReplayCommitAsync(
            prepare.TransactionId, [prepare], prepare.Timestamp));
        Assert.That(grain.EntriesForTest, Is.Empty, "A missing route is not permission to resurrect an out-of-span row.");
        Assert.That(await grain.GetPendingKeysAsync(), Is.EquivalentTo(new[] { "z" }));
    }

    [Test]
    public async Task Replayed_commit_rejects_invalid_transaction_or_forwarding_inputs_before_appending()
    {
        var writer = Substitute.For<ICommitLogWriter>();
        var grain = CreateGrain(commitLog: writer);
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.ApplyPreparedReplayCommitAsync(Guid.NewGuid(), null!, default));
        Assert.ThrowsAsync<ArgumentException>(() => grain.ApplyPreparedReplayCommitAsync(Guid.Empty, [], default));
        Assert.ThrowsAsync<ArgumentException>(() => grain.ApplyPreparedReplayCommitAsync(
            Guid.NewGuid(), [new PendingMutationSnapshot { TransactionId = Guid.NewGuid(), Key = "k" }], default));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ApplyPreparedReplayCommitAsync(
            Guid.NewGuid(), [], default, remainingForwards: -1));
        await writer.DidNotReceive().AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }
}
