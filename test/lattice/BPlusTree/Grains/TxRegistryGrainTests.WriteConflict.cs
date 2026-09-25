using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// How a failed registry state write reaches its callers (issue #3501). The
/// storage fault is translated to <see cref="TxRegistryWriteFailedException"/>
/// so a provider exception type never crosses a grain boundary; an
/// optimistic-concurrency conflict additionally deactivates the registry so the
/// retry reloads its row. A mutation that interleaves with a shard's first
/// high-water raise joins the group the raise precedes, so it is never carried
/// by a write it was not recorded against.
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public void A_write_conflict_is_translated_and_deactivates_the_registry()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", "tree-x"));
        var (grain, state) = CreateGrain(context: context);
        state.ThrowOnWrite = new InconsistentStateException("etag mismatch", "stored", "current");
        var txid = Guid.NewGuid();

        var ex = Assert.ThrowsAsync<TxRegistryWriteFailedException>(() => grain.MarkCommittedAsync(txid));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Conflict, Is.True);
            Assert.That(ex.RegistryKey, Is.EqualTo("tree-x"));
            Assert.That(ex.FaultType, Is.EqualTo(typeof(InconsistentStateException).FullName));
            Assert.That(ex.InnerException, Is.Null,
                "The provider exception is summarised, never carried, so a client need not load its type.");
            Assert.That(state.State.Decisions, Does.Not.ContainKey(txid));
        });
        context.ReceivedWithAnyArgs().Deactivate(default!);
    }

    [Test]
    public void A_non_conflict_write_failure_is_translated_without_deactivating()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", "tree-x~s4"));
        var (grain, state) = CreateGrain(treeId: "tree-x~s4", context: context, grainFactory: RaisingFactory());
        state.ThrowOnWrite = new IOException("storage unavailable");

        var ex = Assert.ThrowsAsync<TxRegistryWriteFailedException>(() => grain.MarkAbortedAsync(Guid.NewGuid()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Conflict, Is.False);
            Assert.That(ex.RegistryKey, Is.EqualTo("tree-x~s4"));
            Assert.That(ex.FaultType, Is.EqualTo(typeof(IOException).FullName));
            Assert.That(ex.Message, Does.Contain("storage unavailable"));
        });
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
    }

    [Test]
    public void IsWriteConflict_recognises_a_conflict_wrapped_by_another_exception()
    {
        var wrapped = new InvalidOperationException("outer", new InconsistentStateException("etag mismatch"));

        Assert.Multiple(() =>
        {
            Assert.That(TxRegistryGrain.IsWriteConflict(new InconsistentStateException("etag mismatch")), Is.True);
            Assert.That(TxRegistryGrain.IsWriteConflict(wrapped), Is.True);
            Assert.That(TxRegistryGrain.IsWriteConflict(new IOException("disk")), Is.False);
        });
    }

    [Test]
    public void A_failed_high_water_raise_never_deactivates_the_registry_even_on_a_conflict()
    {
        // A conflict on the high-water grain's own row is that grain's to
        // handle; the registry's row was never written, so it stays valid.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", "tree-x~s1"));
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        highWater.RaiseShardHighWaterAsync(Arg.Any<int>())
            .Returns(Task.FromException<int>(new InconsistentStateException("high-water etag")));
        factory.GetGrain<ITxRegistryHighWaterGrain>("tree-x").Returns(highWater);
        var (grain, state) = CreateGrain(treeId: "tree-x~s1", context: context, grainFactory: factory);

        var ex = Assert.ThrowsAsync<TxRegistryWriteFailedException>(() => grain.MarkCommittedAsync(Guid.NewGuid()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Conflict, Is.False);
            Assert.That(state.WriteCount, Is.Zero);
        });
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
    }

    [Test]
    public async Task A_mutation_applied_during_the_first_high_water_raise_joins_the_group_the_raise_precedes()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var raise = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        highWater.RaiseShardHighWaterAsync(Arg.Any<int>()).Returns(raise.Task);
        factory.GetGrain<ITxRegistryHighWaterGrain>("tree-x").Returns(highWater);
        var state = new FakePersistentState<TxRegistryState>();
        var writes = 0;
        state.BeforeWrite = () =>
        {
            // Any write after the first fails. Were the interleaved mutation
            // carried by the first write but recorded against a second group,
            // that second write would fail and roll back a durable decision.
            if (++writes > 1) state.ThrowOnWrite = new IOException("second write");
            return Task.CompletedTask;
        };
        var (grain, _) = CreateGrain(state: state, treeId: "tree-x~s3", grainFactory: factory);
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        var markA = OnTurnAsync(turn, () => grain.MarkCommittedAsync(a));
        await DrainAsync(turn);
        var markB = OnTurnAsync(turn, () => grain.MarkAbortedAsync(b));
        await DrainAsync(turn);
        Assume.That(markA.IsCompleted || markB.IsCompleted, Is.False, "Both mutations wait on the outstanding raise.");
        raise.SetResult(4);
        await Task.WhenAll(markA, markB);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1), "Both mutations ride the one write the raise preceded.");
            Assert.That(state.State.Decisions[a], Is.EqualTo(TxStatus.Committed));
            Assert.That(state.State.Decisions[b], Is.EqualTo(TxStatus.Aborted));
        });
        await highWater.Received(1).RaiseShardHighWaterAsync(4);
    }

    [Test]
    public async Task A_mutation_applied_during_a_failing_high_water_raise_fails_with_it_and_is_rolled_back()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var raise = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        highWater.RaiseShardHighWaterAsync(Arg.Any<int>()).Returns(raise.Task, Task.FromResult(4));
        factory.GetGrain<ITxRegistryHighWaterGrain>("tree-x").Returns(highWater);
        var (grain, state) = CreateGrain(treeId: "tree-x~s3", grainFactory: factory);
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        var markA = OnTurnAsync(turn, () => grain.MarkCommittedAsync(a));
        await DrainAsync(turn);
        var markB = OnTurnAsync(turn, () => grain.MarkCommittedAsync(b));
        await DrainAsync(turn);
        raise.SetException(new IOException("high-water down"));

        Assert.Multiple(() =>
        {
            Assert.That(Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await markA)!.Message,
                Does.Contain("high-water down"));
            Assert.That(Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await markB)!.Message,
                Does.Contain("high-water down"));
            Assert.That(state.WriteCount, Is.Zero);
            Assert.That(state.State.Decisions, Is.Empty, "Nothing is written before the shard's index is durable.");
        });

        await OnTurnAsync(turn, () => grain.MarkCommittedAsync(b));
        Assert.That(state.State.Decisions.Keys, Is.EquivalentTo(new[] { b }));
    }

    private static IGrainFactory RaisingFactory()
    {
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        highWater.RaiseShardHighWaterAsync(Arg.Any<int>()).Returns(ci => Task.FromResult(ci.Arg<int>()));
        factory.GetGrain<ITxRegistryHighWaterGrain>(Arg.Any<string>()).Returns(highWater);
        return factory;
    }
}
