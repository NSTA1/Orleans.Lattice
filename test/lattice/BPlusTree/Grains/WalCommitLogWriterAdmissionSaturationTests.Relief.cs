using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalCommitLogWriterAdmissionSaturationTests
{
    [TestCase(MutationKind.TxCommit, 0, WalSaturationState.Saturated)]
    [TestCase(MutationKind.TxAbort, 0, WalSaturationState.Saturated)]
    [TestCase(MutationKind.TxCommit, 1, WalSaturationState.Saturated)]
    [TestCase(MutationKind.TxAbort, 1, WalSaturationState.Saturated)]
    [TestCase(MutationKind.TxCommit, 2, WalSaturationState.Saturated)]
    [TestCase(MutationKind.TxAbort, 2, WalSaturationState.Saturated)]
    [TestCase(MutationKind.TxCommit, 0, WalSaturationState.Throttled)]
    [TestCase(MutationKind.TxAbort, 0, WalSaturationState.Throttled)]
    [TestCase(MutationKind.TxCommit, 1, WalSaturationState.Throttled)]
    [TestCase(MutationKind.TxAbort, 1, WalSaturationState.Throttled)]
    [TestCase(MutationKind.TxCommit, 2, WalSaturationState.Throttled)]
    [TestCase(MutationKind.TxAbort, 2, WalSaturationState.Throttled)]
    public async Task Terminal_relief_bypasses_saturation_gate_in_single_and_batch_paths(
        MutationKind kind, int batchSize, WalSaturationState state)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(state);
        signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromCanceled(new CancellationToken(true)));
        var shard = CreateHealthyShard();
        var writer = CreateWriter(shard, signal, new LatticeOptions
        {
            WalBatchedSingleEntryAppends = true,
            WalThrottledAdmissionPace = TimeSpan.FromSeconds(1),
        });
        var terminal = MakeMutation("0") with { Op = kind, TransactionId = Guid.NewGuid() };

        if (batchSize > 0)
            await writer.AppendManyAsync(Enumerable.Range(0, batchSize)
                .Select(i => terminal with { Key = i.ToString(System.Globalization.CultureInfo.InvariantCulture) }).ToArray());
        else
            await writer.AppendAsync(terminal);

        signal.DidNotReceive().GetCurrentState(Arg.Any<string>());
        await signal.DidNotReceive().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(shard.ReceivedCalls().Count(call =>
            call.GetMethodInfo().Name is nameof(IWalShardGrain.AppendAsync) or nameof(IWalShardGrain.AppendBatchAsync)),
            Is.GreaterThan(0), "terminal records must reach storage while their prepared writes hold progress");
    }

    [TestCase(false, 1)]
    [TestCase(true, 1)]
    [TestCase(false, 2)]
    [TestCase(true, 2)]
    public async Task Mixed_data_and_terminal_batch_remains_saturation_gated(bool terminalFirst, int partitions)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromCanceled(new CancellationToken(true)));
        var shard = CreateHealthyShard();
        var writer = CreateWriter(shard, signal, new LatticeOptions { WalPartitions = partitions });
        var terminal = MakeMutation("0") with { Op = MutationKind.TxCommit, TransactionId = Guid.NewGuid() };
        var data = MakeMutation();
        if (partitions > 1)
            data = data with
            {
                Key = Enumerable.Range(0, 1000).Select(i => $"data{i}").First(key =>
                    WalPartitionHash.Compute(key, partitions) != WalPartitionHash.Compute(terminal.Key, partitions)),
            };
        WalRecord[] entries = terminalFirst ? [terminal, data] : [data, terminal];

        var error = Assert.ThrowsAsync<LatticeSaturatedException>(() => writer.AppendManyAsync(entries));

        Assert.That(error!.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission));
        await shard.DidNotReceive().AppendBatchAsync(
            Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
        await shard.DidNotReceive().AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Terminal_relief_still_observes_writer_shutdown(bool batch)
    {
        var writer = CreateWriter(CreateHealthyShard(), new WalSaturationSignal());
        await writer.DrainAsync(CancellationToken.None);
        var terminal = MakeMutation("0") with { Op = MutationKind.TxCommit, TransactionId = Guid.NewGuid() };

        Assert.ThrowsAsync<LatticeShuttingDownException>(async () =>
        {
            if (batch)
                await writer.AppendManyAsync([terminal, terminal]);
            else
                await writer.AppendAsync(terminal);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Terminal_relief_preserves_partition_capacity_and_storage_dispatch(bool batch)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        var shard = CreateHealthyShard();
        var firstStorage = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstBatchStorage = new TaskCompletionSource<IReadOnlyList<long>>(TaskCreationOptions.RunContinuationsAsynchronously);
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(firstStorage.Task, Task.FromResult(2L));
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(firstBatchStorage.Task, Task.FromResult<IReadOnlyList<long>>([2L]));
        var writer = CreateWriter(shard, signal, new LatticeOptions
        {
            WalPartitions = 1, WalMaxPendingBatches = 1, WalBatchedSingleEntryAppends = true,
        });
        var terminal = MakeMutation("0") with { Op = MutationKind.TxCommit, TransactionId = Guid.NewGuid() };
        Task First() => batch ? writer.AppendManyAsync([terminal]) : writer.AppendAsync(terminal);
        var first = First();
        var second = First();
        try
        {
            Assert.That(first.IsCompleted, Is.False, "relief still awaits durable storage acknowledgement");
            Assert.That(second.IsCompleted, Is.False, "relief cannot manufacture admission capacity");
            Assert.That(shard.ReceivedCalls().Count(call =>
                call.GetMethodInfo().Name is nameof(IWalShardGrain.AppendAsync) or nameof(IWalShardGrain.AppendBatchAsync)),
                Is.EqualTo(1), "only one terminal dispatch may occupy a capacity-one partition");
        }
        finally
        {
            firstStorage.TrySetResult(1L);
            firstBatchStorage.TrySetResult([1L]);
            await Task.WhenAll(first, second);
        }
        signal.DidNotReceive().GetCurrentState(Arg.Any<string>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Terminal_relief_preserves_caller_cancellation(bool batch)
    {
        var writer = CreateWriter(CreateHealthyShard(), new WalSaturationSignal());
        var terminal = MakeMutation("0") with { Op = MutationKind.TxAbort, TransactionId = Guid.NewGuid() };
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            if (batch)
                await writer.AppendManyAsync([terminal], cancellation.Token);
            else
                await writer.AppendAsync(terminal, cancellation.Token);
        });
    }

    [TestCase(MutationKind.TxCommit, false)]
    [TestCase(MutationKind.TxAbort, false)]
    [TestCase(MutationKind.TxCommit, true)]
    [TestCase(MutationKind.TxAbort, true)]
    public void Terminal_relief_preserves_provider_failure(MutationKind kind, bool batch)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        var shard = CreateHealthyShard();
        var failure = new IOException("provider rejected terminal");
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(failure));
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<long>>(failure));
        var writer = CreateWriter(shard, signal, new LatticeOptions { WalBatchedSingleEntryAppends = true });
        var terminal = MakeMutation("0") with { Op = kind, TransactionId = Guid.NewGuid() };

        var error = Assert.ThrowsAsync<IOException>(async () =>
        {
            if (batch)
                await writer.AppendManyAsync([terminal]);
            else
                await writer.AppendAsync(terminal);
        });

        Assert.That(error, Is.SameAs(failure));
        signal.DidNotReceive().GetCurrentState(Arg.Any<string>());
    }

    [TestCase(MutationKind.TxCommit, false)]
    [TestCase(MutationKind.TxAbort, false)]
    [TestCase(MutationKind.TxCommit, true)]
    [TestCase(MutationKind.TxAbort, true)]
    public async Task Terminal_relief_preserves_storage_dispatch_deadline(MutationKind kind, bool batch)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        var shard = CreateHealthyShard();
        var storage = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var batchStorage = new TaskCompletionSource<IReadOnlyList<long>>(TaskCreationOptions.RunContinuationsAsynchronously);
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(storage.Task);
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>()).Returns(batchStorage.Task);
        var writer = CreateWriter(shard, signal, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
            WalBatchedSingleEntryAppends = true,
        });
        var terminal = MakeMutation("0") with { Op = kind, TransactionId = Guid.NewGuid() };
        try
        {
            Task append = batch ? writer.AppendManyAsync([terminal]) : writer.AppendAsync(terminal);
            await Task.WhenAny(append, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.That(append.IsCompleted, Is.True, "a terminal still has a storage dispatch deadline");
            Assert.ThrowsAsync<TimeoutException>(async () => await append);
            Assert.That(shard.ReceivedCalls().Count(call =>
                call.GetMethodInfo().Name is nameof(IWalShardGrain.AppendAsync) or nameof(IWalShardGrain.AppendBatchAsync)),
                Is.EqualTo(1), "the deadline must protect a real storage dispatch, not an admission park");
        }
        finally
        {
            storage.TrySetResult(0);
            batchStorage.TrySetResult([0L]);
        }
        signal.DidNotReceive().GetCurrentState(Arg.Any<string>());
    }
}
