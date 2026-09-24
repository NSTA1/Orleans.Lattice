using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Group-commit coverage for the registry (issue #3475). Every test drives the
/// grain POCO on a single exclusive <see cref="TaskScheduler"/>, which is the
/// turn model an Orleans activation gives it: continuations never run in
/// parallel, but a call that is suspended on its state write lets the next
/// call start, exactly as an <c>[AlwaysInterleave]</c> method does.
/// </summary>
public partial class TxRegistryGrainTests
{
    /// <summary>Runs <paramref name="body"/> on <paramref name="scheduler"/>, so every continuation it schedules stays on the single-threaded turn.</summary>
    private static Task<T> OnTurnAsync<T>(TaskScheduler scheduler, Func<Task<T>> body) =>
        Task.Factory.StartNew(body, CancellationToken.None, TaskCreationOptions.None, scheduler).Unwrap();

    /// <summary>Non-generic overload of <see cref="OnTurnAsync{T}"/>.</summary>
    private static Task OnTurnAsync(TaskScheduler scheduler, Func<Task> body) =>
        Task.Factory.StartNew(body, CancellationToken.None, TaskCreationOptions.None, scheduler).Unwrap();

    [Test]
    public async Task MarkCommittedAsync_concurrent_calls_coalesce_into_at_most_two_serial_writes()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var state = new FakePersistentState<TxRegistryState>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () => gate.Task;
        var (grain, _) = CreateGrain(state);

        const int callers = 16;
        var txids = Enumerable.Range(0, callers).Select(_ => Guid.NewGuid()).ToArray();
        var calls = txids.Select(t => OnTurnAsync(turn, () => grain.MarkCommittedAsync(t))).ToArray();

        // Let every call reach its suspension point before any write completes.
        await OnTurnAsync(turn, () => Task.CompletedTask);
        gate.SetResult();
        await Task.WhenAll(calls);

        Assert.Multiple(() =>
        {
            Assert.That(state.MaxConcurrentWrites, Is.EqualTo(1),
                "The registry must never have more than one state write outstanding.");
            Assert.That(state.WriteCount, Is.LessThanOrEqualTo(2),
                "Mutations that arrive while a write is in flight must join the next write, not issue their own.");
            foreach (var txid in txids)
            {
                Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
            }
        });
    }
}
