using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Liveness coverage for the registry's group commit: hundreds of overlapping
/// saga lifecycles against a store with jittered latency and injected write
/// faults must all run to completion. A lost wakeup (a caller parked on a group
/// that is never written or never completed) presents as a stuck chain, and the
/// failure names the stage each stuck chain is parked in.
/// </summary>
public partial class TxRegistryGrainTests
{
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public async Task Overlapping_saga_lifecycles_under_jittered_faulting_storage_all_complete(int seed)
    {
        const int sagas = 256;
        const int readers = 16;
        var bound = TimeSpan.FromSeconds(60);

        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var state = new FakePersistentState<TxRegistryState>();
        var rng = new Random(seed);
        var rngLock = new object();
        var injectedFaults = 0;
        var writeAttempts = 0;
        state.BeforeWrite = async () =>
        {
            int delayMs;
            lock (rngLock)
            {
                delayMs = rng.Next(0, 4);
            }

            // Fault injection is deterministic in the write count (issue #3563):
            // group commit coalesces the run into few enough writes that a
            // per-write probability can inject none at all. The second write
            // always fails, then one in eleven after it; the latency jitter
            // stays seeded.
            var attempt = Interlocked.Increment(ref writeAttempts);
            var fault = attempt == 2 || (attempt > 2 && attempt % 11 == 0);
            if (delayMs > 0)
            {
                await Task.Delay(delayMs);
            }
            else
            {
                await Task.Yield();
            }
            if (fault)
            {
                Interlocked.Increment(ref injectedFaults);
                throw new IOException("Injected registry write fault.");
            }
        };
        var (grain, _) = CreateGrain(state);

        var stages = new string[sagas];
        using var done = new CancellationTokenSource();

        async Task Retry(int saga, string stage, Func<Task> call)
        {
            stages[saga] = stage;
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await OnTurnAsync(turn, call);
                    return;
                }
                catch (IOException) when (attempt < 200)
                {
                    // Callers retry a failed registry write; so does the saga.
                }
            }
        }

        async Task<T> RetryFor<T>(int saga, string stage, Func<Task<T>> call)
        {
            var result = default(T)!;
            await Retry(saga, stage, async () => result = await call());
            return result;
        }

        async Task Saga(int i)
        {
            var txid = Guid.NewGuid();
            var commit = i % 5 != 0;
            await Retry(i, "admission", () => grain.EnsureSagaAdmissionAsync());
            await Retry(i, "register-participants", () => grain.RegisterParticipantsAsync(txid, [i % 4, (i + 1) % 4]));
            await Retry(i, "register-participant", () => grain.RegisterParticipantAsync(txid, i % 4));
            await RetryFor(i, "status-before", () => grain.GetStatusAsync(txid));
            await Retry(i, "decide", () => commit ? grain.MarkCommittedAsync(txid) : grain.MarkAbortedAsync(txid));
            var many = await RetryFor(i, "status-many", () => grain.GetStatusManyAsync([txid]));
            Assert.That(many[txid], Is.EqualTo(commit ? TxStatus.Committed : TxStatus.Aborted),
                $"Saga {i} must read its own durable verdict.");
            await RetryFor(i, "participants", () => grain.GetParticipantsAsync(txid));
            await RetryFor(i, "recorded-status", () => grain.GetRecordedStatusAsync(txid));
            await Retry(i, "forget", () => grain.ForgetAsync(txid));
            stages[i] = "done";
        }

        async Task Reader(int r)
        {
            var pin = Guid.NewGuid();
            while (!done.IsCancellationRequested)
            {
                try
                {
                    await OnTurnAsync(turn, () => grain.SnapshotAsync());
                    await OnTurnAsync(turn, () => grain.SnapshotWithRevisionAsync());
                    await OnTurnAsync(turn, () => grain.GetDecisionsRevisionAsync());
                    await OnTurnAsync(turn, () => grain.ObserveCrossTreeInFlightAsync());
                    if (r % 4 == 0)
                    {
                        await OnTurnAsync(turn, () => grain.PinSnapshotAsync(pin, [Guid.NewGuid()], TimeSpan.FromSeconds(30)));
                        await OnTurnAsync(turn, () => grain.RefreshPinAsync(pin, TimeSpan.FromSeconds(30)));
                        await OnTurnAsync(turn, () => grain.UnpinSnapshotAsync(pin));
                    }
                }
                catch (IOException)
                {
                    // A reader that mutates (pins) sees the same injected faults.
                }
            }
        }

        var sagaTasks = Enumerable.Range(0, sagas).Select(i => Task.Run(() => Saga(i))).ToArray();
        var readerTasks = Enumerable.Range(0, readers).Select(r => Task.Run(() => Reader(r))).ToArray();

        var all = Task.WhenAll(sagaTasks);
        var finished = await Task.WhenAny(all, Task.Delay(bound));
        done.Cancel();

        if (finished != all)
        {
            var stuck = stages
                .Select((stage, i) => (stage, i))
                .Where(s => s.stage != "done")
                .GroupBy(s => s.stage)
                .Select(g => $"{g.Key}={g.Count()}");
            Assert.Fail(
                $"{stages.Count(s => s != "done")} of {sagas} saga lifecycles did not complete within {bound.TotalSeconds}s "
                + $"(parked stages: {string.Join(", ", stuck)}; writes={state.WriteCount}, injected faults={injectedFaults}).");
        }

        await all;
        await Task.WhenAll(readerTasks).WaitAsync(bound);

        Assert.Multiple(() =>
        {
            Assert.That(state.MaxConcurrentWrites, Is.EqualTo(1),
                "The registry must never have more than one state write outstanding.");
            Assert.That(injectedFaults, Is.GreaterThan(0),
                "The run must actually exercise the group-failure path.");
        });
    }
}
