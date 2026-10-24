using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>Explores cancellation, arrival and paced recovery in the production signal.</summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalAdmissionCoyoteTests
{
    [TestCase(1)]
    [TestCase(2)]
    public void Recovery_preserves_FIFO_pacing_isolation_and_resolves_the_residue(int batch)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new RecoveryModel(batch));
    }

    [Test]
    public async Task Released_signal_waiter_still_waits_for_partition_capacity()
    {
        const string tree = "released-is-not-admitted";
        var signal = new WalSaturationSignal();
        var tracker = new WalCommitLogWriter.PartitionTracker(tree, 0);
        using var cancellation = new CancellationTokenSource();
        Task<double>? follower = null;
        await tracker.AcquireAsync(1, Timeout.InfiniteTimeSpan, cancellation.Token, CancellationToken.None);
        try
        {
            signal.UpdatePartitionState(tree, 0, WalSaturationState.Saturated);
            var first = signal.WaitForNotSaturatedAsync(tree, 0, cancellation.Token);
            var second = signal.WaitForNotSaturatedAsync(tree, 0, cancellation.Token);
            signal.UpdatePartitionState(tree, 0, WalSaturationState.Throttled, 1);
            Assert.That(first.IsCompletedSuccessfully, Is.True);
            Assert.That(second.IsCompleted, Is.False);
            follower = tracker.AcquireAsync(1, Timeout.InfiniteTimeSpan, cancellation.Token, CancellationToken.None);
            Assert.That(follower.IsCompleted, Is.False, "a signal release cannot manufacture semaphore capacity");
        }
        finally
        {
            tracker.ReleaseAdmission();
            if (follower is not null)
            {
                await follower;
                tracker.ReleaseAdmission();
            }
            signal.ResetForTesting();
        }
    }

    private sealed class RecoveryModel(int batch) : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var signal = new WalSaturationSignal();
            const string tree = "coyote-admission";
            const int count = 4;
            var waits = new Task[2, count];
            var cancellations = new CancellationTokenSource[2, count];
            try
            {
                for (var p = 0; p < 2; p++)
                {
                    signal.UpdatePartitionState(tree, p, WalSaturationState.Saturated);
                    for (var c = 0; c < count; c++)
                    {
                        cancellations[p, c] = new CancellationTokenSource();
                        waits[p, c] = signal.WaitForNotSaturatedAsync(tree, p, cancellations[p, c].Token);
                    }
                }

                // Finite environment: every partition eventually gets its ticks,
                // without assuming any particular order between partitions.
                var ticks = new int[2];
                while (ticks[0] < count || ticks[1] < count)
                {
                    var p = ticks[0] == count ? 1 : ticks[1] == count ? 0 : runtime.RandomBoolean() ? 0 : 1;
                    var cancelled = (int)runtime.RandomInteger(count);
                    if (runtime.RandomBoolean())
                        cancellations[p, cancelled].Cancel();

                    var pending = Enumerable.Range(0, count).Where(c => !waits[p, c].IsCompleted).ToArray();
                    var other = Enumerable.Range(0, count).Select(c => waits[1 - p, c].Status).ToArray();
                    signal.UpdatePartitionState(tree, p, WalSaturationState.Throttled, batch);
                    for (var i = 0; i < pending.Length; i++)
                    {
                        Specification.Assert(waits[p, pending[i]].IsCompletedSuccessfully == (i < batch),
                            "[OldestFirst/PacedRecoveryRelease/BoundedResidueDrain] release exactly the oldest live batch");
                    }
                    for (var c = 0; c < count; c++)
                        Specification.Assert(waits[1 - p, c].Status == other[c],
                            "[PerPartitionIsolation] a tick cannot consume a sibling's release quota");
                    ticks[p]++;
                }

                foreach (var wait in waits)
                    Specification.Assert(wait.IsCompleted, "[EveryWaiterResolves] steady recovery must drain the residue");
            }
            finally
            {
                foreach (var cancellation in cancellations)
                    cancellation?.Dispose();
                signal.ResetForTesting();
            }
        }
    }
}
