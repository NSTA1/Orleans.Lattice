using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task GetManyAsync_original_reader_resolves_at_deadline_before_stalled_gate_delivery()
    {
        var h = CreateGetManyProgressHarness();
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.AcquireBarrier = barrier;
        var read = h.Lattice.GetManyAsync(h.Keys);
        Assert.That(h.OptimisticReads, Is.EqualTo(3), "the real fallback reached the held acquisition");
        Assert.That(read.IsCompleted, Is.False);
        try
        {
            h.Clock.Advance(LatticeGrain.GetManyDecisionGateLease);
            using var watchdog = new CancellationTokenSource();
            var expiredWatchdog = Task.Delay(TimeSpan.FromSeconds(5), watchdog.Token);
            var winner = await Task.WhenAny(read, expiredWatchdog);
            await watchdog.CancelAsync();
            Assert.That(winner, Is.SameAs(read),
                "[ReaderEventuallyResolves] the original caller must resolve before late gate delivery");
            Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(() => read);
        }
        finally
        {
            barrier.TrySetResult();
            await TestPoll.UntilAsync(() => h.RegistryProxy.ReceivedCalls()
                .Any(call => call.GetMethodInfo().Name == nameof(ITxRegistryGrain.ReleaseCaptureGateAsync)),
                "late acquisition released");
        }
        await h.Registry.MarkCommittedAsync(h.Transaction);
    }
}
