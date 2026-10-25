using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

public sealed partial class ReplicationTreeFrontierGrainTests
{
    [TestCase(0, 1)]
    [TestCase(0, 4)]
    [TestCase(0, 40)]
    [TestCase(1, 1)]
    [TestCase(1, 4)]
    [TestCase(1, 40)]
    [TestCase(2, 1)]
    [TestCase(2, 4)]
    [TestCase(2, 40)]
    public async Task Origin_waves_overlap_with_a_bound_and_preserve_publication_barriers(int phase, int count)
    {
        var (h, grain, watermarks, held) = CreateOriginWave(count, phase);
        var before = h.State.State.Epoch;
        var writes = h.State.WriteCount;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        var active = 0;
        var peak = 0;
        var censusLock = new object();
        ConfigureOriginWave(h, count, phase, async _ =>
        {
            if (phase == 2)
                Assert.That(h.State.State.ExportHeldOrigins, Is.EquivalentTo(held.Keys),
                    "every held set must settle before any cap rises");
            lock (censusLock)
            {
                entered++;
                active++;
                peak = Math.Max(peak, active);
            }
            await release.Task;
            lock (censusLock) active--;
        });

        var operation = RunOriginWaveAsync(grain, before, watermarks, held, phase, CancellationToken.None);
        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(entered, Is.EqualTo(Math.Min(count, BoundedFanOut.DefaultWidth)));
                Assert.That(peak, Is.EqualTo(Math.Min(count, BoundedFanOut.DefaultWidth)));
                Assert.That(operation.IsCompleted, Is.False);
                Assert.That(h.State.WriteCount, Is.EqualTo(writes));
                Assert.That(h.State.State.Epoch, Is.EqualTo(before));
            });
            await h.Hwm.DidNotReceive().ResetAppliedIdentitiesAsync(Arg.Any<CancellationToken>(), Arg.Any<bool>());
            if (phase == 1)
            {
                Assert.That(h.State.State.ExportHeldOrigins, Is.Empty);
                Assert.That(h.State.State.Origins.Values.All(entry => entry.AwaitingPin), Is.True);
                for (var i = 0; i < count; i++)
                {
                    await h.Factory.GetGrain<IReplicationOriginFrontierGrain>($"origin-{i}")
                        .DidNotReceive().SetTreeCapAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
                }
            }
        }
        finally
        {
            release.TrySetResult();
            await operation.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.EqualTo(count), "one call per origin, including the ragged final wave");
            Assert.That(peak, Is.EqualTo(Math.Min(count, BoundedFanOut.DefaultWidth)));
            Assert.That(active, Is.Zero);
            Assert.That(h.State.WriteCount, Is.GreaterThan(writes));
            if (phase == 0)
            {
                Assert.That(h.State.State.Epoch, Is.Not.EqualTo(before));
                Assert.That(h.State.State.Origins.Values.All(entry => entry.AwaitingPin && entry.LowWatermark == HybridLogicalClock.Zero), Is.True);
            }
            else
            {
                Assert.That(h.State.State.Origins.Values.All(entry => !entry.AwaitingPin && entry.LowWatermark == Hlc(30)), Is.True);
                Assert.That(h.State.State.ExportHeldOrigins, Is.EquivalentTo(held.Keys));
            }
        });
        if (phase == 0)
            await h.Hwm.Received(1).ResetAppliedIdentitiesAsync(CancellationToken.None, false);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public async Task Origin_wave_failure_settles_launched_siblings_without_publishing_or_starting_another_wave(int phase)
    {
        var (h, grain, watermarks, held) = CreateOriginWave(40, phase);
        var before = h.State.State.Epoch;
        var writes = h.State.WriteCount;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        ConfigureOriginWave(h, 40, phase, slot =>
        {
            Interlocked.Increment(ref entered);
            return slot == 0 ? Task.FromException(new InvalidOperationException("origin unavailable")) : release.Task;
        });

        var operation = RunOriginWaveAsync(grain, before, watermarks, held, phase, CancellationToken.None);
        try
        {
            Assert.That(operation.IsCompleted, Is.False, "the failed wave must settle its launched siblings");
        }
        finally
        {
            release.TrySetResult();
        }
        Assert.That(async () => await operation.WaitAsync(TimeSpan.FromSeconds(5)),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("origin unavailable"));
        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.EqualTo(BoundedFanOut.DefaultWidth));
            Assert.That(h.State.WriteCount, Is.EqualTo(writes));
            Assert.That(h.State.State.Epoch, Is.EqualTo(before));
        });
        await h.Hwm.DidNotReceive().ResetAppliedIdentitiesAsync(Arg.Any<CancellationToken>(), Arg.Any<bool>());
        if (phase == 1)
            Assert.That(h.State.State.Origins.Values.All(entry => entry.AwaitingPin), Is.True);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public async Task Origin_wave_cancellation_settles_inflight_calls_and_does_not_dispatch_the_next_wave(int phase)
    {
        var (h, grain, watermarks, held) = CreateOriginWave(40, phase);
        var before = h.State.State.Epoch;
        var writes = h.State.WriteCount;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var entered = 0;
        ConfigureOriginWave(h, 40, phase, _ =>
        {
            Interlocked.Increment(ref entered);
            return release.Task;
        });
        var operation = RunOriginWaveAsync(grain, before, watermarks, held, phase, cancellation.Token);
        cancellation.Cancel();
        release.SetResult();

        Assert.That(async () => await operation.WaitAsync(TimeSpan.FromSeconds(5)), Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.EqualTo(BoundedFanOut.DefaultWidth));
            Assert.That(h.State.WriteCount, Is.EqualTo(writes));
            Assert.That(h.State.State.Epoch, Is.EqualTo(before));
        });
    }

    private static (Harness Harness, ReplicationTreeFrontierGrain Grain,
        Dictionary<string, HybridLogicalClock> Watermarks, Dictionary<string, HybridLogicalClock[]> Held)
        CreateOriginWave(int count, int phase)
    {
        var h = new Harness(RegistryLineage);
        h.State.State.Epoch = Guid.NewGuid();
        h.State.State.ObservedRegistryLineage = RegistryLineage;
        var watermarks = new Dictionary<string, HybridLogicalClock>();
        var held = new Dictionary<string, HybridLogicalClock[]>();
        for (var i = 0; i < count; i++)
        {
            var origin = $"origin-{i}";
            h.State.State.Origins[origin] = new ReplicationTreeOriginFrontier
            {
                AwaitingPin = phase != 0,
                Capped = true,
                LowWatermark = Hlc(10),
            };
            watermarks[origin] = Hlc(30);
            held[origin] = [Hlc(12)];
        }
        return (h, h.Activate(), watermarks, held);
    }

    private static void ConfigureOriginWave(Harness h, int count, int phase, Func<int, Task> call)
    {
        for (var i = 0; i < count; i++)
        {
            var slot = i;
            var origin = Substitute.For<IReplicationOriginFrontierGrain>();
            h.Factory.GetGrain<IReplicationOriginFrontierGrain>($"origin-{i}", Arg.Any<string?>()).Returns(origin);
            if (phase == 1)
                origin.SetHeldAsync(Arg.Any<string>(), Arg.Any<IReadOnlyCollection<HybridLogicalClock>>(), Arg.Any<CancellationToken>())
                    .Returns(_ => call(slot));
            else
                origin.SetTreeCapAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
                    .Returns(_ => call(slot));
        }
    }

    private static async Task RunOriginWaveAsync(
        ReplicationTreeFrontierGrain grain, Guid epoch,
        Dictionary<string, HybridLogicalClock> watermarks, Dictionary<string, HybridLogicalClock[]> held,
        int phase, CancellationToken cancellationToken)
    {
        if (phase == 0)
            await grain.OnContentsReplacingAsync(cancellationToken);
        else
            Assert.That(await grain.PinAsync(epoch, watermarks, held, cancellationToken), Is.True);
    }
}
