using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Issue #6036: a partition whose readable head says entries are waiting past
/// the checkpoint, but whose read returns nothing, was skipped silently on every
/// drain, stalling each consumer of it with no signal. The subscriber now meters
/// every such read and, once they have persisted for both
/// <see cref="WalLogSubscriber.EmptyBacklogEscalationReads"/> drains and
/// <see cref="WalLogSubscriber.EmptyBacklogEscalationAge"/>, refreshes the
/// partition's trim watermark - never a fall-off, since an empty read is
/// legitimate while a phase-2 commit lands.
/// </summary>
public sealed partial class WalLogSubscriberTests
{
    private sealed class ManualClock : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 11, 8, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    private static long CountEmptyBacklogReads(Action body, string tree)
    {
        long count = 0;
        using var listener = Orleans.Lattice.Testing.MeterListening.StartForInstrument(
            LatticeMetrics.WalSubscriberEmptyBacklogReads,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && Equals(tag.Value, tree))
                    {
                        Interlocked.Add(ref count, value);
                    }
                }
            }));
        body();
        return Interlocked.Read(ref count);
    }

    [Test]
    public void A_partition_that_keeps_reading_empty_under_a_waiting_head_is_metered_and_its_watermark_refreshed()
    {
        var clock = new ManualClock();
        var reader = new FakeCommitLogReader();
        var subscriber = new WalLogSubscriber(reader, new InMemoryWalCursorRegistry()) { Clock = clock };
        for (var i = 0; i < 4; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }

        reader.ClampReadsThrough(Tree, 0, 3);
        var handler = new CollectingHandler();
        var checkpoints = new Dictionary<int, long> { [0] = -1 };
        var refreshesBeforeAge = -1;
        var fellOff = false;

        var metered = CountEmptyBacklogReads(() =>
        {
            for (var drain = 0; drain < 4; drain++)
            {
                if (drain == 3)
                {
                    refreshesBeforeAge = reader.WatermarkRefreshes.Count;
                    clock.Advance(TimeSpan.FromSeconds(11));
                }
                else if (drain > 0)
                {
                    clock.Advance(TimeSpan.FromSeconds(10));
                }

                var result = subscriber.DrainAsync(Context(1, checkpoints, pinWal: false), handler).GetAwaiter().GetResult();
                fellOff |= result.FellOffLog;
            }
        }, Tree);

        var recovered = subscriber.DrainAsync(Context(1, checkpoints, pinWal: false), handler).GetAwaiter().GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(metered, Is.EqualTo(4L), "every drain that read nothing under a waiting head must be metered");
            Assert.That(fellOff, Is.False, "an empty read under a waiting head is never a fall-off");
            Assert.That(refreshesBeforeAge, Is.Zero,
                "three empty reads inside the escalation age must not refresh: a phase-2 commit may still be landing");
            Assert.That(reader.WatermarkRefreshes, Is.EqualTo(new[] { (Tree, 0) }),
                "once the empty reads outlast the escalation age the partition's watermark must be refreshed, once");
            Assert.That(recovered.AdvancedOffsets, Is.EqualTo(new Dictionary<int, long> { [0] = 3 }),
                "after the refresh the next drain reads every waiting entry");
        });
    }

    [Test]
    public void A_partition_that_reads_again_before_the_escalation_age_is_never_refreshed()
    {
        var clock = new ManualClock();
        var reader = new FakeCommitLogReader();
        var subscriber = new WalLogSubscriber(reader, new InMemoryWalCursorRegistry()) { Clock = clock };
        reader.Append(Tree, 0, Set(10));
        reader.ClampReadsThrough(Tree, 0, 0);
        var handler = new CollectingHandler();
        var checkpoints = new Dictionary<int, long> { [0] = -1 };

        for (var drain = 0; drain < 2; drain++)
        {
            _ = subscriber.DrainAsync(Context(1, checkpoints, pinWal: false), handler).GetAwaiter().GetResult();
            clock.Advance(TimeSpan.FromSeconds(10));
        }

        // The phase-2 commit lands: the entry becomes readable and the streak ends.
        reader.RefreshTrimWatermarkAsync(Tree, 0).GetAwaiter().GetResult();
        reader.WatermarkRefreshes.Clear();
        var read = subscriber.DrainAsync(Context(1, checkpoints, pinWal: false), handler).GetAwaiter().GetResult();
        checkpoints[0] = read.AdvancedOffsets[0];

        // A new entry is hidden again: a fresh streak must sit out the whole age.
        reader.Append(Tree, 0, Set(20));
        reader.ClampReadsThrough(Tree, 0, 1);
        for (var drain = 0; drain < 3; drain++)
        {
            clock.Advance(TimeSpan.FromSeconds(10));
            _ = subscriber.DrainAsync(Context(1, checkpoints, pinWal: false), handler).GetAwaiter().GetResult();
        }

        Assert.That(reader.WatermarkRefreshes, Is.Empty,
            "a read that returned entries ends the streak, so the empty reads before it never count toward the age");
    }
}
