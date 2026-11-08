using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6007: a starvation drive gives up a slice read that storage does not
/// answer within its per-read deadline, below the cluster response timeout,
/// instead of surfacing a raw Orleans <see cref="TimeoutException"/>.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The drive budget in these fixtures: far above the per-read deadline, so a
    /// drive that returns well inside it can only have been ended by the deadline.
    /// </summary>
    private static readonly TimeSpan SliceDeadlineDriveBudget = TimeSpan.FromSeconds(20);

    private static readonly TimeSpan TestSliceReadDeadline = TimeSpan.FromMilliseconds(200);

    [Test]
    public async Task A_drive_gives_up_a_parked_slice_read_at_its_deadline_and_returns_its_permit()
    {
        var (gate, baseline) = await WarmReplayGateAsync();
        var treeId = UniqueStarvationDriveTree();
        var wal = new GrowingWal();
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: treeId,
            persistedCheckpoint: -1L,
            starvationDriveBudget: SliceDeadlineDriveBudget);
        await ActivateAsync(grain);
        wal.GrowTo(3);

        // The park ignores cancellation, as storage that has stopped answering
        // does, so the caller-side WaitAsync belt is what ends the wait.
        var park = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        wal.OnRead = () =>
        {
            Interlocked.Increment(ref entered);
            return park.Task;
        };

        var reasons = new ConcurrentQueue<string?>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayStarvationDriveAbandonments,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? tree = null, reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value as string;
                    if (tag.Key == LatticeMetrics.TagReason) reason = tag.Value as string;
                }
                if (value > 0 && tree == treeId)
                    reasons.Enqueue(reason);
            }));

        BPlusLeafGrain.DriveSliceReadDeadlineForTest = TestSliceReadDeadline;
        try
        {
            var stopwatch = Stopwatch.StartNew();
            var verdict = await grain.DriveStarvedCheckpointAsync();
            stopwatch.Stop();

            Assert.Multiple(() =>
            {
                Assert.That(Volatile.Read(ref entered), Is.GreaterThan(0),
                    "instrument validation: the drive must have reached the parked slice read");
                Assert.That(park.Task.IsCompleted, Is.False,
                    "storage must still be parked, or this measures recovery rather than the deadline");
                Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                    "a slice read given up at its deadline is the shipped TimedOut verdict, not a fault");
                Assert.That(stopwatch.Elapsed, Is.LessThan(SliceDeadlineDriveBudget / 2),
                    "the per-read deadline, not the drive budget, must end the drive");
                Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                    "the permit must be back while the read is still parked");
                Assert.That(reasons, Is.EqualTo(new[] { "slice_read_deadline" }),
                    "the abandonment must be counted once, as a slice-read deadline rather than a spent budget");
            });
        }
        finally
        {
            BPlusLeafGrain.DriveSliceReadDeadlineForTest = null;
            park.TrySetResult();
        }
    }

    [Test]
    public async Task A_drive_slice_read_deadline_never_reaches_an_activation_replay()
    {
        // Drive-only scope (D7). An activation replay whose read takes longer
        // than the drive deadline must still complete: were the deadline to leak
        // into it, the activation would fail with the drive's typed fault.
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1L);

        var slow = TestSliceReadDeadline * 3;
        var reads = 0;
        wal.OnRead = async () =>
        {
            Interlocked.Increment(ref reads);
            await Task.Delay(slow);
        };

        BPlusLeafGrain.DriveSliceReadDeadlineForTest = TestSliceReadDeadline;
        try
        {
            Assert.DoesNotThrowAsync(async () => await ActivateAsync(grain),
                "an activation replay must not inherit the starvation drive's per-read deadline");
            Assert.That(Volatile.Read(ref reads), Is.GreaterThan(0),
                "instrument validation: the activation must have read through the slow hook");
        }
        finally
        {
            BPlusLeafGrain.DriveSliceReadDeadlineForTest = null;
        }
    }

    [TestCase(30_000, 20_000)]
    [TestCase(90_000, 60_000)]
    [TestCase(900, 1_000)]
    [TestCase(1, 1_000)]
    public void The_drive_slice_read_deadline_is_two_thirds_of_the_response_timeout_and_at_least_a_second(
        int responseTimeoutMs, int expectedMs)
        => Assert.That(
            BPlusLeafGrain.ComputeDriveSliceReadDeadline(TimeSpan.FromMilliseconds(responseTimeoutMs)),
            Is.EqualTo(TimeSpan.FromMilliseconds(expectedMs)));

    [Test]
    public void An_infinite_or_unset_response_timeout_leaves_only_the_drive_budget()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ComputeDriveSliceReadDeadline(Timeout.InfiniteTimeSpan), Is.EqualTo(Timeout.InfiniteTimeSpan));
            Assert.That(BPlusLeafGrain.ComputeDriveSliceReadDeadline(TimeSpan.MaxValue), Is.EqualTo(Timeout.InfiniteTimeSpan));
            Assert.That(BPlusLeafGrain.ComputeDriveSliceReadDeadline(TimeSpan.Zero), Is.EqualTo(Timeout.InfiniteTimeSpan));
        });
    }
}
