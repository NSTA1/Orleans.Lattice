using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainStrandedLeafRecoveryTests
{
    [Test]
    public void Repeated_stalls_back_off_eviction_without_claiming_permanent_failure()
    {
        var logger = Substitute.For<ILogger<ShardRootGrain>>();
        var harness = CreateHarness(logger: logger);
        try
        {
            StrandOnce(harness);
            var firstRetry = Stall(harness);
            var writes = harness.State.WriteCount;
            var secondRetry = Stall(harness);

            Assert.Multiple(() =>
            {
                Assert.That(firstRetry.LeafStranded, Is.True);
                Assert.That(secondRetry.LeafStranded, Is.True);
                Assert.That(harness.ReadCalls(), Is.EqualTo(2),
                    "the fresh read must remain coalesced during backoff, not enqueue on every retry");
                Assert.That(secondRetry.StrandedRecoveryApplications, Is.EqualTo(1));
                Assert.That(harness.State.WriteCount, Is.EqualTo(writes));
                Assert.That(secondRetry.Message, Does.Contain("deferred"));
                Assert.That(secondRetry.Message, Does.Not.Contain("UNREADABLE"));
                Assert.That(secondRetry.Message, Does.Not.Contain("fault is inside"));
                Assert.That(secondRetry.Message, Does.Not.Contain("no further scan attempt"));
            });
            var warnings = logger.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(ILogger.Log))
                .Select(c => c.GetArguments()[2]!.ToString()).ToArray();
            Assert.That(warnings, Has.Length.EqualTo(1));
            Assert.That(warnings[0], Does.Not.Contain("unreadable"));
            Assert.That(warnings[0], Does.Not.Contain("no further scan attempt"));
        }
        finally
        {
            harness.Drain();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Replay_queue_pressure_preserves_the_read_and_recovers_when_it_drains()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 24);
        var harness = CreateHarness();
        try
        {
            var stall = StrandOnce(harness);
            for (var i = 0; i < 4; i++)
                stall = Stall(harness);

            Assert.Multiple(() =>
            {
                Assert.That(stall.LeafStranded, Is.True);
                Assert.That(stall.StrandedRecoveryApplications, Is.Zero);
                Assert.That(harness.ReadCalls(), Is.EqualTo(1));
                Assert.That(harness.State.WriteCount, Is.Zero);
                Assert.That(stall.Message, Does.Contain("24 queued"));
                Assert.That(stall.Message, Does.Contain("does not prove"));
            });

            // The queued original request can complete; no eviction or restart is needed.
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
            harness.Heal();
            harness.Parked[0].SetResult([new("k0000", new byte[] { 0 })]);
            var page = await Page(harness);
            Assert.That(page.Entries, Has.Count.EqualTo(2));
            harness.Parked[0] = new(TaskCreationOptions.RunContinuationsAsynchronously);
            harness.Rewedge();
            Assert.That(Stall(harness).ConsecutiveZeroProgressStalls, Is.EqualTo(1));
        }
        finally
        {
            harness.Drain();
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    public void Recovery_backoff_is_exponential_bounded_and_resets_on_a_different_leaf()
    {
        var harness = CreateHarness();
        harness.Options.MaxScanPageStallDuration = TimeSpan.FromMilliseconds(50);
        try
        {
            var recoveryAttempts = new List<int>();
            var previous = 0;
            for (var attempt = 1; attempt <= 165; attempt++)
            {
                var stall = Stall(harness);
                Assert.That(stall.LeafInFlight, Is.EqualTo(harness.WedgedLeafId.ToString()),
                    "every measured ceiling must reach the leaf, not expire during the prologue");
                if (stall.StrandedRecoveryApplications != previous)
                {
                    previous = stall.StrandedRecoveryApplications;
                    recoveryAttempts.Add(attempt);
                }
            }

            Assert.That(recoveryAttempts, Is.EqualTo(new[] { 3, 9, 21, 45, 93, 141 }),
                "doubling stops at 48 stalled page fills, so a permanently orphaned read is retried");
            harness.MoveWedgeToSecondLeaf();
            var other = StrandOnce(harness);
            Assert.That(other.StrandedRecoveryApplications, Is.EqualTo(1));
        }
        finally
        {
            harness.Drain();
        }
    }
}
