using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Issue #5570: the blocked-floor warning names the blocking pin's category.
/// <para>
/// The journey estate logs at warning level only, and every tree there reported
/// a pin that "carries no usable offset" without saying whether the leaf had
/// never checkpointed (by design: live writes do not advance a checkpoint, so
/// the block clears after the leaf's first WAL replay) or had checkpointed with
/// no snapshot covering it. The classification existed, but only on a metric
/// and an information-level line. These fixtures pin that the episode's one
/// warning carries it.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static async Task<IReadOnlyList<string>> BlockedWarningsForLeafStateAsync(LeafNodeState leafState)
    {
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var storage = new CannedLeafStateStorage(leafState);
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        var logs = new RecordingLoggerFactory();

        var scheduler = CreateScheduler(
            factory, gc, Adaptive(), time,
            logger: new Logger<LatticeWalGcScheduler>(logs),
            leafStateStorage: storage);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        return logs.Entries
            .Where(e => e.Level == LogLevel.Warning
                && e.Message.Contains("carries no usable offset", StringComparison.Ordinal))
            .Select(e => e.Message)
            .ToArray();
    }

    [Test]
    public async Task The_blocked_warning_names_a_never_checkpointed_pin_and_what_clears_it()
    {
        var warnings = await BlockedWarningsForLeafStateAsync(new LeafNodeState
        {
            TreeId = StrandedTree,
            ProjectionCheckpointOffsetsByPartition = null,
        });

        Assert.That(warnings, Has.Count.EqualTo(1),
            "the warning fires once per blocked episode, so two passes must still produce exactly one.");
        Assert.Multiple(() =>
        {
            Assert.That(warnings[0], Does.Contain("never_checkpointed"),
                "a host logging at warning level must be able to tell a never-checkpointed leaf - correct by "
                    + "design - from a coverage hole without a metrics scrape.");
            Assert.That(warnings[0], Does.Contain("first WAL replay"),
                "the warning must say what clears the block, not that the WAL is retained without bound.");
            Assert.That(warnings[0], Does.Not.Contain("without bound"),
                "a never-checkpointed block clears after the leaf's first replay; it is not unbounded.");
        });
    }

    [Test]
    public async Task The_blocked_warning_names_a_checkpointed_uncovered_pin()
    {
        var warnings = await BlockedWarningsForLeafStateAsync(LeafCheckpointedAt(offset: 1234));

        Assert.That(warnings, Has.Count.EqualTo(1));
        Assert.That(warnings[0], Does.Contain("checkpointed_uncovered").And.Contain("snapshot capture"),
            "a checkpointed leaf with no covering snapshot is the repairable state and must be named as such.");
    }

    [Test]
    public void Every_blocking_pin_state_has_its_own_remedy_text()
    {
        var members = Enum.GetValues<WalGcBlockingPinState>();
        Assert.That(members, Has.Length.GreaterThanOrEqualTo(4), "anti-vacuity: the enum must be non-trivial.");

        var unclassified = LatticeWalGcScheduler.DescribeBlockingPinRemedy(null);
        Assert.Multiple(() =>
        {
            foreach (var member in members)
            {
                Assert.That(LatticeWalGcScheduler.DescribeBlockingPinRemedy(member), Is.Not.EqualTo(unclassified),
                    $"{member} must not fall through to the unclassified remedy text.");
                Assert.That(LatticeWalGcScheduler.DescribeBlockingPinState(member), Is.Not.EqualTo("unclassified"),
                    $"{member} must be named by its metric tag.");
            }
        });
    }
}
