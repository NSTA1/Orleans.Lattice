using System.Reflection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the best-effort contract of the point-in-time pin release on both of
/// its real call sites (issue #2405): an unpin failure is logged and swallowed,
/// never propagated, so the caller's remaining cleanup still runs.
/// </summary>
public partial class LatticeCursorGrainTests
{
    [Test]
    public void ReleasePointInTimePinAsync_carries_no_dead_rethrow_parameter()
    {
        var method = typeof(LatticeCursorGrain).GetMethod(
            "ReleasePointInTimePinAsync",
            BindingFlags.NonPublic | BindingFlags.Instance);

        Assert.That(method, Is.Not.Null,
            "the pin release helper must still exist; this guard pins its shape");
        Assert.That(method!.GetParameters(), Is.Empty,
            "both call sites release best-effort, so a propagation-mode parameter is unreachable");
    }

    [Test]
    public async Task CloseAsync_pointInTime_unpin_failure_is_logged_and_swallowed()
    {
        var logs = new RecordingLoggerFactory();
        var (grain, state, pinId, registry) = await OpenPointInTimeCursorWithFailingUnpinAsync(logs);

        Assert.DoesNotThrowAsync(() => grain.CloseAsync());

        await registry.Received(1).UnpinSnapshotAsync(pinId);
        AssertUnpinFailureWarning(logs, pinId);
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
            "the state clear after the release must still run when the unpin fails");
        Assert.That(state.State.SnapshotPinId, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task ReceiveReminder_idle_ttl_pointInTime_unpin_failure_is_logged_and_swallowed()
    {
        var logs = new RecordingLoggerFactory();
        var (grain, state, pinId, registry) = await OpenPointInTimeCursorWithFailingUnpinAsync(logs);

        Assert.DoesNotThrowAsync(() => grain.ReceiveReminder("cursor-ttl", new TickStatus()));

        await registry.Received(1).UnpinSnapshotAsync(pinId);
        AssertUnpinFailureWarning(logs, pinId);
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
            "the state clear after the release must still run when the unpin fails");
        Assert.That(state.State.SnapshotPinId, Is.EqualTo(Guid.Empty));
    }

    private static async Task<(LatticeCursorGrain Grain,
                               FakePersistentState<LatticeCursorState> State,
                               Guid PinId,
                               ITxRegistryGrain Registry)> OpenPointInTimeCursorWithFailingUnpinAsync(
        RecordingLoggerFactory logs)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.SnapshotAsync().Returns(new Dictionary<Guid, TxStatus>
        {
            [Guid.NewGuid()] = TxStatus.Committed,
        });
        registry.UnpinSnapshotAsync(Arg.Any<Guid>())
            .ThrowsAsync(new InvalidOperationException("registry unavailable"));

        var (grain, state, _) = CreateGrainWithRegistry(
            existingState: null,
            options: new LatticeOptions
            {
                TxRegistryShardCount = 1,
                MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
                MaxPinnedSagaDecisions = 100,
            },
            reminderRegistry: null,
            registry: registry,
            loggerFactory: logs);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            PointInTime = true,
        });

        var pinId = state.State.SnapshotPinId;
        Assert.That(pinId, Is.Not.EqualTo(Guid.Empty), "precondition: the cursor holds a pin");
        logs.Clear();
        return (grain, state, pinId, registry);
    }

    private static void AssertUnpinFailureWarning(RecordingLoggerFactory logs, Guid pinId)
    {
        var warning = logs.Warnings.SingleOrDefault(e =>
            e.Message.Contains("failed to release point-in-time snapshot pin", StringComparison.Ordinal));
        Assert.That(warning, Is.Not.Null, "the swallowed unpin failure must be logged");
        Assert.That(warning!.Value("PinId"), Is.EqualTo(pinId));
    }
}
