using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5981 (W12 on #5605): the live advance moves a resident leaf's checkpoint once per
/// coverage-lag tick, and a resident leaf applies its own writes synchronously, so that
/// checkpoint is no claim about undrained work. Reported as one, it refreshed the leaf's
/// drain-lag eligibility with an offset behind the partition head, and every live-written
/// leaf read as lagging between ticks while its siblings appended. A live-advanced partition
/// now reports its cursor (the WAL GC reads it) and withdraws its offset
/// (<see cref="IWalCursorRegistry.ClearCheckpointAsync"/>); a later replay advance claims an
/// offset again. Each detector runs the leaf against the real in-memory registry.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static async Task<(BPlusLeafGrain Grain, InMemoryWalCursorRegistry Registry, string TreeId)> LiveAdvancedResidentLeafAsync(string name)
    {
        var registry = new InMemoryWalCursorRegistry();
        var treeId = UniqueCoverageRepairTreeId(name);
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: treeId,
            cursorReporter: new InMemoryLeafCursorReporter(registry),
            liveAdvance: new LiveAdvanceRig());
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await SetStampedAsync(grain, "a", "1");
        await SetStampedAsync(grain, "b", "2");
        await SetStampedAsync(grain, "c", "3");
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True, "precondition: the leaf live-advanced");
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(2L), "precondition: over its own appends");
        return (grain, registry, treeId);
    }

    private static async Task<WalCursorSnapshot> LeafConsumerAsync(InMemoryWalCursorRegistry registry, string treeId) =>
        (await registry.SnapshotAsync(treeId))
            .Single(c => c.ConsumerId.StartsWith(ILeafCursorReporter.MaterialiserConsumerIdPrefix, StringComparison.Ordinal));

    [Test]
    public async Task A_live_advanced_resident_leaf_under_sibling_writes_is_not_counted_lagging_between_ticks()
    {
        var (_, registry, treeId) = await LiveAdvancedResidentLeafAsync("live-advance-drain-lag");
        var observedAt = DateTimeOffset.UtcNow;
        try
        {
            // Siblings keep appending to the shared partition after the leaf's checkpoint, an
            // hour before the sample: well past the threshold, as between two ticks.
            WalCommitLogWriter._walHeadWallClockTicks[treeId] = observedAt.UtcTicks;
            for (var offset = 3; offset <= 5; offset++)
            {
                WalCommitLogWriter.RecordAppend(treeId, 0, offset, observedAt.UtcTicks - TimeSpan.FromHours(1).Ticks);
            }

            var signal = new WalSaturationSignal();
            signal.ResetForTesting();
            var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions
            {
                WalSaturationRecoveryWindow = TimeSpan.Zero,
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(30),
                WalSaturationMaterialiserLagSampleWindows = 1,
                WalDrainLagConsumerFreshness = TimeSpan.FromMinutes(5),
            });
            var sampler = new WalSaturationSampler(
                signal,
                new WalSaturationObserverDispatcher(
                    Array.Empty<IWalSaturationObserver>(), NullLogger<WalSaturationObserverDispatcher>.Instance),
                monitor,
                NullLogger<WalSaturationSampler>.Instance,
                new VirtualTimeProvider(observedAt),
                registry);

            await sampler.SampleOnceAsync(CancellationToken.None);

            Assert.That(signal.GetCurrentState(treeId), Is.EqualTo(WalSaturationState.Healthy),
                "a resident leaf the live advance moved read as an hour behind its siblings' appends (#5981)");
        }
        finally
        {
            WalCommitLogWriter._walAppendTimes.TryRemove(treeId, out _);
            WalCommitLogWriter._walHeadWallClockTicks.TryRemove(treeId, out _);
        }
    }

    [Test]
    public async Task A_live_advanced_partition_keeps_its_cursor_and_withdraws_its_registry_offset()
    {
        var (_, registry, treeId) = await LiveAdvancedResidentLeafAsync("live-advance-withdraw");

        var consumer = await LeafConsumerAsync(registry, treeId);

        Assert.Multiple(() =>
        {
            Assert.That(consumer.Cursor, Is.GreaterThan(HybridLogicalClock.Zero),
                "the live-advance flush must still report the cursor the WAL GC reads");
            Assert.That(consumer.CheckpointOffsets, Is.Not.Null,
                "the consumer must read as offset-aware with no claim, not as a legacy HLC-only consumer");
            Assert.That(consumer.CheckpointOffsets!.ContainsKey(0), Is.False,
                "the live-advanced checkpoint was reported as a drain claim (#5981)");
        });
    }

    [Test]
    public async Task A_live_advanced_leaf_that_later_replays_claims_its_offset_again()
    {
        var (grain, registry, treeId) = await LiveAdvancedResidentLeafAsync("live-advance-reclaim");

        using (LatticeApplyOffsetContext.BeginScope(0, 5))
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(5, CancellationToken.None);
        }

        var consumer = await LeafConsumerAsync(registry, treeId);
        Assert.That(consumer.CheckpointOffsets is { } offsets && offsets.TryGetValue(0, out var claimed) ? claimed : (long?)null,
            Is.EqualTo(5L), "a replay advance must claim its checkpoint again (A3)");
    }
}
