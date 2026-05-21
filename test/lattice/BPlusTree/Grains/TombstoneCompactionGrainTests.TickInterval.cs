using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the per-tree compaction shard-tick interval.
/// Asserts that <see cref="LatticeOptions.CompactionShardTickInterval"/>
/// is snapshotted at pass start, that mid-pass option changes do not
/// reshape an in-flight pass, and that a configured value below
/// <see cref="LatticeOptions.MinCompactionShardTickInterval"/> is
/// clamped up to the floor.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task TickInterval_default_is_propagated_into_pass_snapshot()
    {
        // Default LatticeOptions => default tick interval (2 s).
        var (grain, _, _, grainFactory, _) = CreateGrain();
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(grain.CurrentTickIntervalForTests,
            Is.EqualTo(LatticeOptions.DefaultCompactionShardTickInterval));
    }

    [Test]
    public async Task TickInterval_per_tree_override_propagates_into_pass_snapshot()
    {
        var custom = TimeSpan.FromMilliseconds(250);
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionShardTickInterval = custom,
        };
        var (grain, _, _, grainFactory, _) = CreateGrain(options);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(grain.CurrentTickIntervalForTests, Is.EqualTo(custom));
    }

    [Test]
    public async Task TickInterval_below_floor_is_clamped_to_floor_in_pass_snapshot()
    {
        LatticeOptionsResolver.ResetWarnedClampedTickIntervalTreesForTests();
        var below = TimeSpan.FromMilliseconds(50);
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionShardTickInterval = below,
        };
        var (grain, _, _, grainFactory, _) = CreateGrain(options);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(grain.CurrentTickIntervalForTests,
            Is.EqualTo(LatticeOptions.MinCompactionShardTickInterval));
    }

    [Test]
    public async Task TickInterval_is_snapshot_at_pass_start_and_mid_pass_change_is_ignored()
    {
        // Snapshot-at-pass-start semantics: a mid-pass option change must
        // not retroactively reshape the in-flight pass. Both the grain's
        // IOptionsMonitor and the resolver's IOptionsMonitor return the
        // same LatticeOptions reference, so mutating the instance after
        // pass start emulates a live IOptionsMonitor reload.
        var initial = TimeSpan.FromMilliseconds(500);
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionShardTickInterval = initial,
        };
        var (grain, _, _, grainFactory, _) = CreateGrain(options);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        Assert.That(grain.CurrentTickIntervalForTests, Is.EqualTo(initial),
            "snapshot must reflect the value resolved at pass start");

        // Mutate the live options instance in place; the in-flight pass
        // must keep the original snapshot.
        options.CompactionShardTickInterval = TimeSpan.FromMilliseconds(150);

        Assert.That(grain.CurrentTickIntervalForTests, Is.EqualTo(initial),
            "mid-pass option change must not retroactively reshape the in-flight snapshot");
    }

    [Test]
    public async Task TickInterval_next_pass_picks_up_changed_value()
    {
        // Companion to the snapshot test: after the in-flight pass
        // completes, a new pass must observe the updated value.
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionShardTickInterval = TimeSpan.FromMilliseconds(500),
        };
        var (grain, _, reminderRegistry, grainFactory, _) = CreateGrain(options);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync(); // shard 0
        await grain.ProcessNextShardAsync(); // shard 1
        await grain.ProcessNextShardAsync(); // completes the pass

        // Configure a new value before the next pass begins.
        var changed = TimeSpan.FromMilliseconds(150);
        options.CompactionShardTickInterval = changed;

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(grain.CurrentTickIntervalForTests, Is.EqualTo(changed),
            "the next pass must pick up the configured value");
    }
}
