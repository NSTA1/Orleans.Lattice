using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TombstoneCompactionGrainTests
{
    // --- RequestCompactionAsync ---

    [Test]
    public async Task RequestCompaction_throws_when_triggerKind_is_null()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.That(() => grain.TryBeginRequestedCompactionAsync(0, null!),
            Throws.InstanceOf<ArgumentNullException>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task RequestCompaction_throws_when_triggerKind_is_unknown()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.That(async () => await grain.TryBeginRequestedCompactionAsync(0, "garbage"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RequestCompaction_returns_false_when_compaction_disabled()
    {
        var options = new LatticeOptions { TombstoneGracePeriod = Timeout.InfiniteTimeSpan };
        var (grain, _, _, _, _) = CreateGrain(options);

        var result = await grain.TryBeginRequestedCompactionAsync(0, "operator");

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task RequestCompaction_returns_false_when_pass_already_in_progress()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        Assert.That(state.State.InProgress, Is.True);

        var result = await grain.TryBeginRequestedCompactionAsync(0, "operator");

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task RequestCompaction_returns_false_when_shard_not_in_topology()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        // ShardCount is 2 (indices 0 and 1) - request 99.
        var result = await grain.TryBeginRequestedCompactionAsync(99, "operator");

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task RequestCompaction_records_trigger_timestamp_for_ratio_trigger()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        var before = DateTimeOffset.UtcNow;
        var result = await grain.TryBeginRequestedCompactionAsync(1, "ratio");
        var after = DateTimeOffset.UtcNow;

        Assert.That(result, Is.True);
        Assert.That(state.State.LastTriggerAt, Is.Not.Null);
        Assert.That(state.State.LastTriggerAt!.ContainsKey(1), Is.True);
        var stamped = state.State.LastTriggerAt[1];
        Assert.That(stamped, Is.GreaterThanOrEqualTo(before).And.LessThanOrEqualTo(after));
    }

    [Test]
    public async Task RequestCompaction_drops_ratio_request_within_cooldown_window()
    {
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionTriggerCooldown = TimeSpan.FromMinutes(5),
        };
        var existing = new FakePersistentState<TombstoneCompactionState>();
        existing.State.LastTriggerAt[1] = DateTimeOffset.UtcNow;
        var (grain, _, _, grainFactory, _) = CreateGrain(options, existing);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        var result = await grain.TryBeginRequestedCompactionAsync(1, "ratio");

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task RequestCompaction_operator_request_bypasses_cooldown()
    {
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionTriggerCooldown = TimeSpan.FromMinutes(5),
        };
        var existing = new FakePersistentState<TombstoneCompactionState>();
        existing.State.LastTriggerAt[1] = DateTimeOffset.UtcNow;
        var (grain, _, _, grainFactory, _) = CreateGrain(options, existing);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        var result = await grain.TryBeginRequestedCompactionAsync(1, "operator");

        Assert.That(result, Is.True);
    }

    [Test]
    public async Task RequestCompaction_size_request_outside_cooldown_is_honoured()
    {
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionTriggerCooldown = TimeSpan.FromMinutes(5),
        };
        var existing = new FakePersistentState<TombstoneCompactionState>();
        existing.State.LastTriggerAt[1] = DateTimeOffset.UtcNow.AddMinutes(-10);
        var (grain, state, _, grainFactory, _) = CreateGrain(options, existing);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        var result = await grain.TryBeginRequestedCompactionAsync(1, "size");

        Assert.That(result, Is.True);
        Assert.That(state.State.InProgress, Is.True);
        // Pass was scoped to a single shard.
        Assert.That(state.State.PhysicalShardIndices, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public async Task RequestCompaction_zero_cooldown_disables_gating()
    {
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionTriggerCooldown = TimeSpan.Zero,
        };
        var existing = new FakePersistentState<TombstoneCompactionState>();
        existing.State.LastTriggerAt[0] = DateTimeOffset.UtcNow;
        var (grain, _, _, grainFactory, _) = CreateGrain(options, existing);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        var result = await grain.TryBeginRequestedCompactionAsync(0, "ratio");

        Assert.That(result, Is.True);
    }
}
