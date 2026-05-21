using System.Diagnostics.Metrics;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the per-leaf skipped-outcome telemetry path in
/// <see cref="TombstoneCompactionGrain"/>. When a leaf's
/// <c>CompactTombstonesAsync</c> call throws, the coordinator must
/// emit one <c>orleans.lattice.compaction.leaves.visited</c>
/// measurement tagged <c>outcome=skipped</c> before re-throwing so
/// shard-level retry/skip semantics are preserved.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task ProcessNextShard_emits_visited_skipped_when_leaf_walk_throws()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();

        // Seed a single leaf whose CompactTombstonesAsync fails.
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shardRoot);
        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns(Task.FromResult(new DirtyLeavesSnapshot { DirtyLeaves = [], ObservedAdvance = default }));
        shardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        var leafMock = Substitute.For<IBPlusLeafGrain>();
        grainFactory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leafMock);
        leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Throws(new InvalidOperationException("leaf failed"));

        var visited = new List<KeyValuePair<string, object?>[]>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == "orleans.lattice.compaction.leaves.visited")
                {
                    l.EnableMeasurementEvents(inst);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value <= 0) return;
            visited.Add(tags.ToArray());
        });
        listener.Start();

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // ProcessNextShardAsync swallows shard-level exceptions and
        // converts them to retry/skip bookkeeping; the per-leaf catch
        // block must still have emitted the skipped measurement.
        await grain.ProcessNextShardAsync();

        Assert.That(visited, Has.Count.EqualTo(1),
            "exactly one visited measurement should be emitted on leaf failure");
        var tags = visited[0];
        Assert.That(tags.Any(t =>
            t.Key == LatticeMetrics.TagOutcome && (t.Value as string) == "skipped"),
            Is.True, "outcome tag must be 'skipped'");
        Assert.That(tags.Any(t =>
            t.Key == LatticeMetrics.TagTree && (t.Value as string) == TreeId),
            Is.True, "tree tag must be present");
    }

    [Test]
    public async Task ProcessNextShard_skipped_emission_carries_trigger_tag_when_policy_active()
    {
        // Enable a policy knob so the trigger-label scope is opened
        // inside CompactShardAsync.
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            MinTombstoneRatioForCompaction = 0.5,
            CompactionTriggerCooldown = TimeSpan.Zero,
        };
        var (grain, _, _, grainFactory, _) = CreateGrain(options);
        SetupShardWithLeaves(grainFactory, 0); // unused; we override shard 1 below

        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1").Returns(shardRoot);
        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns(Task.FromResult(new DirtyLeavesSnapshot { DirtyLeaves = [], ObservedAdvance = default }));
        shardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        var leafMock = Substitute.For<IBPlusLeafGrain>();
        grainFactory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leafMock);
        leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Throws(new InvalidOperationException("leaf failed"));

        var visited = new List<KeyValuePair<string, object?>[]>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == "orleans.lattice.compaction.leaves.visited")
                {
                    l.EnableMeasurementEvents(inst);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value <= 0) return;
            visited.Add(tags.ToArray());
        });
        listener.Start();

        // Drive the policy-trigger path, which sets _currentTriggerKind
        // to "ratio" before the shard walk runs.
        var honoured = await grain.TryBeginRequestedCompactionAsync(1, "ratio");
        Assert.That(honoured, Is.True);

        await grain.ProcessNextShardAsync();

        Assert.That(visited, Has.Count.EqualTo(1));
        var tags = visited[0];
        Assert.That(tags.Any(t =>
            t.Key == LatticeMetrics.TagOutcome && (t.Value as string) == "skipped"),
            Is.True, "outcome tag must be 'skipped'");
        Assert.That(tags.Any(t =>
            t.Key == LatticeMetrics.TagTrigger && (t.Value as string) == "ratio"),
            Is.True, "trigger tag must be 'ratio' when a policy knob is active");
    }
}
