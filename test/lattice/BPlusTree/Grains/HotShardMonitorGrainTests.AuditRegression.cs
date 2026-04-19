using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression test for audit bug #12: <see cref="HotShardMonitorGrain"/>
/// maintains a private per-shard cooldown dictionary that was never pruned.
/// When a shard was removed from the <see cref="ShardMap"/> (e.g. after a
/// merge/resize), its entry remained in the dictionary forever — a slow but
/// unbounded memory leak over the lifetime of a long-running monitor.
/// </summary>
public partial class HotShardMonitorGrainTests
{
    [Test]
    public async Task RunSamplingPass_prunes_cooldown_entries_for_shards_no_longer_in_map()
    {
        var (grain, _, _, _, _, _, _) = CreateGrain(physicalShardCount: 2, virtualShardCount: 16);

        // Reach into the private _shardCooldownUntilUtc dictionary and seed
        // an entry for a shard index (99) that does not exist in the map.
        var field = typeof(HotShardMonitorGrain).GetField(
            "_shardCooldownUntilUtc",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "Expected private _shardCooldownUntilUtc field.");
        var cooldowns = (Dictionary<int, DateTime>)field!.GetValue(grain)!;
        cooldowns[99] = DateTime.UtcNow.AddHours(1);

        await grain.RunSamplingPassAsync();

        Assert.That(cooldowns.ContainsKey(99), Is.False,
            "Cooldown entry for a shard index no longer in the ShardMap must be pruned.");
    }
}
