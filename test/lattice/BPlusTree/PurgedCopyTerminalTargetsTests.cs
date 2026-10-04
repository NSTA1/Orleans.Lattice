using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4475: the resized copy's shards a terminal refused by a purged copy is
/// redelivered to.
/// </summary>
[TestFixture]
public class PurgedCopyTerminalTargetsTests
{
    [Test]
    public void Resolve_includes_the_same_index_and_each_keys_owner_sorted_without_duplicates()
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var keys = Enumerable.Range(0, 64).Select(i => $"key-{i}").ToList();
        var owners = keys.Select(map.Resolve).ToHashSet();
        var index = Enumerable.Range(0, LatticeConstants.DefaultShardCount).FirstOrDefault(i => !owners.Contains(i), 0);

        var targets = PurgedCopyTerminalTargets.Resolve(index, keys, map);

        Assert.That(targets, Is.EqualTo(owners.Append(index).Distinct().Order().ToList()));
    }

    [Test]
    public void Resolve_with_no_keys_targets_only_the_same_index()
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);

        Assert.That(PurgedCopyTerminalTargets.Resolve(3, [], map), Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public void Resolve_rejects_null_arguments()
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);

        Assert.Throws<ArgumentNullException>(() => PurgedCopyTerminalTargets.Resolve(0, null!, map));
        Assert.Throws<ArgumentNullException>(() => PurgedCopyTerminalTargets.Resolve(0, [], null!));
    }
}
