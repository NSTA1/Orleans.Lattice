using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The delta-construction half of <see cref="OrMapAccessor{TKey, TValue}"/>.
/// <para>
/// Every mutating method builds an <see cref="OrMapDelta{TKey, TValue}"/>
/// alongside the in-memory mutation, and the delta - not the mutated map - is
/// what replication ships. The sibling fixture covers the happy paths where the
/// map is empty beforehand, which leaves the three shapes that only arise on a
/// map that already holds state: removing a key that was never written,
/// flattening a remote's tombstones on merge, and minting a fresh causal dot
/// above the dots already observed.
/// </para>
/// </summary>
public partial class CrdtAccessorIntegrationTests
{
    /// <summary>The highest counter any replica has minted for <paramref name="mapKey"/>, live or tombstoned.</summary>
    private static long HighestObservedCounter(OrMap<string, OrSet> map, string mapKey)
    {
        long highest = 0;
        if (map.Adds.TryGetValue(mapKey, out var adds))
        {
            foreach (var add in adds)
            {
                if (add.Counter > highest) highest = add.Counter;
            }
        }

        if (map.Tombstones.TryGetValue(mapKey, out var tombstones))
        {
            foreach (var dot in tombstones)
            {
                if (dot.Counter > highest) highest = dot.Counter;
            }
        }

        return highest;
    }

    [Test]
    public async Task OrMap_RemoveAsync_on_a_never_written_map_key_tombstones_nothing()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrMap<string, OrSet>("k");

        // A key with no observed dots has nothing to tombstone, so the delta
        // carries an empty tombstone set rather than a manufactured dot.
        await accessor.RemoveAsync("never-written");

        var stored = await accessor.GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(stored.ContainsKey("never-written"), Is.False);
            Assert.That(stored.Tombstones.ContainsKey("never-written"), Is.False,
                "removing an unobserved key must not invent a tombstone");
        });
    }

    [Test]
    public async Task OrMap_RemoveAsync_on_a_never_written_map_key_does_not_block_a_later_write()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrMap<string, OrSet>("k");
        await accessor.RemoveAsync("never-written");

        // The control that the empty-tombstone delta above is inert rather than
        // merely invisible: a subsequent write to the same key must still land.
        var inner = new OrSet();
        inner.Add(Bytes("alpha"), "r1", 1);
        await accessor.SetAsync("never-written", "r1", inner);

        var stored = await accessor.GetValueAsync("never-written");
        Assert.That(stored, Is.Not.Null);
        Assert.That(stored!.Contains(Bytes("alpha")), Is.True);
    }

    [Test]
    public async Task OrMap_MergeAsync_carries_a_remotes_tombstones_so_its_removals_are_observed()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrMap<string, OrSet>("k");

        // A remote that added two keys and then removed one of them. Its
        // tombstone map is non-empty, so the merge delta has to flatten it -
        // dropping it would resurrect "gone" on every subsequent merge.
        var remote = new OrMap<string, OrSet>();
        var kept = new OrSet();
        kept.Add(Bytes("keep-me"), "r2", 1);
        remote.Set("kept", "r2", kept);
        var doomed = new OrSet();
        doomed.Add(Bytes("drop-me"), "r2", 1);
        remote.Set("gone", "r2", doomed);
        remote.Remove("gone");

        Assert.That(remote.Tombstones, Is.Not.Empty, "the fixture must actually present a tombstone to flatten");

        await accessor.MergeAsync(remote);

        var stored = await accessor.GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(stored.ContainsKey("kept"), Is.True);
            Assert.That(stored.ContainsKey("gone"), Is.False);
            Assert.That(stored.Tombstones.ContainsKey("gone"), Is.True,
                "the remote's tombstone has to be retained, not just its effect applied");
        });
    }

    [Test]
    public async Task OrMap_MergeAsync_of_a_remote_removal_is_add_wins_against_a_concurrent_local_write()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrMap<string, OrSet>("k");

        // Local writes a dot the remote never observed.
        var local = new OrSet();
        local.Add(Bytes("local"), "r1", 1);
        await accessor.SetAsync("tags", "r1", local);

        // The remote removes only the dots it observed - its own.
        var remote = new OrMap<string, OrSet>();
        var remoteInner = new OrSet();
        remoteInner.Add(Bytes("remote"), "r2", 1);
        remote.Set("tags", "r2", remoteInner);
        remote.Remove("tags");

        await accessor.MergeAsync(remote);

        // Add-wins: the unobserved local dot survives the remote's removal.
        var survived = await accessor.GetValueAsync("tags");
        Assert.That(survived, Is.Not.Null);
        Assert.That(survived!.Contains(Bytes("local")), Is.True);
    }

    [Test]
    public async Task OrMap_SetAsync_after_a_remove_mints_a_dot_above_every_tombstoned_one()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrMap<string, OrSet>("k");

        var first = new OrSet();
        first.Add(Bytes("first"), "r1", 1);
        await accessor.SetAsync("tags", "r1", first);
        await accessor.RemoveAsync("tags");

        var afterRemove = await accessor.GetAsync();
        long tombstoned = HighestObservedCounter(afterRemove, "tags");
        Assert.That(tombstoned, Is.GreaterThan(0), "the remove must have left an observed dot to out-rank");

        // Re-writing has to mint a counter above every dot already observed for
        // the replica - live or tombstoned. A counter that collided with the
        // tombstoned dot would be swallowed and the key would stay dead.
        var second = new OrSet();
        second.Add(Bytes("second"), "r1", 1);
        await accessor.SetAsync("tags", "r1", second);

        var revived = await accessor.GetValueAsync("tags");
        Assert.Multiple(() =>
        {
            Assert.That(revived, Is.Not.Null, "re-setting a removed key must make it live again");
            Assert.That(revived!.Contains(Bytes("second")), Is.True);
        });
    }

    [Test]
    public async Task OrMap_SetAsync_mints_a_strictly_increasing_counter_per_replica()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrMap<string, OrSet>("k");

        var one = new OrSet();
        one.Add(Bytes("one"), "r1", 1);
        await accessor.SetAsync("tags", "r1", one);
        long afterFirst = HighestObservedCounter(await accessor.GetAsync(), "tags");

        var two = new OrSet();
        two.Add(Bytes("two"), "r1", 1);
        await accessor.SetAsync("tags", "r1", two);
        long afterSecond = HighestObservedCounter(await accessor.GetAsync(), "tags");

        Assert.That(afterSecond, Is.GreaterThan(afterFirst));
    }

    [Test]
    public async Task OrMap_SetAsync_counter_allocation_ignores_other_replicas_dots()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrMap<string, OrSet>("k");

        // A remote replica with a deliberately high counter. The local replica's
        // next dot is derived from its OWN highest counter, so a foreign dot must
        // not inflate it - the two replicas number their dots independently.
        var remote = new OrMap<string, OrSet>();
        var remoteInner = new OrSet();
        remoteInner.Add(Bytes("remote"), "r2", 1);
        remote.Set("tags", "r2", remoteInner);
        remote.Adds["tags"][0].Counter = 500;
        await accessor.MergeAsync(remote);

        var local = new OrSet();
        local.Add(Bytes("local"), "r1", 1);
        await accessor.SetAsync("tags", "r1", local);

        var stored = await accessor.GetAsync();
        var localDot = stored.Adds["tags"].Single(e => e.ReplicaId == "r1");
        Assert.Multiple(() =>
        {
            Assert.That(localDot.Counter, Is.EqualTo(1),
                "the local replica numbers from its own dots, not the remote's");
            Assert.That(stored.ContainsKey("tags"), Is.True);
        });
    }
}
