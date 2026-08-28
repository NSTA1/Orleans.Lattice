using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regression for the read-purity contract on
/// <see cref="OrMap{TKey, TValue}.Get(TKey)"/> when the value CRDT is itself an
/// <see cref="OrMap{TKey, TValue}"/> (composite nesting).
/// <para>
/// <c>Get</c>'s multi-live-entry path seeded the fold with <c>new TValue()</c>
/// and folded every contributor into it. For a nested map the first fold
/// adopted the stored entry's inner state by reference and the second fold then
/// wrote through that alias - so a pure read mutated the map's own durable
/// state, and two replicas with byte-identical authored history shipped
/// different per-dot payloads purely because one of them served a read.
/// </para>
/// </summary>
[TestFixture]
public class OrMapNestedValueTests
{
    private static OrMap<string, PnCounter> Inner(string key, string replicaId, long increment)
    {
        var inner = new OrMap<string, PnCounter>();
        var counter = new PnCounter();
        counter.Increment("author", increment);
        inner.Set(key, replicaId, counter);
        return inner;
    }

    // Two live outer entries under one key (distinct outer dots), each carrying
    // a nested map whose own inner dot collides with the other's.
    private static OrMap<string, OrMap<string, PnCounter>> NestedMapWithTwoLiveEntries()
    {
        var outer = new OrMap<string, OrMap<string, PnCounter>>();
        outer.Set("k", "rA", Inner("ik", "s1", 1));
        outer.Set("k", "rB", Inner("ik", "s1", 2));
        return outer;
    }

    private static long StoredInnerValue(OrMap<string, OrMap<string, PnCounter>> outer, int entryIndex) =>
        outer.Adds["k"][entryIndex].Value.Get("ik")!.Value;

    [Test]
    public void Get_when_a_key_has_two_live_nested_map_entries_does_not_mutate_the_stored_values()
    {
        var outer = NestedMapWithTwoLiveEntries();
        var before = StoredInnerValue(outer, 0);

        outer.Get("k");

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo(1), "precondition: the first stored entry holds 1");
            Assert.That(StoredInnerValue(outer, 0), Is.EqualTo(before),
                "Get is a read: it must not fold contributors into the map's own stored entries");
        });
    }

    [Test]
    public void Get_when_called_repeatedly_returns_the_same_value()
    {
        var outer = NestedMapWithTwoLiveEntries();

        var first = outer.Get("k")!.Get("ik")!.Value;
        outer.Get("k");
        var third = outer.Get("k")!.Get("ik")!.Value;

        Assert.That(third, Is.EqualTo(first),
            "repeated reads of an unmutated map must return the same value");
    }

    [Test]
    public void Get_when_a_key_has_two_live_nested_map_entries_returns_a_value_isolated_from_the_map()
    {
        var outer = NestedMapWithTwoLiveEntries();

        var read = outer.Get("k")!;
        read.Set("ik", "caller", new PnCounter());
        read.Set("fresh-key", "caller", new PnCounter());

        Assert.That(outer.Get("k")!.ContainsKey("fresh-key"), Is.False,
            "the value handed back by Get must be independent of the map's stored state");
    }

    [Test]
    public void Get_when_a_replica_serves_a_read_does_not_change_its_serialized_state()
    {
        // Two replicas with identical authored history. One serves a read.
        var quiet = NestedMapWithTwoLiveEntries();
        var readServing = NestedMapWithTwoLiveEntries();

        readServing.Get("k");

        Assert.That(StoredInnerValue(readServing, 0), Is.EqualTo(StoredInnerValue(quiet, 0)),
            "two replicas with identical authored history must ship identical per-dot payloads");
    }
}
