namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Covers <see cref="ObservedRemoveDots"/>, the dot bookkeeping shared by the
/// observed-remove accessors and the tag index's atomic write path.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class ObservedRemoveDotsTests
{
    [Test]
    public void NextCounter_for_an_OrFlag_follows_the_replicas_highest_dot_across_enables_and_tombstones()
    {
        var flag = new OrFlag();
        flag.Enables.AddRange([Dot("r", 2), Dot("other", 9)]);
        flag.Tombstones.Add(Dot("r", 5));

        Assert.Multiple(() =>
        {
            Assert.That(ObservedRemoveDots.NextCounter(flag, "r"), Is.EqualTo(6));
            Assert.That(ObservedRemoveDots.NextCounter(flag, "fresh"), Is.EqualTo(1));
        });
    }

    [Test]
    public void NextCounter_for_an_RwFlag_includes_disable_dots()
    {
        var flag = new RwFlag();
        flag.Enables.Add(Dot("r", 1));
        flag.Disables.Add(Dot("r", 7));
        flag.Tombstones.Add(Dot("r", 3));

        Assert.That(ObservedRemoveDots.NextCounter(flag, "r"), Is.EqualTo(8));
    }

    [Test]
    public void NextCounter_for_sets_spans_every_dot_map()
    {
        var orSet = new OrSet();
        orSet.Adds["x"] = [Dot("r", 2)];
        orSet.Tombstones["y"] = [Dot("r", 4)];
        var rwSet = new RwSet();
        rwSet.Adds["x"] = [Dot("r", 2)];
        rwSet.Removes["y"] = [Dot("r", 6)];
        rwSet.Tombstones["z"] = [Dot("r", 4)];

        Assert.Multiple(() =>
        {
            Assert.That(ObservedRemoveDots.NextCounter(orSet, "r"), Is.EqualTo(5));
            Assert.That(ObservedRemoveDots.NextCounter(rwSet, "r"), Is.EqualTo(7));
        });
    }

    [Test]
    public void ObservedDisables_copies_the_disable_dots_and_shares_the_empty_array()
    {
        var empty = new RwFlag();
        var flag = new RwFlag();
        flag.Disables.AddRange([Dot("r", 1), Dot("s", 2)]);

        var observed = ObservedRemoveDots.ObservedDisables(flag);

        Assert.Multiple(() =>
        {
            Assert.That(ObservedRemoveDots.ObservedDisables(empty), Is.SameAs(Array.Empty<OrSetDot>()));
            Assert.That(observed, Is.EqualTo(flag.Disables));
        });
    }

    [Test]
    public void FlattenToDeltaDots_decodes_each_element_and_carries_every_dot()
    {
        var element = new byte[] { 1, 2, 3 };
        var map = new Dictionary<string, List<OrSetDot>>
        {
            [Convert.ToBase64String(element)] = [Dot("r", 1), Dot("s", 2)],
            ["AA=="] = [],
        };

        var flattened = ObservedRemoveDots.FlattenToDeltaDots(map);

        Assert.Multiple(() =>
        {
            Assert.That(flattened, Has.Length.EqualTo(2));
            Assert.That(flattened[0].Element, Is.EqualTo(element));
            Assert.That(flattened[1].ReplicaId, Is.EqualTo("s"));
            Assert.That(flattened[1].Counter, Is.EqualTo(2));
            Assert.That(ObservedRemoveDots.FlattenToDeltaDots([]), Is.SameAs(Array.Empty<OrSetDeltaDot>()));
        });
    }

    private static OrSetDot Dot(string replica, long counter) => new() { ReplicaId = replica, Counter = counter };
}
