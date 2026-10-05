using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The re-bind update of <see cref="SagaAbandonedCopies"/>, the model's
/// <c>left' = (left \cup {bound}) \ {alias}</c> (issue #4689).
/// </summary>
public class SagaAbandonedCopiesTests
{
    private const string Previous = "orders";
    private const string Shadow = "orders-bkprestore-1";

    [Test]
    public void Record_adds_the_copy_left_with_the_shards_touched_there()
    {
        var record = SagaAbandonedCopies.Record(null, Previous, [3, 1], Shadow);

        Assert.That(record, Is.Not.Null);
        Assert.That(record!.Keys, Is.EquivalentTo(new[] { Previous }));
        Assert.That(record[Previous], Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public void Record_joins_the_shards_of_a_copy_left_twice()
    {
        var once = SagaAbandonedCopies.Record(null, Previous, [1], Shadow);
        var back = SagaAbandonedCopies.Record(once, Shadow, [0], Previous);
        Assert.That(back!.Keys, Is.EquivalentTo(new[] { Shadow }),
            "re-binding back onto a copy removes it: its prepares are the saga's own again");

        var twice = SagaAbandonedCopies.Record(
            new Dictionary<string, List<int>> { [Previous] = [1] }, Previous, [2], Shadow);
        Assert.That(twice![Previous], Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void Record_is_empty_for_an_unbound_saga_or_a_rebind_onto_the_same_copy()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SagaAbandonedCopies.Record(null, null, [1], Shadow), Is.Null);
            Assert.That(SagaAbandonedCopies.Record(null, Shadow, [1], Shadow), Is.Null);
        });
    }

    [Test]
    public void Record_never_mutates_the_previous_record()
    {
        var previous = new Dictionary<string, List<int>> { [Previous] = [1] };

        _ = SagaAbandonedCopies.Record(previous, Previous, [2], Shadow);
        _ = SagaAbandonedCopies.Record(previous, Shadow, [0], Previous);

        Assert.That(previous[Previous], Is.EqualTo(new[] { 1 }));
        Assert.That(previous.Keys, Is.EquivalentTo(new[] { Previous }));
    }

    [Test]
    public void Record_validates_its_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => SagaAbandonedCopies.Record(null, Previous, null!, Shadow));
        Assert.Throws<ArgumentNullException>(() => SagaAbandonedCopies.Record(null, Previous, [1], null!));
    }
}
