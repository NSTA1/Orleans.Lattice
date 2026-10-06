namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4733: a chunk of an origin's cross-tree purge frontier arrives as
/// peer input in a call header, so it is parsed strictly and bounded; a
/// receiver must never drop a tombstone on a value the origin did not
/// advertise.
/// </summary>
[TestFixture]
public class CrossTreePurgeFrontierTests
{
    [Test]
    public void The_text_form_round_trips_any_tree_id()
    {
        var frontier = new CrossTreePurgeFrontier
        {
            Frontiers = System.Collections.Immutable.ImmutableDictionary.CreateRange(
                StringComparer.Ordinal,
                [new KeyValuePair<string, long>("tree:a|b,c", 7), new KeyValuePair<string, long>("other", 0)]),
        };

        Assert.That(CrossTreePurgeFrontier.TryParse(frontier.ToText(), out var parsed), Is.True);
        Assert.That(parsed!.Frontiers, Is.EquivalentTo(frontier.Frontiers));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("1|")]
    [TestCase("2|dHJlZQ==:1")]
    [TestCase("1|dHJlZQ==")]
    [TestCase("1|dHJlZQ==:-1")]
    [TestCase("1|dHJlZQ==:x")]
    [TestCase("1|not base64:1")]
    [TestCase("1|:1")]
    [TestCase("1|dHJlZQ==:1,dHJlZQ==:2")]
    public void Anything_but_the_canonical_form_is_refused(string? text)
    {
        Assert.That(CrossTreePurgeFrontier.TryParse(text, out var parsed), Is.False);
        Assert.That(parsed, Is.Null);
    }

    [Test]
    public void Too_many_trees_and_over_long_text_are_refused()
    {
        var tooMany = "1|" + string.Join(',', Enumerable.Range(0, CrossTreePurgeFrontier.MaxEntries + 1)
            .Select(i => Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("t" + i)) + ":1"));
        var tooLong = "1|dHJlZQ==:" + new string('1', CrossTreePurgeFrontier.MaxTextLength);
        Assert.Multiple(() =>
        {
            Assert.That(CrossTreePurgeFrontier.TryParse(tooMany, out _), Is.False);
            Assert.That(CrossTreePurgeFrontier.TryParse(tooLong, out _), Is.False);
        });
    }
}
