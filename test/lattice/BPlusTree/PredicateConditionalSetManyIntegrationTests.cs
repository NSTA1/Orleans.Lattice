using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Multi-shard integration coverage for the conditional bulk write
/// (<see cref="TypedLatticeExtensions.SetManyAsync{T}(ILattice, System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, T}}, System.Linq.Expressions.Expression{System.Func{T, bool}}, CancellationToken)"/>).
/// Each entry is committed only if the key's <b>current</b> stored value
/// satisfies the guard predicate, evaluated server-side at write time. The
/// four-shard fixture spreads the keys across shards so the fan-out, per-shard
/// guard, and written-set aggregation are all exercised.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PredicateConditionalSetManyIntegrationTests
{
    private sealed record Doc(int Index, int Score);

    private FourShardClusterFixture _fixture = null!;

    private const int Count = 40;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"cw-{i:D5}";

    private async Task<ILattice> SeededAsync(string id)
    {
        var tree = await _fixture.CreateTreeAsync(id);
        for (int i = 0; i < Count; i++)
            await tree.SetAsync(KeyOf(i), new Doc(i, i));
        return tree;
    }

    [Test]
    public async Task Conditional_set_many_writes_only_keys_whose_current_value_matches()
    {
        var tree = await SeededAsync($"cw-match-{Guid.NewGuid():N}");

        // New values bump Score by 1000 so we can prove which keys were written.
        var entries = new List<KeyValuePair<string, Doc>>();
        for (int i = 0; i < Count; i++)
            entries.Add(new(KeyOf(i), new Doc(i, i + 1000)));

        // Guard: only update keys whose CURRENT score >= 20.
        var written = await tree.SetManyAsync<Doc>(entries, d => d.Score >= 20);

        var expected = Enumerable.Range(20, Count - 20).Select(KeyOf).ToArray();
        Assert.That(written, Is.EquivalentTo(expected));

        for (int i = 0; i < Count; i++)
        {
            var doc = await tree.GetAsync<Doc>(KeyOf(i));
            Assert.That(doc, Is.Not.Null);
            if (i >= 20)
                Assert.That(doc!.Score, Is.EqualTo(i + 1000), $"{KeyOf(i)} matched and must be updated");
            else
                Assert.That(doc!.Score, Is.EqualTo(i), $"{KeyOf(i)} guarded out and must be unchanged");
        }
    }

    [Test]
    public async Task Conditional_set_many_skips_missing_keys()
    {
        var tree = await SeededAsync($"cw-missing-{Guid.NewGuid():N}");

        var entries = new List<KeyValuePair<string, Doc>>
        {
            new(KeyOf(5), new Doc(5, 5000)),       // present, score 5 >= 1 => written
            new("cw-99999", new Doc(99999, 9000)), // absent => skipped
        };

        var written = await tree.SetManyAsync<Doc>(entries, d => d.Score >= 1);

        Assert.That(written, Is.EquivalentTo(new[] { KeyOf(5) }));
        Assert.That(await tree.GetAsync<Doc>("cw-99999"), Is.Null);
        Assert.That((await tree.GetAsync<Doc>(KeyOf(5)))!.Score, Is.EqualTo(5000));
    }

    [Test]
    public async Task Conditional_set_many_no_matches_writes_nothing()
    {
        var tree = await SeededAsync($"cw-none-{Guid.NewGuid():N}");

        var entries = new List<KeyValuePair<string, Doc>>();
        for (int i = 0; i < Count; i++)
            entries.Add(new(KeyOf(i), new Doc(i, i + 1000)));

        var written = await tree.SetManyAsync<Doc>(entries, d => d.Score > 1_000_000);

        Assert.That(written, Is.Empty);
        for (int i = 0; i < Count; i++)
            Assert.That((await tree.GetAsync<Doc>(KeyOf(i)))!.Score, Is.EqualTo(i));
    }
}
