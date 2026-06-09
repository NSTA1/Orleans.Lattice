using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Deterministic multi-page / multi-shard coverage for the streaming-scan
/// predicate push-down overloads. The four-shard fixture pins
/// <c>MaxLeafKeys = 4</c>, so seeding a few dozen entries forces many leaves
/// and several pages per shard. This proves the predicate is re-applied on
/// every enumerator turn (not just the first page), with no concurrency in
/// play so any drift is a pure push-down bug rather than a race.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PredicateScanMultiPageIntegrationTests
{
    private sealed record Scored(int Index, int Score);

    private FourShardClusterFixture _fixture = null!;

    private const int Count = 80;
    private const int Threshold = 40;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"mp-{i:D5}";

    private async Task<ILattice> SeededAsync(string id)
    {
        var tree = await _fixture.CreateTreeAsync(id);
        for (int i = 0; i < Count; i++)
            await tree.SetAsync(KeyOf(i), new Scored(i, i));
        return tree;
    }

    [Test]
    public async Task ScanKeys_predicate_filters_across_all_pages()
    {
        var tree = await SeededAsync($"mp-keys-{Guid.NewGuid():N}");
        var expected = Enumerable.Range(0, Count).Where(i => i < Threshold).Select(KeyOf).ToList();

        var got = new List<string>();
        await foreach (var k in tree.ScanKeysAsync<Scored>(s => s.Score < Threshold))
            got.Add(k);

        Assert.That(got, Is.EqualTo(expected));
    }

    [Test]
    public async Task ScanEntries_predicate_filters_across_all_pages()
    {
        var tree = await SeededAsync($"mp-entries-{Guid.NewGuid():N}");
        var expected = Enumerable.Range(0, Count).Where(i => i < Threshold).Select(KeyOf).ToList();

        var got = new List<string>();
        await foreach (var e in tree.ScanEntriesAsync<Scored>(s => s.Score < Threshold))
        {
            Assert.That(e.Value.Score, Is.LessThan(Threshold));
            got.Add(e.Key);
        }

        Assert.That(got, Is.EqualTo(expected));
    }

    [Test]
    public async Task ScanValues_predicate_filters_across_all_pages()
    {
        var tree = await SeededAsync($"mp-values-{Guid.NewGuid():N}");
        var matchCount = Enumerable.Range(0, Count).Count(i => i >= Threshold);

        var got = new List<int>();
        await foreach (var v in tree.ScanValuesAsync<Scored>(s => s.Score >= Threshold))
        {
            Assert.That(v.Score, Is.GreaterThanOrEqualTo(Threshold));
            got.Add(v.Score);
        }

        Assert.That(got, Has.Count.EqualTo(matchCount));
    }

    [Test]
    public async Task Concurrent_filtered_scans_on_shared_activation_do_not_leak()
    {
        var tree = await SeededAsync($"mp-concurrent-{Guid.NewGuid():N}");
        var expected = Enumerable.Range(0, Count).Where(i => i < Threshold).Select(KeyOf).ToList();

        var failures = new System.Collections.Concurrent.ConcurrentBag<string>();
        var tasks = new List<Task>();
        for (int w = 0; w < 8; w++)
        {
            tasks.Add(Task.Run(async () =>
            {
                for (int pass = 0; pass < 6; pass++)
                {
                    var got = new List<string>();
                    await foreach (var k in tree.ScanKeysAsync<Scored>(s => s.Score < Threshold))
                        got.Add(k);
                    if (!got.SequenceEqual(expected, StringComparer.Ordinal))
                    {
                        var extra = got.Except(expected, StringComparer.Ordinal).Take(5);
                        failures.Add($"extra=[{string.Join(",", extra)}] count={got.Count}");
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);
        Assert.That(failures, Is.Empty, string.Join(" | ", failures.Take(10)));
    }
}
