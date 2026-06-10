using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- SetManyWherePredicateAsync (P-007 conditional bulk write guard) ---

    private sealed record Scored(int Score);

    private static byte[] ScoredJson(int score) => Encoding.UTF8.GetBytes($"{{\"Score\":{score}}}");

    private static LatticePredicateNode ScoreAtLeast(int threshold) =>
        LatticePredicatePushdown.Compile<Scored>(
            s => s.Score >= threshold, JsonLatticeSerializer<Scored>.Default);

    private static KeyValuePair<string, byte[]> Kv(string key, int score) => new(key, ScoredJson(score));

    [Test]
    public async Task SetManyWherePredicate_writes_only_entries_whose_current_value_matches()
    {
        var grain = CreateGrain();
        // Seed current values: a=10 (guarded out), b=50 (matches), c=5 (guarded out).
        await grain.SetAsync("a", ScoredJson(10));
        await grain.SetAsync("b", ScoredJson(50));
        await grain.SetAsync("c", ScoredJson(5));

        var result = await grain.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>> { Kv("a", 99), Kv("b", 99), Kv("c", 99) },
            ScoreAtLeast(40));

        Assert.That(result.WrittenKeys, Is.EquivalentTo(new[] { "b" }));
        Assert.That(result.Split, Is.Null);
        // Only b was overwritten; a and c retain their seeded values.
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("a"))!), Is.EqualTo("{\"Score\":10}"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("{\"Score\":99}"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("c"))!), Is.EqualTo("{\"Score\":5}"));
    }

    [Test]
    public async Task SetManyWherePredicate_skips_missing_keys()
    {
        var grain = CreateGrain();
        await grain.SetAsync("present", ScoredJson(100));

        var result = await grain.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>> { Kv("present", 1), Kv("absent", 1) },
            ScoreAtLeast(50));

        // 'absent' has no current value, so it is treated as non-matching and skipped.
        Assert.That(result.WrittenKeys, Is.EquivalentTo(new[] { "present" }));
        Assert.That(await grain.GetAsync("absent"), Is.Null);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("present"))!), Is.EqualTo("{\"Score\":1}"));
    }

    [Test]
    public async Task SetManyWherePredicate_skips_tombstoned_keys()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", ScoredJson(100));
        await grain.DeleteAsync("k");

        var result = await grain.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>> { Kv("k", 7) },
            ScoreAtLeast(1));

        Assert.That(result.WrittenKeys, Is.Empty);
        Assert.That(await grain.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task SetManyWherePredicate_no_matches_returns_empty_and_writes_nothing()
    {
        var writer = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: writer);
        await grain.SetAsync("a", ScoredJson(1));
        await grain.SetAsync("b", ScoredJson(2));
        var appendsAfterSeed = writer.AppendCount;

        var result = await grain.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>> { Kv("a", 9), Kv("b", 9) },
            ScoreAtLeast(1000));

        Assert.That(result.WrittenKeys, Is.Empty);
        Assert.That(result.Split, Is.Null);
        Assert.That(writer.AppendCount, Is.EqualTo(appendsAfterSeed),
            "A fully guarded-out batch must not append anything to the commit log.");
    }

    [Test]
    public async Task SetManyWherePredicate_matched_subset_commits_as_single_batched_append()
    {
        var writer = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: writer);
        for (var i = 0; i < 8; i++)
            await grain.SetAsync($"k{i:D2}", ScoredJson(i * 10));
        var appendManyBefore = writer.AppendManyCallCount;

        // Matches k05..k07 (scores 50,60,70 >= 50): three entries.
        var result = await grain.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>>
            {
                Kv("k00", 999), Kv("k01", 999), Kv("k02", 999), Kv("k03", 999),
                Kv("k04", 999), Kv("k05", 999), Kv("k06", 999), Kv("k07", 999),
            },
            ScoreAtLeast(50));

        Assert.That(result.WrittenKeys, Is.EquivalentTo(new[] { "k05", "k06", "k07" }));
        Assert.That(writer.AppendManyCallCount, Is.EqualTo(appendManyBefore + 1),
            "The matched subset must be committed through one batched commit-log call.");
    }

    [Test]
    public async Task SetManyWherePredicate_empty_list_is_noop()
    {
        var grain = CreateGrain();
        var result = await grain.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>>(), ScoreAtLeast(1));
        Assert.That(result.WrittenKeys, Is.Empty);
        Assert.That(result.Split, Is.Null);
    }

    [Test]
    public void SetManyWherePredicate_throws_on_null_entries()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.SetManyWherePredicateAsync(null!, ScoreAtLeast(1)),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task SetManyWherePredicate_overwriting_existing_keys_never_splits()
    {
        // A conditional write only ever overwrites keys that already have a
        // live value (missing keys are skipped), so it is always an in-place
        // update that cannot grow the leaf past MaxLeafKeys - no split can
        // occur even when every entry matches.
        var grain = CreateGrain(maxLeafKeys: 4);
        var entries = new List<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < 4; i++)
        {
            await grain.SetAsync($"k{i:D2}", ScoredJson(100));
            entries.Add(Kv($"k{i:D2}", 200));
        }

        var result = await grain.SetManyWherePredicateAsync(entries, ScoreAtLeast(1));

        Assert.That(result.WrittenKeys, Has.Count.EqualTo(4));
        Assert.That(result.Split, Is.Null);
        for (var i = 0; i < 4; i++)
            Assert.That(Encoding.UTF8.GetString((await grain.GetAsync($"k{i:D2}"))!), Is.EqualTo("{\"Score\":200}"));
    }
}
