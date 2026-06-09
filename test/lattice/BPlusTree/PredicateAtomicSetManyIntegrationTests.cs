using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Multi-shard integration coverage for the guarded atomic bulk write
/// (<see cref="TypedLatticeExtensions.SetManyAtomicAsync{T}(ILattice, System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, T}}, System.Linq.Expressions.Expression{System.Func{T, bool}}, CancellationToken)"/>).
/// The whole batch commits all-or-nothing, gated on every targeted key's
/// pre-saga value satisfying the guard predicate; a single non-matching key
/// (or a key with no live value) aborts the saga and commits nothing. The
/// four-shard fixture spreads the keys across shards so the saga's per-shard
/// pre-saga capture and guard evaluation are exercised.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PredicateAtomicSetManyIntegrationTests
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

    private static string KeyOf(int i) => $"ga-{i:D5}";

    private async Task<ILattice> SeededAsync(string id, int seedScore)
    {
        var tree = await _fixture.CreateTreeAsync(id);
        for (int i = 0; i < Count; i++)
            await tree.SetAsync(KeyOf(i), new Doc(i, seedScore));
        return tree;
    }

    private static List<KeyValuePair<string, Doc>> MarkerBatch(int marker)
    {
        var entries = new List<KeyValuePair<string, Doc>>(Count);
        for (int i = 0; i < Count; i++)
            entries.Add(new(KeyOf(i), new Doc(i, marker)));
        return entries;
    }

    [Test]
    public async Task Guarded_atomic_commits_whole_batch_when_every_key_matches()
    {
        var tree = await SeededAsync($"ga-commit-{Guid.NewGuid():N}", seedScore: 1000);

        var outcome = await tree.SetManyAtomicAsync<Doc>(MarkerBatch(7777), d => d.Score >= 500);

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.Committed));
        for (int i = 0; i < Count; i++)
            Assert.That((await tree.GetAsync<Doc>(KeyOf(i)))!.Score, Is.EqualTo(7777),
                $"{KeyOf(i)} must carry the committed marker");
    }

    [Test]
    public async Task Guarded_atomic_aborts_whole_batch_when_one_key_fails()
    {
        var tree = await SeededAsync($"ga-abort-{Guid.NewGuid():N}", seedScore: 1000);
        // Drop a single key below the guard so the whole batch must abort.
        await tree.SetAsync(KeyOf(17), new Doc(17, 0));

        var outcome = await tree.SetManyAtomicAsync<Doc>(MarkerBatch(7777), d => d.Score >= 500);

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
        // Nothing was committed: every key keeps its pre-saga value.
        for (int i = 0; i < Count; i++)
        {
            var expected = i == 17 ? 0 : 1000;
            Assert.That((await tree.GetAsync<Doc>(KeyOf(i)))!.Score, Is.EqualTo(expected),
                $"{KeyOf(i)} must be unchanged after a precondition failure");
        }
    }

    [Test]
    public async Task Guarded_atomic_treats_missing_key_as_non_match()
    {
        var tree = await SeededAsync($"ga-missing-{Guid.NewGuid():N}", seedScore: 1000);

        var entries = MarkerBatch(7777);
        entries.Add(new("ga-99999", new Doc(99999, 7777)));  // absent pre-saga value

        var outcome = await tree.SetManyAtomicAsync<Doc>(entries, d => d.Score >= 500);

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
        Assert.That(await tree.GetAsync<Doc>("ga-99999"), Is.Null);
        for (int i = 0; i < Count; i++)
            Assert.That((await tree.GetAsync<Doc>(KeyOf(i)))!.Score, Is.EqualTo(1000),
                $"{KeyOf(i)} must be unchanged after a precondition failure");
    }

    [Test]
    public async Task Guarded_atomic_idempotency_key_reattach_returns_memoized_outcome()
    {
        var tree = await SeededAsync($"ga-idem-{Guid.NewGuid():N}", seedScore: 1000);
        await tree.SetAsync(KeyOf(3), new Doc(3, 0));  // forces a precondition failure
        var opId = Guid.NewGuid().ToString("N");

        var first = await tree.SetManyAtomicAsync<Doc>(MarkerBatch(7777), d => d.Score >= 500, opId);
        Assert.That(first, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));

        // The data now satisfies the guard, but re-attaching with the same
        // operationId must return the original memoized outcome, not re-evaluate.
        await tree.SetAsync(KeyOf(3), new Doc(3, 1000));
        var second = await tree.SetManyAtomicAsync<Doc>(MarkerBatch(7777), d => d.Score >= 500, opId);

        Assert.That(second, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
        // Still nothing committed from the guarded batch.
        Assert.That((await tree.GetAsync<Doc>(KeyOf(0)))!.Score, Is.EqualTo(1000));
    }

    [Test]
    public async Task Guarded_atomic_empty_batch_returns_committed()
    {
        var tree = await _fixture.CreateTreeAsync($"ga-empty-{Guid.NewGuid():N}");

        var outcome = await tree.SetManyAtomicAsync<Doc>(
            new List<KeyValuePair<string, Doc>>(), d => d.Score >= 500);

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.Committed));
        Assert.That(await tree.CountAsync(), Is.EqualTo(0));
    }
}
