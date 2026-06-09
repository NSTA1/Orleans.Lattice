using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Deterministic multi-page / multi-shard coverage for the conditional
/// range-delete push-down overload
/// (<see cref="TypedLatticeExtensions.DeleteRangeAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string, string, CancellationToken)"/>).
/// The four-shard fixture pins <c>MaxLeafKeys = 4</c>, so seeding a few dozen
/// entries forces many leaves across several shards. The predicate is evaluated
/// once at write time on each owning leaf, which tombstones only the in-range
/// keys whose value satisfies it. With no concurrency in play any drift is a
/// pure push-down bug rather than a race.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PredicateRangeDeleteIntegrationTests
{
    private sealed record Scored(int Index, int Score);

    private FourShardClusterFixture _fixture = null!;

    private const int Count = 80;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"rd-{i:D5}";

    private async Task<ILattice> SeededAsync(string id)
    {
        var tree = await _fixture.CreateTreeAsync(id);
        for (int i = 0; i < Count; i++)
            await tree.SetAsync(KeyOf(i), new Scored(i, i));
        return tree;
    }

    [Test]
    public async Task DeleteRange_predicate_tombstones_only_matching_keys_in_range()
    {
        var tree = await SeededAsync($"rd-match-{Guid.NewGuid():N}");

        // Delete keys with Score < 30 in [10, 50); higher-scored in-range keys
        // stay, and nothing outside the half-open range is touched even if it
        // would match.
        var deleted = await tree.DeleteRangeAsync<Scored>(
            s => s.Score < 30,
            startInclusive: KeyOf(10),
            endExclusive: KeyOf(50));

        // Score == Index, so in-range matches are indices [10, 30).
        var expectedDeleted = Enumerable.Range(10, 40).Count(i => i < 30);
        Assert.That(deleted, Is.EqualTo(expectedDeleted), "must report exactly the matched in-range count");

        for (int i = 0; i < Count; i++)
        {
            var present = await tree.GetAsync<Scored>(KeyOf(i));
            var shouldBeDeleted = i >= 10 && i < 50 && i < 30;
            if (shouldBeDeleted)
                Assert.That(present, Is.Null, $"{KeyOf(i)} matched the predicate in range and must be tombstoned");
            else
                Assert.That(present, Is.Not.Null, $"{KeyOf(i)} must survive (out of range or non-matching)");
        }
    }

    [Test]
    public async Task DeleteRange_predicate_matching_zero_keys_deletes_nothing()
    {
        var tree = await SeededAsync($"rd-zero-{Guid.NewGuid():N}");

        var deleted = await tree.DeleteRangeAsync<Scored>(
            s => s.Score > 1_000_000,
            startInclusive: KeyOf(0),
            endExclusive: KeyOf(Count));

        Assert.That(deleted, Is.Zero);
        for (int i = 0; i < Count; i++)
            Assert.That(await tree.GetAsync<Scored>(KeyOf(i)), Is.Not.Null, $"{KeyOf(i)} must be untouched");
    }

    [Test]
    public async Task DeleteRange_predicate_respects_range_bounds()
    {
        var tree = await SeededAsync($"rd-bounds-{Guid.NewGuid():N}");

        // Predicate matches everything, but the range is a narrow window: only
        // keys inside [20, 25) may be tombstoned.
        var deleted = await tree.DeleteRangeAsync<Scored>(
            s => s.Score >= 0,
            startInclusive: KeyOf(20),
            endExclusive: KeyOf(25));

        Assert.That(deleted, Is.EqualTo(5));
        for (int i = 0; i < Count; i++)
        {
            var present = await tree.GetAsync<Scored>(KeyOf(i));
            if (i >= 20 && i < 25)
                Assert.That(present, Is.Null, $"{KeyOf(i)} in range and matching must be tombstoned");
            else
                Assert.That(present, Is.Not.Null, $"{KeyOf(i)} outside range must survive");
        }
    }

    [Test]
    public async Task DeleteRange_predicate_is_idempotent_on_repeat()
    {
        var tree = await SeededAsync($"rd-idem-{Guid.NewGuid():N}");

        var first = await tree.DeleteRangeAsync<Scored>(
            s => s.Score < 30,
            startInclusive: KeyOf(0),
            endExclusive: KeyOf(Count));
        Assert.That(first, Is.EqualTo(30));

        // Second pass matches nothing live - the rows are already tombstoned.
        var second = await tree.DeleteRangeAsync<Scored>(
            s => s.Score < 30,
            startInclusive: KeyOf(0),
            endExclusive: KeyOf(Count));
        Assert.That(second, Is.Zero, "already-tombstoned keys are not live and must not be re-counted");

        for (int i = 0; i < 30; i++)
            Assert.That(await tree.GetAsync<Scored>(KeyOf(i)), Is.Null);
        for (int i = 30; i < Count; i++)
            Assert.That(await tree.GetAsync<Scored>(KeyOf(i)), Is.Not.Null);
    }
}
