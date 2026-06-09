using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Deterministic multi-page / multi-shard coverage for the conditional
/// resumable range-delete cursor
/// (<see cref="TypedLatticeExtensions.OpenDeleteRangeCursorAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string, string, CancellationToken)"/>).
/// The cursor steps page by page, tombstoning only the in-range keys whose
/// value satisfies the predicate, while non-matching in-range keys and
/// out-of-range keys survive. The four-shard fixture pins <c>MaxLeafKeys = 4</c>
/// so the bounded steps span several leaves across shards.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PredicateDeleteRangeCursorIntegrationTests
{
    private sealed record Scored(int Index, int Score);

    private FourShardClusterFixture _fixture = null!;

    private const int Count = 60;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"dc-{i:D5}";

    private async Task<ILattice> SeededAsync(string id)
    {
        var tree = await _fixture.CreateTreeAsync(id);
        for (int i = 0; i < Count; i++)
            await tree.SetAsync(KeyOf(i), new Scored(i, i));
        return tree;
    }

    [Test]
    public async Task Conditional_delete_cursor_tombstones_only_matching_keys_page_by_page()
    {
        var tree = await SeededAsync($"dc-match-{Guid.NewGuid():N}");

        // Delete keys with Score >= 30 across the whole tree, in bounded steps.
        var cursorId = await tree.OpenDeleteRangeCursorAsync<Scored>(
            s => s.Score >= 30,
            startInclusive: KeyOf(0),
            endExclusive: KeyOf(Count));

        var steps = 0;
        var lastTotal = 0;
        while (true)
        {
            var progress = await tree.DeleteRangeStepAsync(cursorId, 5);
            lastTotal = progress.DeletedTotal;
            steps++;
            if (progress.IsComplete) break;
            Assert.That(steps, Is.LessThan(100), "guard against a non-terminating cursor");
        }

        var expectedDeleted = Enumerable.Range(0, Count).Count(i => i >= 30);
        Assert.That(lastTotal, Is.EqualTo(expectedDeleted));
        Assert.That(steps, Is.GreaterThan(1), "bounded steps must page the delete");

        for (int i = 0; i < Count; i++)
        {
            var present = await tree.GetAsync<Scored>(KeyOf(i));
            if (i >= 30)
                Assert.That(present, Is.Null, $"{KeyOf(i)} matched and must be tombstoned");
            else
                Assert.That(present, Is.Not.Null, $"{KeyOf(i)} did not match and must survive");
        }
    }

    [Test]
    public async Task Conditional_delete_cursor_respects_range_bounds()
    {
        var tree = await SeededAsync($"dc-bounds-{Guid.NewGuid():N}");

        // Predicate matches everything, but only [20, 40) is in range.
        var cursorId = await tree.OpenDeleteRangeCursorAsync<Scored>(
            s => s.Score >= 0,
            startInclusive: KeyOf(20),
            endExclusive: KeyOf(40));

        while (true)
        {
            var progress = await tree.DeleteRangeStepAsync(cursorId, 7);
            if (progress.IsComplete)
            {
                Assert.That(progress.DeletedTotal, Is.EqualTo(20));
                break;
            }
        }

        for (int i = 0; i < Count; i++)
        {
            var present = await tree.GetAsync<Scored>(KeyOf(i));
            if (i >= 20 && i < 40)
                Assert.That(present, Is.Null, $"{KeyOf(i)} in range and matching must be tombstoned");
            else
                Assert.That(present, Is.Not.Null, $"{KeyOf(i)} outside range must survive");
        }
    }

    [Test]
    public async Task Conditional_delete_cursor_matching_nothing_completes_without_deleting()
    {
        var tree = await SeededAsync($"dc-none-{Guid.NewGuid():N}");

        var cursorId = await tree.OpenDeleteRangeCursorAsync<Scored>(
            s => s.Score > 1_000_000,
            startInclusive: KeyOf(0),
            endExclusive: KeyOf(Count));

        var progress = await tree.DeleteRangeStepAsync(cursorId, 10);

        Assert.That(progress.IsComplete, Is.True);
        Assert.That(progress.DeletedTotal, Is.Zero);
        for (int i = 0; i < Count; i++)
            Assert.That(await tree.GetAsync<Scored>(KeyOf(i)), Is.Not.Null);
    }
}
