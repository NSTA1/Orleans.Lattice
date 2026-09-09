using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Round-trip accounting for the tag index's write and intersection-query
/// paths, asserted against a counting in-memory <see cref="ILattice"/>.
/// </summary>
/// <remarks>
/// These are the claims a benchmark structurally cannot make. A microbenchmark
/// over an in-memory store prices a round trip at close to zero, so a change
/// that trades many small calls for one larger call shows up there as flat or
/// worse; the property that actually matters - how many times the index calls
/// its backing tree - is only observable by counting the calls. Every
/// assertion below is therefore a count, and each names the pre-batching figure
/// it replaced so a regression reads as a number moving back rather than as an
/// opaque failure.
/// </remarks>
[TestFixture]
public class LatticeTagIndexRoundTripBatchingTests
{
    private const string TreeId = "subject-tree";
    private const string IndexName = "idx";

    /// <summary>
    /// An in-memory <see cref="ILattice"/> substitute that records how many
    /// times each batching-relevant member was called, plus the batch widths it
    /// was handed.
    /// </summary>
    private sealed class CountingTree
    {
        public readonly SortedDictionary<string, byte[]> Data = new(StringComparer.Ordinal);
        public int SetAsyncCalls;
        public int SetManyAsyncCalls;
        public int GetManyAsyncCalls;
        public int DeleteAsyncCalls;
        public readonly List<int> SetManyWidths = [];
        public readonly List<int> GetManyWidths = [];

        /// <summary>Optional gate awaited by every delete, used to prove overlap.</summary>
        public Func<Task>? DeleteGate;

        public ILattice Lattice { get; }

        public int TotalWriteCalls => SetAsyncCalls + SetManyAsyncCalls;

        public CountingTree()
        {
            var tree = Substitute.For<ILattice>();

            tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(true));

            tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci => Task.FromResult(Data.TryGetValue(ci.Arg<string>(), out var v) ? v : null));

            tree.ExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci => Task.FromResult(Data.ContainsKey(ci.Arg<string>())));

            tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    SetAsyncCalls++;
                    Data[ci.Arg<string>()] = ci.Arg<byte[]>();
                    return Task.CompletedTask;
                });

            tree.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var entries = ci.Arg<List<KeyValuePair<string, byte[]>>>();
                    SetManyAsyncCalls++;
                    SetManyWidths.Add(entries.Count);
                    foreach (var entry in entries)
                    {
                        Data[entry.Key] = entry.Value;
                    }
                    return Task.CompletedTask;
                });

            tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var keys = ci.Arg<List<string>>();
                    GetManyAsyncCalls++;
                    GetManyWidths.Add(keys.Count);
                    var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                    foreach (var key in keys)
                    {
                        if (Data.TryGetValue(key, out var v))
                        {
                            result[key] = v;
                        }
                    }
                    return Task.FromResult(result);
                });

            tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    Interlocked.Increment(ref DeleteAsyncCalls);
                    var key = ci.Arg<string>();
                    var gate = DeleteGate;
                    if (gate is null)
                    {
                        return Task.FromResult(Data.Remove(key));
                    }
                    return AwaitGateThenRemove(gate, key);
                });

            tree.KeysAsync(
                    Arg.Any<string?>(),
                    Arg.Any<string?>(),
                    Arg.Any<bool>(),
                    Arg.Any<bool?>(),
                    Arg.Any<CancellationToken>())
                .Returns(ci => Range(ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<bool>(2)));

            Lattice = tree;
        }

        private async Task<bool> AwaitGateThenRemove(Func<Task> gate, string key)
        {
            await gate().ConfigureAwait(false);
            lock (Data)
            {
                return Data.Remove(key);
            }
        }

        private async IAsyncEnumerable<string> Range(string? startInclusive, string? endExclusive, bool reverse)
        {
            List<string> keys;
            lock (Data)
            {
                keys = [.. Data.Keys];
            }
            if (reverse)
            {
                keys.Reverse();
            }
            foreach (var key in keys)
            {
                if (startInclusive is not null && string.CompareOrdinal(key, startInclusive) < 0)
                {
                    continue;
                }
                if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
                {
                    continue;
                }
                yield return key;
                await Task.Yield();
            }
        }
    }

    private static (CountingTree tree, ILatticeTagIndex index) Create()
    {
        var counting = new CountingTree();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(counting.Lattice);
        // Built through the coordinator entry point because it takes its trees
        // from the grain factory rather than calling GetPrimaryKeyString on a
        // caller-supplied reference, which a substitute cannot answer. The
        // resulting context is the same type the public single-tree surface
        // returns, exercising the same Key / WithAllTags implementations.
        return (counting, LatticeTagIndexContext.CreateForCoordinator(grainFactory, IndexName));
    }

    // ── Add path: 2N sequential writes collapse into one batched write ──

    [Test]
    public async Task Add_writes_every_membership_row_in_a_single_batched_call()
    {
        var (tree, index) = Create();
        string[] tags = ["red", "green", "blue", "amber", "violet"];

        await index.Key("k1").AddAsync(tags);

        Assert.Multiple(() =>
        {
            // One batch, carrying the tag-major row and its key-major mirror
            // for each of the five tags. Before batching this path issued ten
            // separate SetAsync calls.
            Assert.That(tree.SetManyAsyncCalls, Is.EqualTo(1));
            Assert.That(tree.SetManyWidths, Is.EqualTo(new[] { tags.Length * 2 }));

            // The only remaining single write is the covered-tree marker, which
            // is written once per tree and is not part of the tag fan-out.
            Assert.That(tree.SetAsyncCalls, Is.EqualTo(1));
            Assert.That(tree.TotalWriteCalls, Is.EqualTo(2), "a five-tag add cost eleven writes before batching");
        });
    }

    [Test]
    public async Task Add_round_trip_count_does_not_grow_with_the_tag_count()
    {
        var (tree, index) = Create();

        await index.Key("k1").AddAsync(["a"]);
        var afterFirst = tree.TotalWriteCalls;

        await index.Key("k2").AddAsync(["a", "b", "c", "d", "e", "f", "g", "h"]);
        var afterEight = tree.TotalWriteCalls - afterFirst;

        // One batched write either way: the marker is already cached after the
        // first add, so an eight-tag add costs exactly the same one call a
        // one-tag add does. Sequentially it would have cost sixteen.
        Assert.That(afterEight, Is.EqualTo(1));
    }

    [Test]
    public async Task Add_stores_the_same_rows_the_sequential_path_stored()
    {
        var (tree, index) = Create();
        await index.Key("k1").AddAsync(["red", "blue"]);

        var stored = await index.Key("k1").GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(stored, Is.EquivalentTo(new[] { "red", "blue" }));
            // Two tag-major rows, two key-major mirrors, one covered marker.
            Assert.That(tree.Data, Has.Count.EqualTo(5));
        });
    }

    [Test]
    public async Task Add_rejects_an_invalid_tag_before_writing_any_row()
    {
        var (tree, index) = Create();

        Assert.That(
            async () => await index.Key("k1").AddAsync(["good", "al\0so-bad"]),
            Throws.ArgumentException);

        // Validation is hoisted ahead of the batch, so a rejected tag late in
        // the list cannot leave the earlier tags durably written.
        Assert.Multiple(() =>
        {
            Assert.That(tree.SetManyAsyncCalls, Is.Zero);
            Assert.That(tree.Data.Keys, Has.No.Member("good"));
        });
    }

    // ── Remove path: the row deletions overlap instead of serialising ──

    [Test]
    public async Task Remove_issues_its_row_deletions_concurrently()
    {
        var (tree, index) = Create();
        string[] tags = ["red", "green", "blue", "amber", "violet"];
        await index.Key("k1").AddAsync(tags);

        // Ten rows to delete. The gate does not release until all ten deletes
        // are simultaneously in flight, so a sequential implementation cannot
        // finish: the first delete would block forever waiting for a tenth that
        // is never issued. Completing at all is the proof of overlap.
        const int expected = 10;
        var arrived = 0;
        var allInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tree.DeleteGate = () =>
        {
            if (Interlocked.Increment(ref arrived) == expected)
            {
                allInFlight.SetResult();
            }
            return allInFlight.Task;
        };

        var remove = index.Key("k1").RemoveAsync(tags);
        var finished = await Task.WhenAny(remove, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.That(finished, Is.SameAs(remove), "the row deletions did not overlap; they were issued sequentially");
        await remove;

        Assert.Multiple(() =>
        {
            Assert.That(tree.DeleteAsyncCalls, Is.EqualTo(expected));
            Assert.That(arrived, Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task Remove_deletes_exactly_the_rows_the_sequential_path_deleted()
    {
        var (tree, index) = Create();
        await index.Key("k1").AddAsync(["red", "green", "blue"]);
        await index.Key("k2").AddAsync(["red"]);

        await index.Key("k1").RemoveAsync(["red", "blue"]);

        Assert.Multiple(async () =>
        {
            Assert.That(await index.Key("k1").GetAsync(), Is.EquivalentTo(new[] { "green" }));
            Assert.That(await index.Key("k2").GetAsync(), Is.EquivalentTo(new[] { "red" }));
            Assert.That(tree.DeleteAsyncCalls, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task Remove_of_a_single_tag_stays_on_the_direct_path()
    {
        var (tree, index) = Create();
        await index.Key("k1").AddAsync(["red", "green"]);

        await index.Key("k1").RemoveAsync(["red"]);

        Assert.Multiple(async () =>
        {
            Assert.That(tree.DeleteAsyncCalls, Is.EqualTo(2));
            Assert.That(await index.Key("k1").GetAsync(), Is.EquivalentTo(new[] { "green" }));
        });
    }

    [Test]
    public async Task Remove_rejects_an_invalid_tag_before_deleting_any_row()
    {
        var (tree, index) = Create();
        await index.Key("k1").AddAsync(["red", "green"]);

        Assert.That(
            async () => await index.Key("k1").RemoveAsync(["red", "gr\0een"]),
            Throws.ArgumentException);

        Assert.Multiple(async () =>
        {
            Assert.That(tree.DeleteAsyncCalls, Is.Zero);
            Assert.That(await index.Key("k1").GetAsync(), Is.EquivalentTo(new[] { "red", "green" }));
        });
    }

    // ── Intersection query: candidates are probed a window at a time ──

    [Test]
    public async Task Intersection_query_probes_candidates_in_windows()
    {
        var (tree, index) = Create();
        const int candidates = 70;
        for (var i = 0; i < candidates; i++)
        {
            await index.Key($"k{i:D3}").AddAsync(["common", "second", "third"]);
        }

        tree.GetManyAsyncCalls = 0;
        tree.GetManyWidths.Clear();

        var matched = new List<string>();
        await foreach (var key in index.WithAllTags("common", "second", "third"))
        {
            matched.Add(key);
        }

        var window = LatticeTagIndexContext.AndQueryCandidateWindow;
        var expectedCalls = (candidates + window - 1) / window;

        Assert.Multiple(() =>
        {
            Assert.That(matched, Has.Count.EqualTo(candidates));
            // Probing one candidate at a time cost one call per candidate: 70.
            Assert.That(tree.GetManyAsyncCalls, Is.EqualTo(expectedCalls));
            // Each full window carries two sibling-tag rows per candidate.
            Assert.That(tree.GetManyWidths[0], Is.EqualTo(window * 2));
            // The number of rows read is unchanged, only the call count fell.
            Assert.That(tree.GetManyWidths.Sum(), Is.EqualTo(candidates * 2));
        });
    }

    [Test]
    public async Task Intersection_query_preserves_posting_list_order_across_windows()
    {
        var (_, index) = Create();
        const int candidates = 70;
        var expected = new List<string>();
        for (var i = 0; i < candidates; i++)
        {
            var key = $"k{i:D3}";
            await index.Key(key).AddAsync(["common", "second"]);
            expected.Add(key);
        }

        var matched = new List<string>();
        await foreach (var key in index.WithAllTags("common", "second"))
        {
            matched.Add(key);
        }

        // The posting list is ordinally ordered and a window emits in the order
        // it buffered, so windowing must not reorder the result.
        Assert.That(matched, Is.EqualTo(expected));
    }

    [Test]
    public async Task Intersection_query_excludes_candidates_missing_a_tag_in_any_window_position()
    {
        var (_, index) = Create();
        const int candidates = 70;
        var expected = new List<string>();
        for (var i = 0; i < candidates; i++)
        {
            var key = $"k{i:D3}";
            // Drop the second tag from the first, last, and window-boundary
            // candidates so a windowing off-by-one is caught wherever it sits.
            var drop = i is 0 or 31 or 32 or 69;
            await index.Key(key).AddAsync(drop ? ["common"] : ["common", "second"]);
            if (!drop)
            {
                expected.Add(key);
            }
        }

        var matched = new List<string>();
        await foreach (var key in index.WithAllTags("common", "second"))
        {
            matched.Add(key);
        }

        Assert.That(matched, Is.EqualTo(expected));
    }

    [Test]
    public async Task Intersection_query_handles_a_partial_final_window()
    {
        var (tree, index) = Create();
        const int candidates = 5;
        for (var i = 0; i < candidates; i++)
        {
            await index.Key($"k{i:D3}").AddAsync(["common", "second"]);
        }

        tree.GetManyAsyncCalls = 0;
        tree.GetManyWidths.Clear();

        var matched = new List<string>();
        await foreach (var key in index.WithAllTags("common", "second"))
        {
            matched.Add(key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Has.Count.EqualTo(candidates));
            Assert.That(tree.GetManyAsyncCalls, Is.EqualTo(1));
            Assert.That(tree.GetManyWidths, Is.EqualTo(new[] { candidates }));
        });
    }
}
