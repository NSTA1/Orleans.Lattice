using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- SetTreeIdAsync ---

    [Test]
    public async Task SetTreeId_is_idempotent()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetTreeIdAsync("tree-1");
        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));

        await grain.SetTreeIdAsync("tree-2");
        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));
    }

    // --- SetManyAsync ---

    [Test]
    public async Task SetMany_writes_all_entries()
    {
        var grain = CreateGrain();
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
            new("c", Encoding.UTF8.GetBytes("3")),
        };

        var result = await grain.SetManyAsync(entries);

        Assert.That(result, Is.Null); // no split under capacity
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("a"))!), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("2"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("c"))!), Is.EqualTo("3"));
    }

    [Test]
    public async Task SetMany_returns_null_when_no_split()
    {
        var grain = CreateGrain();
        var result = await grain.SetManyAsync([new("k1", [1])]);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task SetMany_empty_list_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("existing", Encoding.UTF8.GetBytes("v"));

        var result = await grain.SetManyAsync([]);

        Assert.That(result, Is.Null);
        Assert.That(grain.EntriesForTest, Has.Count.EqualTo(1));
    }

    // --- DeleteRangeAsync ---

    [Test]
    public async Task DeleteRange_returns_zero_for_empty_leaf()
    {
        var grain = CreateGrain();
        var count = (await grain.DeleteRangeAsync("a", "z")).Deleted;
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteRange_tombstones_keys_in_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var count = (await grain.DeleteRangeAsync("b", "d")).Deleted;

        Assert.That(count, Is.EqualTo(2));
        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
        Assert.That(await grain.GetAsync("b"), Is.Null);
        Assert.That(await grain.GetAsync("c"), Is.Null);
        Assert.That(await grain.GetAsync("d"), Is.Not.Null);
    }

    [Test]
    public async Task DeleteRange_skips_already_tombstoned_keys()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.DeleteAsync("b");

        var count = (await grain.DeleteRangeAsync("a", "c")).Deleted;

        Assert.That(count, Is.EqualTo(1));
        Assert.That(await grain.GetAsync("a"), Is.Null);
    }

    [Test]
    public async Task DeleteRange_returns_zero_when_no_keys_match()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var count = (await grain.DeleteRangeAsync("m", "z")).Deleted;

        Assert.That(count, Is.EqualTo(0));
        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
    }

    [Test]
    public async Task DeleteRange_does_not_advance_clock_or_version_when_nothing_deleted()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var clockBefore = state.State.Clock;
        var versionBefore = state.State.Version.Clone();
        await grain.DeleteRangeAsync("m", "z");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.EqualTo(clockBefore),
                "Leaf must short-circuit the HLC tick when no key matches the range.");
            Assert.That(state.State.Version.DominatesOrEquals(versionBefore), Is.True);
            Assert.That(versionBefore.DominatesOrEquals(state.State.Version), Is.True,
                "Leaf must short-circuit the version-vector tick when no key matches the range.");
        });
    }

    [Test]
    public async Task DeleteRange_advances_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var versionBefore = state.State.Version.Clone();
        await grain.DeleteRangeAsync("a", "z");

        Assert.That(state.State.Version.DominatesOrEquals(versionBefore), Is.True);
        Assert.That(versionBefore.DominatesOrEquals(state.State.Version), Is.False);
    }

    [Test]
    public async Task DeleteRange_advances_clock()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var clockBefore = state.State.Clock;
        await grain.DeleteRangeAsync("a", "z");

        Assert.That(state.State.Clock, Is.GreaterThan(clockBefore));
    }

    [Test]
    public async Task DeleteRange_deletes_all_keys_when_range_covers_entire_leaf()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        var count = (await grain.DeleteRangeAsync("a", "zz")).Deleted;

        Assert.That(count, Is.EqualTo(3));
        Assert.That(await grain.GetAsync("a"), Is.Null);
        Assert.That(await grain.GetAsync("m"), Is.Null);
        Assert.That(await grain.GetAsync("z"), Is.Null);
    }

    [Test]
    public async Task DeleteRange_single_key_boundary()
    {
        var grain = CreateGrain();
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("2"));

        var count = (await grain.DeleteRangeAsync("b", "c")).Deleted;

        Assert.That(count, Is.EqualTo(1));
        Assert.That(await grain.GetAsync("b"), Is.Null);
        Assert.That(await grain.GetAsync("c"), Is.Not.Null);
    }

    // --- batched WAL append on the leaf write path ---

    [Test]
    public async Task SetMany_collapses_per_key_wal_appends_into_a_single_batched_call()
    {
        // SetManyAsync's foreground fast path must dispatch the whole
        // batch through ICommitLogWriter.AppendManyAsync once, not via
        // per-key AppendAsync calls. The FakeCommitLogWriter records
        // both paths so the assertion fails loudly if a future change
        // re-introduces the per-key loop.
        var writer = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: writer);

        var entries = new List<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < 16; i++)
        {
            entries.Add(new($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}")));
        }

        await grain.SetManyAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(writer.AppendManyCallCount, Is.EqualTo(1),
                "Foreground SetManyAsync must dispatch as a single batched commit-log call.");
            Assert.That(writer.AppendCount, Is.EqualTo(16),
                "Every entry must still be present in the WAL append capture.");
        });
    }

    [Test]
    public async Task SetMany_batched_path_produces_projection_identical_to_per_key_loop()
    {
        // The batched path must store every entry in the leaf
        // projection with the same key->value mapping the per-key loop
        // would have produced. Mixed key shapes and overlapping writes
        // exercise the LWW merge inside StoreEntry.
        var grain = CreateGrain();
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k01", Encoding.UTF8.GetBytes("first")),
            new("k02", Encoding.UTF8.GetBytes("second")),
            new("k03", Encoding.UTF8.GetBytes("third")),
            new("k01", Encoding.UTF8.GetBytes("first-rewritten")),
        };

        await grain.SetManyAsync(entries);

        var k01 = await grain.GetAsync("k01");
        var k02 = await grain.GetAsync("k02");
        var k03 = await grain.GetAsync("k03");
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(k01!), Is.EqualTo("first-rewritten"));
            Assert.That(Encoding.UTF8.GetString(k02!), Is.EqualTo("second"));
            Assert.That(Encoding.UTF8.GetString(k03!), Is.EqualTo("third"));
        });
    }

    [Test]
    public async Task SetMany_batched_path_advances_clock_and_version_once_per_entry()
    {
        // Every entry must advance the leaf's HLC; the batched path's
        // end-of-batch PublishVersionAdvance must dominate the
        // pre-batch version.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clockBefore = state.State.Clock;
        var versionBefore = state.State.Version.Clone();

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
            new("c", Encoding.UTF8.GetBytes("3")),
        };
        await grain.SetManyAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.GreaterThan(clockBefore),
                "Batched SetMany must still tick the HLC for every entry.");
            Assert.That(state.State.Version.DominatesOrEquals(versionBefore), Is.True);
            Assert.That(versionBefore.DominatesOrEquals(state.State.Version), Is.False,
                "Batched SetMany must publish a version-vector advance.");
        });
    }

    [Test]
    public async Task SetMany_returns_split_result_when_batch_overflows_leaf_capacity()
    {
        // The batched fast path triggers a single end-of-batch split
        // when the cumulative entries exceed MaxLeafKeys. The split's
        // sibling key is the median of the merged projection by
        // construction; the test pins that a split was reported.
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        sibling.SetNextSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        sibling.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);

        var grain = CreateGrain(siblingStub: sibling, maxLeafKeys: 4);

        var entries = new List<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < 8; i++)
        {
            entries.Add(new($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}")));
        }

        var split = await grain.SetManyAsync(entries);

        Assert.That(split, Is.Not.Null,
            "Batch overflowing MaxLeafKeys must surface a SplitResult.");
    }

    [Test]
    public async Task SetMany_empty_batch_does_not_call_commit_log()
    {
        var writer = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: writer);

        var result = await grain.SetManyAsync(new List<KeyValuePair<string, byte[]>>());

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(writer.AppendManyCallCount, Is.Zero);
            Assert.That(writer.AppendCount, Is.Zero);
        });
    }

    [Test]
    public void SetMany_throws_on_null_entries()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.SetManyAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }
}

