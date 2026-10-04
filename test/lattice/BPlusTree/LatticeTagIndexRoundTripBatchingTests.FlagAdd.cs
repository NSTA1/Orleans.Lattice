using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Round-trip accounting for the tag index's flag-membership add path.
/// </summary>
/// <remarks>
/// Under a flag membership mode a row is authored as a typed enable delta
/// minted against that row's own current state, so the rows cannot collapse
/// into one batched value write the way the LwwRegister path does. They can
/// still stop being serial, and that is what is asserted here. The call
/// <em>count</em> is unchanged by the optimisation - the same 2N rows are
/// written either way - so a count assertion cannot discriminate the wave from
/// the loop it replaced, and the discriminating instrument is the peak number
/// of writes simultaneously in flight.
/// </remarks>
public partial class LatticeTagIndexRoundTripBatchingTests
{
    /// <summary>
    /// Builds a coordinator-scoped index over one counting tree in OrFlag
    /// membership mode, where every row write is an enable delta rather than a
    /// plain value set.
    /// </summary>
    private static (CountingTree tree, LatticeTagIndexContext ctx) CreateFlagMode()
    {
        var tree = new CountingTree();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree.Lattice);
        return (tree, LatticeTagIndexContext.CreateForCoordinator(
            grainFactory, IndexName, LatticeMergeMode.OrFlag, replicaId: "r1"));
    }

    [Test]
    public async Task Flag_mode_add_issues_its_row_writes_concurrently()
    {
        var (tree, ctx) = CreateFlagMode();

        // Warm the covered-tree marker so the gate below sees only the tag
        // fan-out. A one-tag add takes the direct-await fast path, so this
        // cannot itself prove anything about overlap.
        await ctx.AddTagsForKeyAsync(TreeId, "warm", ["warm"], CancellationToken.None);
        tree.ApplyCrdtDeltaCalls = 0;

        string[] tags = ["red", "green", "blue", "amber", "violet"];

        // Five tags is ten rows, each one enable delta. The gate does not
        // release until all ten deltas are simultaneously in flight, so the
        // serial implementation cannot finish: its first enable would wait
        // forever on a tenth that is never issued. Completing at all is the
        // proof of overlap, and the peak assertion states it explicitly.
        const int expected = 10;
        var arrived = 0;
        var peak = 0;
        var inFlight = 0;
        var allInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tree.ApplyCrdtDeltaGate = () =>
        {
            var now = Interlocked.Increment(ref inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) &&
                   Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            if (Interlocked.Increment(ref arrived) == expected)
            {
                allInFlight.TrySetResult();
            }

            return WaitThenLeave();
        };

        async Task WaitThenLeave()
        {
            await allInFlight.Task.ConfigureAwait(false);
            Interlocked.Decrement(ref inFlight);
        }

        var add = ctx.AddTagsForKeyAsync(TreeId, "k1", tags, CancellationToken.None);
        var finished = await Task.WhenAny(add, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.That(finished, Is.SameAs(add), "the row writes did not overlap; they were issued sequentially");
        await add;

        Assert.Multiple(() =>
        {
            Assert.That(tree.ApplyCrdtDeltaCalls, Is.EqualTo(expected));
            // Equality, not a ceiling: a ceiling also passes under the serial
            // implementation, which peaks at one.
            Assert.That(Volatile.Read(ref peak), Is.EqualTo(expected),
                "peak simultaneous enables must reach the whole fan-out below the wave cap");
        });
    }

    [Test]
    public async Task Flag_mode_add_caps_its_write_wave_at_the_row_concurrency_limit()
    {
        var (tree, ctx) = CreateFlagMode();
        await ctx.AddTagsForKeyAsync(TreeId, "warm", ["warm"], CancellationToken.None);

        // 24 tags is 48 rows, past the cap of 32.
        var tags = new string[24];
        for (var i = 0; i < tags.Length; i++)
        {
            tags[i] = $"tag{i:D2}";
        }

        const int cap = LatticeTagIndexContext.RemoveRowConcurrencyLimit;
        var arrived = 0;
        var peak = 0;
        var inFlight = 0;
        var capReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tree.ApplyCrdtDeltaGate = () =>
        {
            var now = Interlocked.Increment(ref inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) &&
                   Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            if (Interlocked.Increment(ref arrived) == cap)
            {
                capReached.TrySetResult();
            }

            return WaitThenLeave();
        };

        async Task WaitThenLeave()
        {
            await capReached.Task.ConfigureAwait(false);
            Interlocked.Decrement(ref inFlight);
        }

        var add = ctx.AddTagsForKeyAsync(TreeId, "k1", tags, CancellationToken.None);
        var finished = await Task.WhenAny(add, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.That(finished, Is.SameAs(add), "the row writes did not overlap");
        await add;

        // An unbounded wave would reach 48 and a serial loop would reach 1, so
        // equality with the cap rejects both.
        Assert.That(Volatile.Read(ref peak), Is.EqualTo(cap),
            "the wave must fill to the cap and no further");
    }

    [Test]
    public async Task Flag_mode_add_rejects_an_invalid_tag_before_writing_any_row()
    {
        var (tree, ctx) = CreateFlagMode();
        await ctx.AddTagsForKeyAsync(TreeId, "warm", ["warm"], CancellationToken.None);
        tree.ApplyCrdtDeltaCalls = 0;

        Assert.That(
            async () => await ctx.AddTagsForKeyAsync(TreeId, "k1", ["good", "al\0so-bad"], CancellationToken.None),
            Throws.ArgumentException);

        // Validation is hoisted ahead of the wave, so a rejected tag late in
        // the list cannot leave the earlier tags durably enabled. Interleaved
        // with the writes - as this branch did before the wave - overlapping
        // them would have widened that window rather than introduced it.
        Assert.That(tree.ApplyCrdtDeltaCalls, Is.Zero);
    }

    [Test]
    public async Task Flag_mode_add_stores_the_same_rows_the_sequential_path_stored()
    {
        var (tree, ctx) = CreateFlagMode();
        await ctx.AddTagsForKeyAsync(TreeId, "k1", ["red", "blue"], CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Two tag-major rows and two key-major mirrors, exactly as the
            // sequential loop wrote. The covered-tree marker the first add on a
            // fresh context also writes is not a membership row, so it is
            // excluded rather than counted.
            Assert.That(
                tree.CrdtDeltaKeys.Where(k => !k.StartsWith("\0covered\0", StringComparison.Ordinal)),
                Has.Exactly(4).Items);
            Assert.That(tree.CrdtDeltaKeys, Has.Some.Contains("red"));
            Assert.That(tree.CrdtDeltaKeys, Has.Some.Contains("blue"));
        });
    }

    [Test]
    public async Task Flag_mode_single_tag_add_writes_exactly_two_rows()
    {
        var (tree, ctx) = CreateFlagMode();
        await ctx.AddTagsForKeyAsync(TreeId, "warm", ["warm"], CancellationToken.None);
        tree.ApplyCrdtDeltaCalls = 0;

        await ctx.AddTagsForKeyAsync(TreeId, "k1", ["solo"], CancellationToken.None);

        // The one-tag fast path awaits the pair directly rather than renting a
        // wave container; the row count must be unchanged by that shortcut.
        Assert.That(tree.ApplyCrdtDeltaCalls, Is.EqualTo(2));
    }
}
