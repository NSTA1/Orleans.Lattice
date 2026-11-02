using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5460: the two leaf seams a shard's bulk append reads and writes - the
/// append tail (live count and right edge in one turn) and the narrowing of the
/// rightmost leaf's high bound before appended leaves are linked behind it.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task GetAppendTail_reports_no_keys_for_an_empty_leaf()
    {
        var grain = CreateGrain();

        var (count, highest) = await grain.GetAppendTailAsync();

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.Zero);
            Assert.That(highest, Is.Null);
        });
    }

    [Test]
    public async Task GetAppendTail_reports_the_live_count_and_the_highest_live_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k3", Encoding.UTF8.GetBytes("v3"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        await grain.SetAsync("k4", Encoding.UTF8.GetBytes("v4"));
        await grain.DeleteAsync("k4");

        var (count, highest) = await grain.GetAppendTailAsync();
        var counted = await grain.CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(counted), "the count is the one CountAsync reports");
            Assert.That(count, Is.EqualTo(3));
            Assert.That(highest, Is.EqualTo("k3"), "a tombstoned key is not the right edge");
        });
    }

    [Test]
    public async Task NarrowHighKey_narrows_an_open_high_bound_and_persists_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.NarrowHighKeyAsync("m");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));
            Assert.That(state.State.LowKeyInclusive, Is.Null, "the low bound is left as it was");
        });
    }

    [Test]
    public async Task NarrowHighKey_narrows_a_wider_bound_but_never_widens_one()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "c";
        state.State.HighKeyExclusive = "t";
        var grain = CreateGrain(state);

        await grain.NarrowHighKeyAsync("m");
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));

        // A re-driven graft names the bound it already applied, or a wider one.
        await grain.NarrowHighKeyAsync("m");
        await grain.NarrowHighKeyAsync("x");
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));
    }

    [Test]
    public void NarrowHighKey_refuses_a_bound_at_or_below_the_low_bound()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "m";
        var grain = CreateGrain(state);

        Assert.That(async () => await grain.NarrowHighKeyAsync("m"), Throws.InvalidOperationException);
        Assert.That(async () => await grain.NarrowHighKeyAsync("a"), Throws.InvalidOperationException);
        Assert.That(state.State.HighKeyExclusive, Is.Null);
    }

    [Test]
    public void NarrowHighKey_rejects_a_null_bound()
    {
        var grain = CreateGrain();

        Assert.That(async () => await grain.NarrowHighKeyAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task NarrowHighKey_hands_back_a_copy_of_every_row_at_or_above_the_bound_and_keeps_them_until_dropped()
    {
        // Issue #5610: the graft relocates these rows into the appended leaves and
        // only then drops them, so a relocation that fails part-way is handed the
        // same rows again when the graft is resumed.
        var grain = CreateGrain();
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("vb"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("vm"));
        await grain.SetAsync("q", Encoding.UTF8.GetBytes("vq"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("vz"));
        await grain.DeleteAsync("q");

        var stranded = await grain.NarrowHighKeyAsync("m");

        Assert.Multiple(() =>
        {
            Assert.That(stranded.Select(r => r.Key), Is.EqualTo(new[] { "m", "q", "z" }));
            Assert.That(stranded.Single(r => r.Key == "q").IsTombstone, Is.True, "a tombstone is handed back as one");
            Assert.That(Encoding.UTF8.GetString(stranded.Single(r => r.Key == "z").Value!), Is.EqualTo("vz"));
        });

        var again = await grain.NarrowHighKeyAsync("m");
        Assert.That(again.Select(r => r.Key), Is.EqualTo(new[] { "m", "q", "z" }),
            "a resumed graft is handed the same rows: the narrow removed nothing");
    }

    [Test]
    public async Task DropRelocatedRows_drops_exactly_the_rows_handed_back_and_keeps_rows_in_span()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("vb"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("vm"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("vz"));

        var stranded = await grain.NarrowHighKeyAsync("m");
        var handedBack = stranded.ToDictionary(r => r.Key, r => r.Timestamp);

        // A stale stamp for z: a row rewritten since it was handed back is kept.
        handedBack["z"] = HybridLogicalClock.Zero;

        // A key the leaf still declares is never dropped, whatever stamp is named.
        var bRow = (await grain.GetRawEntryAsync("b"))!.Value;
        handedBack["b"] = bRow.Timestamp;

        await grain.DropRelocatedRowsAsync(handedBack);

        var remaining = await grain.NarrowHighKeyAsync("m");
        var bAfter = await grain.GetRawEntryAsync("b");
        Assert.Multiple(() =>
        {
            Assert.That(remaining.Select(r => r.Key), Is.EqualTo(new[] { "z" }), "only m, handed back at its stamp, is dropped");
            Assert.That(bAfter, Is.Not.Null, "a row in span is kept");
        });

        await grain.DropRelocatedRowsAsync(handedBack);
        Assert.That((await grain.NarrowHighKeyAsync("m")).Select(r => r.Key), Is.EqualTo(new[] { "z" }), "idempotent");
    }

    [Test]
    public async Task NarrowHighKey_links_the_successor_in_the_same_persist_so_a_later_write_above_the_bound_forwards()
    {
        // Issue #5654: narrowed but unlinked, the leaf has no forward target, so a
        // write above the bound fails open onto it, outside its span.
        var state = new FakePersistentState<LeafNodeState>();
        var successor = Substitute.For<IBPlusLeafGrain>();
        var grain = CreateGrain(state, siblingStub: successor);
        var successorId = GrainId.Create("leaf", "successor");

        var writesBefore = state.WriteCount;
        await grain.NarrowHighKeyAsync("m", successorId);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));
            Assert.That(state.State.NextSibling, Is.EqualTo(successorId));
            Assert.That(state.WriteCount - writesBefore, Is.EqualTo(1), "bound and link land in one persist");
        });

        await grain.SetAsync("z", Encoding.UTF8.GetBytes("vz"));

        await successor.Received(1).SetAsync("z", Arg.Any<byte[]>());
        Assert.That(await grain.NarrowHighKeyAsync("m"), Is.Empty, "the write was forwarded, not stored here");
    }
}