using Orleans.Lattice.BPlusTree.State;
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
}
