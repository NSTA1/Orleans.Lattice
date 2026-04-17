using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- Sibling pointers ---

    [Test]
    public async Task NextSibling_is_null_initially()
    {
        var grain = CreateGrain();
        Assert.That(await grain.GetNextSiblingAsync(), Is.Null);
    }

    [Test]
    public async Task SetNextSibling_persists_sibling_id()
    {
        var grain = CreateGrain();
        var siblingId = GrainId.Create("leaf", "sibling-1");
        await grain.SetNextSiblingAsync(siblingId);

        Assert.That(await grain.GetNextSiblingAsync(), Is.EqualTo(siblingId));
    }

    // --- PrevSibling pointers ---

    [Test]
    public async Task PrevSibling_is_null_initially()
    {
        var grain = CreateGrain();
        Assert.That(await grain.GetPrevSiblingAsync(), Is.Null);
    }

    [Test]
    public async Task SetPrevSibling_persists_sibling_id()
    {
        var grain = CreateGrain();
        var siblingId = GrainId.Create("leaf", "sibling-left");
        await grain.SetPrevSiblingAsync(siblingId);

        Assert.That(await grain.GetPrevSiblingAsync(), Is.EqualTo(siblingId));
    }

    // --- Multiple keys ---

    [Test]
    public async Task Multiple_keys_are_independent()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("a"))!), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("2"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("c"))!), Is.EqualTo("3"));
    }

    [Test]
    public async Task Deleting_one_key_does_not_affect_others()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        await grain.DeleteAsync("a");

        Assert.That(await grain.GetAsync("a"), Is.Null);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("2"));
    }
}
