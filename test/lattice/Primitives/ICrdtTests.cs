using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class ICrdtTests
{
    [Test]
    public void OrSet_IsBottom_is_true_for_empty_and_false_after_add()
    {
        var s = new OrSet();
        Assert.That(s.IsBottom, Is.True);
        s.Add(new byte[] { 1 }, "r1", 1);
        Assert.That(s.IsBottom, Is.False);
        s.Remove(new byte[] { 1 });
        Assert.That(s.IsBottom, Is.True);
    }

    [Test]
    public void PnCounter_IsBottom_is_true_only_when_no_replicas_recorded()
    {
        var c = new PnCounter();
        Assert.That(c.IsBottom, Is.True);
        c.Increment("r1", 1);
        Assert.That(c.IsBottom, Is.False);
        c.Decrement("r1", 1);
        // Value sums to zero but replica history is recorded - not bottom.
        Assert.That(c.Value, Is.EqualTo(0));
        Assert.That(c.IsBottom, Is.False);
    }

    [Test]
    public void VersionVector_IsBottom_is_true_when_no_entries()
    {
        var v = new VersionVector();
        Assert.That(v.IsBottom, Is.True);
        v.Tick("r1");
        Assert.That(v.IsBottom, Is.False);
    }

    [Test]
    public void MvRegister_IsBottom_is_true_when_no_live_entries()
    {
        var r = new MvRegister();
        Assert.That(r.IsBottom, Is.True);
        r.Set("r1", new byte[] { 0x42 });
        Assert.That(r.IsBottom, Is.False);
    }

    [Test]
    public void MergeFrom_throws_on_null_via_interface()
    {
        ICrdt<OrSet> s = new OrSet();
        Assert.That(() => s.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void MergeFrom_via_interface_dispatches_to_concrete_merge()
    {
        ICrdt<PnCounter> a = new PnCounter();
        var b = new PnCounter();
        b.Increment("r1", 5);
        a.MergeFrom(b);
        Assert.That(((PnCounter)a).Value, Is.EqualTo(5));
    }

    [Test]
    public void OrMap_IsBottom_is_true_for_empty_and_after_every_key_tombstoned()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(m.IsBottom, Is.True);
        m.Set("k", "r1", new OrSet());
        Assert.That(m.IsBottom, Is.False);
        m.Remove("k");
        Assert.That(m.IsBottom, Is.True);
    }

    [Test]
    public void OrMap_implements_ICrdt_and_dispatches_MergeFrom()
    {
        ICrdt<OrMap<string, PnCounter>> a = new OrMap<string, PnCounter>();
        var b = new OrMap<string, PnCounter>();
        var counter = new PnCounter();
        counter.Increment("r1", 7);
        b.Set("k", "r1", counter);

        a.MergeFrom(b);

        var merged = ((OrMap<string, PnCounter>)a).Get("k");
        Assert.That(merged, Is.Not.Null);
        Assert.That(merged!.Value, Is.EqualTo(7));
    }
}
