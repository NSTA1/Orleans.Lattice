using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class MvRegisterTests
{
    private static byte[] B(string s) => System.Text.Encoding.UTF8.GetBytes(s);
    private static string S(byte[] b) => System.Text.Encoding.UTF8.GetString(b);

    private static IReadOnlyList<string> ValuesAsStrings(MvRegister r)
        => r.Values().Select(static b => System.Text.Encoding.UTF8.GetString(b)).ToArray();

    [Test]
    public void New_register_is_empty()
    {
        var r = new MvRegister();
        Assert.That(r.IsEmpty, Is.True);
        Assert.That(r.Count, Is.EqualTo(0));
        Assert.That(r.Values(), Is.Empty);
    }

    [Test]
    public void Set_stores_single_value_on_one_replica()
    {
        var r = new MvRegister();
        r.Set("r1", B("alpha"));
        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(ValuesAsStrings(r), Is.EquivalentTo(new[] { "alpha" }));
        Assert.That(r.Context["r1"], Is.EqualTo(1));
    }

    [Test]
    public void Sequential_set_on_same_replica_drops_prior_value()
    {
        var r = new MvRegister();
        r.Set("r1", B("a"));
        r.Set("r1", B("b"));
        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(ValuesAsStrings(r), Is.EquivalentTo(new[] { "b" }));
        Assert.That(r.Context["r1"], Is.EqualTo(2));
    }

    [Test]
    public void Set_throws_on_null_value()
    {
        var r = new MvRegister();
        Assert.That(() => r.Set("r1", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Set_throws_on_empty_replica_id()
    {
        var r = new MvRegister();
        Assert.That(() => r.Set("", B("x")), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => r.Set(null!, B("x")), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Concurrent_writes_from_different_replicas_survive_merge()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));

        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var merged = MvRegister.Merge(a, b);
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "alpha", "beta" }));
        Assert.That(merged.Context["r1"], Is.EqualTo(1));
        Assert.That(merged.Context["r2"], Is.EqualTo(1));
    }

    [Test]
    public void Write_after_merge_observes_and_supersedes_prior_dots()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var merged = MvRegister.Merge(a, b);
        // r1 observes the merged context and writes a new value.
        merged.Set("r1", B("gamma"));
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "gamma" }));
        Assert.That(merged.Context["r1"], Is.EqualTo(2));
        Assert.That(merged.Context["r2"], Is.EqualTo(1));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var once = MvRegister.Merge(a, b);
        var twice = MvRegister.Merge(once, b);
        Assert.That(ValuesAsStrings(twice), Is.EquivalentTo(ValuesAsStrings(once)));
        Assert.That(twice.Context, Is.EquivalentTo(once.Context));
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var ab = MvRegister.Merge(a, b);
        var ba = MvRegister.Merge(b, a);
        Assert.That(ValuesAsStrings(ab), Is.EquivalentTo(ValuesAsStrings(ba)));
        Assert.That(ab.Context, Is.EquivalentTo(ba.Context));
    }

    [Test]
    public void Merge_with_self_is_identity()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var copy = MvRegister.Merge(a, a);
        Assert.That(ValuesAsStrings(copy), Is.EquivalentTo(new[] { "alpha" }));
        Assert.That(copy.Context["r1"], Is.EqualTo(1));
    }

    [Test]
    public void Sequential_write_then_merge_with_stale_drops_stale_value()
    {
        var a = new MvRegister();
        a.Set("r1", B("a1"));
        // Snapshot a's stale state for later merge.
        var stale = a.Clone();
        a.Set("r1", B("a2"));

        var merged = MvRegister.Merge(a, stale);
        // The stale dot is dominated by a's post-write context (counter 2)
        // and must not be re-introduced.
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "a2" }));
    }

    [Test]
    public void Merge_throws_on_null_left()
    {
        Assert.That(() => MvRegister.Merge(null!, new MvRegister()), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_throws_on_null_right()
    {
        Assert.That(() => MvRegister.Merge(new MvRegister(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        Assert.That(() => new MvRegister().MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_produces_independent_copy()
    {
        var a = new MvRegister();
        a.Set("r1", B("a1"));
        var copy = a.Clone();
        copy.Set("r1", B("a2"));
        Assert.That(ValuesAsStrings(a), Is.EquivalentTo(new[] { "a1" }));
        Assert.That(ValuesAsStrings(copy), Is.EquivalentTo(new[] { "a2" }));
    }

    [Test]
    public void Values_returns_deterministic_order()
    {
        var a = new MvRegister();
        a.Set("rB", B("beta"));
        var b = new MvRegister();
        b.Set("rA", B("alpha"));
        var merged = MvRegister.Merge(a, b);
        // Ordered ascending by (ReplicaId, Counter): rA before rB.
        Assert.That(ValuesAsStrings(merged), Is.EqualTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void Three_way_concurrent_merge_keeps_all_three_values()
    {
        var a = new MvRegister();
        a.Set("r1", B("a"));
        var b = new MvRegister();
        b.Set("r2", B("b"));
        var c = new MvRegister();
        c.Set("r3", B("c"));

        var merged = MvRegister.Merge(MvRegister.Merge(a, b), c);
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "a", "b", "c" }));
    }
}
