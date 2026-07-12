namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the new <see cref="LatticeMergeContext"/> versioning fields
/// (<see cref="LatticeMergeContext.TreeId"/>,
/// <see cref="LatticeMergeContext.LocalVersion"/>,
/// <see cref="LatticeMergeContext.IncomingVersion"/>): they default to the inert
/// values so the five-argument constructor an existing caller uses is byte-for-byte
/// unchanged, and they round-trip the values the leaf stamps.
/// </summary>
[TestFixture]
public class LatticeMergeContextTests
{
    private static byte[] Bytes(params byte[] b) => b;

    [Test]
    public void Ctor_five_arg_defaults_versioning_fields_to_inert()
    {
        var ctx = new LatticeMergeContext("k", LatticeMergeMode.LwwRegister, null, null, Bytes(1));

        Assert.That(ctx.TreeId, Is.Null);
        Assert.That(ctx.LocalVersion, Is.EqualTo(0u));
        Assert.That(ctx.IncomingVersion, Is.EqualTo(0u));
    }

    [Test]
    public void Ctor_stamps_tree_id_and_versions()
    {
        var ctx = new LatticeMergeContext(
            "k", LatticeMergeMode.OrSet, Bytes(1), null, Bytes(2),
            treeId: "orders", localVersion: 3, incomingVersion: 5);

        Assert.That(ctx.TreeId, Is.EqualTo("orders"));
        Assert.That(ctx.LocalVersion, Is.EqualTo(3u));
        Assert.That(ctx.IncomingVersion, Is.EqualTo(5u));
    }

    [Test]
    public void Ctor_preserves_core_fields()
    {
        var local = Bytes(1);
        var incoming = Bytes(2);
        var merged = Bytes(3);
        var ctx = new LatticeMergeContext("k", LatticeMergeMode.LwwRegister, local, incoming, merged, "t");

        Assert.That(ctx.Key, Is.EqualTo("k"));
        Assert.That(ctx.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        Assert.That(ctx.LocalValue, Is.SameAs(local));
        Assert.That(ctx.IncomingValue, Is.SameAs(incoming));
        Assert.That(ctx.MergedValue, Is.SameAs(merged));
    }

    [Test]
    public void Ctor_null_key_throws()
    {
        Assert.That(
            () => new LatticeMergeContext(null!, LatticeMergeMode.LwwRegister, null, null, Bytes(1)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_merged_value_throws()
    {
        Assert.That(
            () => new LatticeMergeContext("k", LatticeMergeMode.LwwRegister, null, null, null!),
            Throws.ArgumentNullException);
    }
}
