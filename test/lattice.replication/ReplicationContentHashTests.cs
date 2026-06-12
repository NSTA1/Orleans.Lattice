using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for the stable content-hash digest used by the
/// sender-side content-hash dedup measurement.
/// </summary>
[TestFixture]
public class ReplicationContentHashTests
{
    private static WalRecord Set(string key, byte[] value) => new()
    {
        TreeId = "tree",
        Op = MutationKind.Set,
        Key = key,
        Value = value,
        OriginClusterId = "site-a",
    };

    [Test]
    public void Compute_is_stable_for_identical_content()
    {
        var a = ReplicationContentHash.Compute(Set("k", new byte[] { 1, 2, 3 }));
        var b = ReplicationContentHash.Compute(Set("k", new byte[] { 1, 2, 3 }));

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Compute_differs_when_value_differs()
    {
        var a = ReplicationContentHash.Compute(Set("k", new byte[] { 1, 2, 3 }));
        var b = ReplicationContentHash.Compute(Set("k", new byte[] { 1, 2, 4 }));

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Compute_differs_when_key_differs()
    {
        var a = ReplicationContentHash.Compute(Set("a", new byte[] { 1 }));
        var b = ReplicationContentHash.Compute(Set("b", new byte[] { 1 }));

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Compute_differs_when_op_differs()
    {
        var set = ReplicationContentHash.Compute(MutationKind.Set, "k", null, ReadOnlySpan<byte>.Empty);
        var del = ReplicationContentHash.Compute(MutationKind.Delete, "k", null, ReadOnlySpan<byte>.Empty);

        Assert.That(set, Is.Not.EqualTo(del));
    }

    [Test]
    public void Compute_differs_when_end_exclusive_key_differs()
    {
        var a = ReplicationContentHash.Compute(MutationKind.DeleteRange, "a", "c", ReadOnlySpan<byte>.Empty);
        var b = ReplicationContentHash.Compute(MutationKind.DeleteRange, "a", "d", ReadOnlySpan<byte>.Empty);

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Compute_does_not_alias_across_key_value_boundary()
    {
        // {"ab", value=""} and {"a", value="b"} must hash differently
        // because the field separator prevents the partitions from
        // aliasing.
        var a = ReplicationContentHash.Compute(MutationKind.Set, "ab", null, ReadOnlySpan<byte>.Empty);
        var b = ReplicationContentHash.Compute(MutationKind.Set, "a", null, new byte[] { (byte)'b' });

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Compute_handles_null_key_and_empty_value()
    {
        Assert.That(
            () => ReplicationContentHash.Compute(MutationKind.Delete, null, null, ReadOnlySpan<byte>.Empty),
            Throws.Nothing);
    }
}
