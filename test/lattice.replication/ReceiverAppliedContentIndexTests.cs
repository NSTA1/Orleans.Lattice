using NUnit.Framework;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="ReceiverAppliedContentIndex"/>, the
/// receiver-side best-effort content-hash cache that drives manifest-based
/// payload elision.
/// </summary>
[TestFixture]
public sealed class ReceiverAppliedContentIndexTests
{
    [Test]
    public void TryGetContentHash_returns_false_for_cold_key()
    {
        var index = new ReceiverAppliedContentIndex();

        var held = index.TryGetContentHash("tree", "missing", out var hash);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.False);
            Assert.That(hash, Is.EqualTo(0UL));
        });
    }

    [Test]
    public void RecordSet_then_TryGetContentHash_returns_recorded_hash()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree", "a", 42UL, 64);

        var held = index.TryGetContentHash("tree", "a", out var hash);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True);
            Assert.That(hash, Is.EqualTo(42UL));
        });
    }

    [Test]
    public void RecordSet_overwrites_existing_hash_for_same_key()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree", "a", 1UL, 64);
        index.RecordSet("tree", "a", 2UL, 64);

        index.TryGetContentHash("tree", "a", out var hash);

        Assert.Multiple(() =>
        {
            Assert.That(hash, Is.EqualTo(2UL));
            Assert.That(index.CountForTree("tree"), Is.EqualTo(1));
        });
    }

    [Test]
    public void RecordDelete_removes_recorded_key()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree", "a", 42UL, 64);

        index.RecordDelete("tree", "a");

        Assert.That(index.TryGetContentHash("tree", "a", out _), Is.False);
    }

    [Test]
    public void RecordDelete_is_noop_for_unknown_key()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree", "a", 42UL, 64);

        index.RecordDelete("tree", "absent");

        Assert.That(index.TryGetContentHash("tree", "a", out _), Is.True);
    }

    [Test]
    public void InvalidateTree_clears_all_keys_for_that_tree_only()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree-a", "k1", 1UL, 64);
        index.RecordSet("tree-a", "k2", 2UL, 64);
        index.RecordSet("tree-b", "k1", 3UL, 64);

        index.InvalidateTree("tree-a");

        Assert.Multiple(() =>
        {
            Assert.That(index.TryGetContentHash("tree-a", "k1", out _), Is.False);
            Assert.That(index.TryGetContentHash("tree-a", "k2", out _), Is.False);
            Assert.That(index.TryGetContentHash("tree-b", "k1", out var bHash), Is.True);
            Assert.That(bHash, Is.EqualTo(3UL));
        });
    }

    [Test]
    public void Partitions_are_isolated_per_tree()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree-a", "shared", 1UL, 64);
        index.RecordSet("tree-b", "shared", 2UL, 64);

        index.TryGetContentHash("tree-a", "shared", out var aHash);
        index.TryGetContentHash("tree-b", "shared", out var bHash);

        Assert.Multiple(() =>
        {
            Assert.That(aHash, Is.EqualTo(1UL));
            Assert.That(bHash, Is.EqualTo(2UL));
        });
    }

    [Test]
    public void RecordSet_evicts_least_recently_used_key_at_capacity()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree", "k1", 1UL, 2);
        index.RecordSet("tree", "k2", 2UL, 2);
        index.RecordSet("tree", "k3", 3UL, 2);

        Assert.Multiple(() =>
        {
            Assert.That(index.TryGetContentHash("tree", "k1", out _), Is.False);
            Assert.That(index.TryGetContentHash("tree", "k2", out _), Is.True);
            Assert.That(index.TryGetContentHash("tree", "k3", out _), Is.True);
            Assert.That(index.CountForTree("tree"), Is.EqualTo(2));
        });
    }

    [Test]
    public void TryGetContentHash_promotes_key_so_it_survives_eviction()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree", "k1", 1UL, 2);
        index.RecordSet("tree", "k2", 2UL, 2);

        // Touch k1 so k2 becomes the least-recently-used entry.
        index.TryGetContentHash("tree", "k1", out _);
        index.RecordSet("tree", "k3", 3UL, 2);

        Assert.Multiple(() =>
        {
            Assert.That(index.TryGetContentHash("tree", "k1", out _), Is.True);
            Assert.That(index.TryGetContentHash("tree", "k2", out _), Is.False);
            Assert.That(index.TryGetContentHash("tree", "k3", out _), Is.True);
        });
    }

    [Test]
    public void RecordSet_floors_capacity_at_one()
    {
        var index = new ReceiverAppliedContentIndex();
        index.RecordSet("tree", "k1", 1UL, 0);
        index.RecordSet("tree", "k2", 2UL, 0);

        Assert.Multiple(() =>
        {
            Assert.That(index.CountForTree("tree"), Is.EqualTo(1));
            Assert.That(index.TryGetContentHash("tree", "k2", out var hash), Is.True);
            Assert.That(hash, Is.EqualTo(2UL));
        });
    }

    [Test]
    public void CountForTree_returns_zero_for_unknown_tree()
    {
        var index = new ReceiverAppliedContentIndex();

        Assert.That(index.CountForTree("absent"), Is.EqualTo(0));
    }

    [Test]
    public void RecordSet_throws_on_null_arguments()
    {
        var index = new ReceiverAppliedContentIndex();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => index.RecordSet(null!, "k", 1UL, 64));
            Assert.Throws<ArgumentNullException>(() => index.RecordSet("tree", null!, 1UL, 64));
        });
    }

    [Test]
    public void RecordDelete_throws_on_null_arguments()
    {
        var index = new ReceiverAppliedContentIndex();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => index.RecordDelete(null!, "k"));
            Assert.Throws<ArgumentNullException>(() => index.RecordDelete("tree", null!));
        });
    }

    [Test]
    public void TryGetContentHash_and_InvalidateTree_throw_on_null_arguments()
    {
        var index = new ReceiverAppliedContentIndex();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => index.TryGetContentHash(null!, "k", out _));
            Assert.Throws<ArgumentNullException>(() => index.TryGetContentHash("tree", null!, out _));
            Assert.Throws<ArgumentNullException>(() => index.InvalidateTree(null!));
        });
    }
}
