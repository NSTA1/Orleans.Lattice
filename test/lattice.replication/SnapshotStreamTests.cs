using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class SnapshotStreamTests
{
    private static IAsyncEnumerable<SnapshotEntry> EmptyStream() => AsyncEnumerable.Empty<SnapshotEntry>();

    [Test]
    public void Constructor_throws_when_tree_name_is_null()
    {
        Assert.That(
            () => new SnapshotStream(null!, HybridLogicalClock.Zero, new VersionVector(), EmptyStream()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Constructor_throws_when_tree_name_is_whitespace()
    {
        Assert.That(
            () => new SnapshotStream("   ", HybridLogicalClock.Zero, new VersionVector(), EmptyStream()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Constructor_throws_when_frontier_is_null()
    {
        Assert.That(
            () => new SnapshotStream("t", HybridLogicalClock.Zero, null!, EmptyStream()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_entries_is_null()
    {
        Assert.That(
            () => new SnapshotStream("t", HybridLogicalClock.Zero, new VersionVector(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_assigns_properties()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var vc = new VersionVector();
        vc.Tick("site-a");
        var stream = EmptyStream();

        var snapshot = new SnapshotStream("tree", ts, vc, stream);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TreeName, Is.EqualTo("tree"));
            Assert.That(snapshot.AsOfHlc, Is.EqualTo(ts));
            Assert.That(snapshot.CausalStableFrontier, Is.SameAs(vc));
            Assert.That(snapshot.Entries, Is.SameAs(stream));
        });
    }

    private static class AsyncEnumerable
    {
        public static async IAsyncEnumerable<T> Empty<T>()
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
