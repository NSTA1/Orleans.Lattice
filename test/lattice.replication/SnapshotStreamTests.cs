using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="SnapshotStream"/>'s constructor variants.
/// </summary>
[TestFixture]
public class SnapshotStreamTests
{
    private static async IAsyncEnumerable<SnapshotEntry> EmptyEntries()
    {
        await Task.CompletedTask;
        yield break;
    }

    [Test]
    public void Constructor_throws_on_null_tree_name()
    {
        Assert.That(
            () => new SnapshotStream(
                null!,
                HybridLogicalClock.Zero,
                new VersionVector(),
                EmptyEntries()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Constructor_throws_on_null_frontier()
    {
        Assert.That(
            () => new SnapshotStream(
                "tree",
                HybridLogicalClock.Zero,
                null!,
                EmptyEntries()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_on_null_entries()
    {
        Assert.That(
            () => new SnapshotStream(
                "tree",
                HybridLogicalClock.Zero,
                new VersionVector(),
                null!),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
