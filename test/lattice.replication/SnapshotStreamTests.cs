using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="SnapshotStream"/>'s saga-blacklist field
/// and constructor variants.
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
    public void Constructor_defaults_saga_blacklist_to_empty_when_null()
    {
        var stream = new SnapshotStream(
            "tree",
            HybridLogicalClock.Zero,
            new VersionVector(),
            EmptyEntries(),
            sagaBlacklist: null);

        Assert.That(stream.SagaBlacklist, Is.Empty);
    }

    [Test]
    public void Constructor_defaults_saga_blacklist_to_empty_when_omitted()
    {
        var stream = new SnapshotStream(
            "tree",
            HybridLogicalClock.Zero,
            new VersionVector(),
            EmptyEntries());

        Assert.That(stream.SagaBlacklist, Is.Empty);
    }

    [Test]
    public void Constructor_preserves_supplied_saga_blacklist()
    {
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        var stream = new SnapshotStream(
            "tree",
            HybridLogicalClock.Zero,
            new VersionVector(),
            EmptyEntries(),
            sagaBlacklist: ids);

        Assert.That(stream.SagaBlacklist, Is.EqualTo(ids));
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
