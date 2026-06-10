using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Unit tests for <see cref="WalPlacementPin"/> - the durable per-tree record of
/// which storage-provider catalog key backs each WAL partition. Covers the
/// default pin, per-partition resolution, override application, and clean
/// reversal back to the default key.
/// </summary>
[TestFixture]
public sealed class WalPlacementPinTests
{
    [Test]
    public void Create_yields_version_zero_default_pin_with_no_overrides()
    {
        var pin = WalPlacementPin.Create();

        Assert.That(pin.Version, Is.EqualTo(0));
        Assert.That(pin.DefaultProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(pin.Overrides, Is.Null);
    }

    [Test]
    public void ResolveKey_returns_default_for_partition_without_override()
    {
        var pin = WalPlacementPin.Create();

        Assert.That(pin.ResolveKey(0), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(pin.ResolveKey(7), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }

    [Test]
    public void WithPartition_routes_one_partition_to_named_key_and_bumps_version()
    {
        var moved = WalPlacementPin.Create().WithPartition(2, "secondary", 1);

        Assert.That(moved.Version, Is.EqualTo(1));
        Assert.That(moved.ResolveKey(2), Is.EqualTo("secondary"));
        // Other partitions still resolve to the default key.
        Assert.That(moved.ResolveKey(0), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(moved.Overrides, Is.Not.Null);
        Assert.That(moved.Overrides!, Has.Count.EqualTo(1));
    }

    [Test]
    public void WithPartition_back_to_default_removes_override_for_clean_reversal()
    {
        var moved = WalPlacementPin.Create().WithPartition(2, "secondary", 1);

        var reverted = moved.WithPartition(2, IWalStorageProviderCatalog.DefaultProviderKey, 2);

        Assert.That(reverted.Version, Is.EqualTo(2));
        Assert.That(reverted.ResolveKey(2), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        // The override map collapses back to null once empty, restoring the
        // exact shape of the default pin (aside from the version).
        Assert.That(reverted.Overrides, Is.Null);
    }

    [Test]
    public void WithPartition_preserves_other_overrides_when_reverting_one()
    {
        var pin = WalPlacementPin.Create()
            .WithPartition(0, "acct-a", 1)
            .WithPartition(1, "acct-b", 2);

        var reverted = pin.WithPartition(0, IWalStorageProviderCatalog.DefaultProviderKey, 3);

        Assert.That(reverted.ResolveKey(0), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(reverted.ResolveKey(1), Is.EqualTo("acct-b"));
        Assert.That(reverted.Overrides!, Has.Count.EqualTo(1));
    }

    [Test]
    public void WithPartition_does_not_mutate_the_source_pin()
    {
        var original = WalPlacementPin.Create();

        _ = original.WithPartition(0, "secondary", 1);

        Assert.That(original.Version, Is.EqualTo(0));
        Assert.That(original.Overrides, Is.Null);
    }

    [Test]
    public void WithPartitions_applies_every_move_under_a_single_version_bump()
    {
        var moved = WalPlacementPin.Create().WithPartitions(
            new[] { (0, "acct-a"), (1, "acct-b"), (2, "acct-a") }, 1);

        Assert.That(moved.Version, Is.EqualTo(1));
        Assert.That(moved.ResolveKey(0), Is.EqualTo("acct-a"));
        Assert.That(moved.ResolveKey(1), Is.EqualTo("acct-b"));
        Assert.That(moved.ResolveKey(2), Is.EqualTo("acct-a"));
        Assert.That(moved.ResolveKey(3), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(moved.Overrides!, Has.Count.EqualTo(3));
    }

    [Test]
    public void WithPartitions_routing_back_to_default_removes_those_overrides()
    {
        var pin = WalPlacementPin.Create().WithPartitions(
            new[] { (0, "acct-a"), (1, "acct-b") }, 1);

        var reverted = pin.WithPartitions(
            new[]
            {
                (0, IWalStorageProviderCatalog.DefaultProviderKey),
                (1, IWalStorageProviderCatalog.DefaultProviderKey),
            },
            2);

        Assert.That(reverted.Version, Is.EqualTo(2));
        Assert.That(reverted.ResolveKey(0), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(reverted.ResolveKey(1), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(reverted.Overrides, Is.Null);
    }

    [Test]
    public void WithPartitions_last_write_wins_for_a_repeated_partition()
    {
        var moved = WalPlacementPin.Create().WithPartitions(
            new[] { (0, "acct-a"), (0, "acct-b") }, 1);

        Assert.That(moved.ResolveKey(0), Is.EqualTo("acct-b"));
        Assert.That(moved.Overrides!, Has.Count.EqualTo(1));
    }

    [Test]
    public void WithPartitions_does_not_mutate_the_source_pin()
    {
        var original = WalPlacementPin.Create().WithPartition(0, "acct-a", 1);

        _ = original.WithPartitions(new[] { (1, "acct-b"), (2, "acct-c") }, 2);

        Assert.That(original.Version, Is.EqualTo(1));
        Assert.That(original.Overrides!, Has.Count.EqualTo(1));
        Assert.That(original.ResolveKey(1), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }
}
