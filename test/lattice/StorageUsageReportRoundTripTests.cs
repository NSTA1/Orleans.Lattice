using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Pins the Orleans-serializer wire-shape contract for the byte-accurate
/// storage-usage DTOs <see cref="TreeStorageUsageReport"/> and
/// <see cref="ClusterStorageUsageReport"/>: every slot must round-trip
/// verbatim and a default-constructed value must decode cleanly.
/// </summary>
[TestFixture]
public sealed class StorageUsageReportRoundTripTests
{
    private ServiceProvider _services = null!;
    private Serializer<TreeStorageUsageReport> _treeSerializer = null!;
    private Serializer<ClusterStorageUsageReport> _clusterSerializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _treeSerializer = _services.GetRequiredService<Serializer<TreeStorageUsageReport>>();
        _clusterSerializer = _services.GetRequiredService<Serializer<ClusterStorageUsageReport>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void TreeStorageUsageReport_round_trips_every_slot()
    {
        var sampledAt = DateTimeOffset.UtcNow;
        var report = new TreeStorageUsageReport
        {
            TreeId = "tree-a",
            WalRetainedBytes = 1024,
            SnapshotBytes = 2048,
            LeafStateBytes = 512,
            TotalBytes = 3584,
            Partial = true,
            SampledAt = sampledAt,
        };

        var decoded = _treeSerializer.Deserialize(_treeSerializer.SerializeToArray(report));

        Assert.That(decoded.TreeId, Is.EqualTo("tree-a"));
        Assert.That(decoded.WalRetainedBytes, Is.EqualTo(1024));
        Assert.That(decoded.SnapshotBytes, Is.EqualTo(2048));
        Assert.That(decoded.LeafStateBytes, Is.EqualTo(512));
        Assert.That(decoded.TotalBytes, Is.EqualTo(3584));
        Assert.That(decoded.Partial, Is.True);
        Assert.That(decoded.SampledAt, Is.EqualTo(sampledAt));
    }

    [Test]
    public void TreeStorageUsageReport_default_decodes_to_zeroed_values()
    {
        var decoded = _treeSerializer.Deserialize(_treeSerializer.SerializeToArray(default));

        Assert.That(decoded.TreeId, Is.Null);
        Assert.That(decoded.WalRetainedBytes, Is.EqualTo(0));
        Assert.That(decoded.TotalBytes, Is.EqualTo(0));
        Assert.That(decoded.Partial, Is.False);
    }

    [Test]
    public void ClusterStorageUsageReport_round_trips_every_slot()
    {
        var sampledAt = DateTimeOffset.UtcNow;
        var tree = new TreeStorageUsageReport
        {
            TreeId = "tree-a",
            WalRetainedBytes = 10,
            SnapshotBytes = 20,
            LeafStateBytes = 30,
            TotalBytes = 60,
            Partial = false,
            SampledAt = sampledAt,
        };
        var report = new ClusterStorageUsageReport
        {
            TreeCount = 1,
            WalRetainedBytes = 10,
            SnapshotBytes = 20,
            LeafStateBytes = 30,
            TotalBytes = 60,
            Partial = false,
            Trees = ImmutableArray.Create(tree),
            SampledAt = sampledAt,
        };

        var decoded = _clusterSerializer.Deserialize(_clusterSerializer.SerializeToArray(report));

        Assert.That(decoded.TreeCount, Is.EqualTo(1));
        Assert.That(decoded.WalRetainedBytes, Is.EqualTo(10));
        Assert.That(decoded.SnapshotBytes, Is.EqualTo(20));
        Assert.That(decoded.LeafStateBytes, Is.EqualTo(30));
        Assert.That(decoded.TotalBytes, Is.EqualTo(60));
        Assert.That(decoded.Partial, Is.False);
        Assert.That(decoded.Trees, Has.Length.EqualTo(1));
        Assert.That(decoded.Trees[0].TreeId, Is.EqualTo("tree-a"));
        Assert.That(decoded.SampledAt, Is.EqualTo(sampledAt));
    }

    [Test]
    public void ClusterStorageUsageReport_default_decodes_to_zeroed_values()
    {
        var decoded = _clusterSerializer.Deserialize(_clusterSerializer.SerializeToArray(default));

        Assert.That(decoded.TreeCount, Is.EqualTo(0));
        Assert.That(decoded.TotalBytes, Is.EqualTo(0));
        Assert.That(decoded.Partial, Is.False);
        Assert.That(decoded.Trees.IsDefault, Is.True);
    }
}
