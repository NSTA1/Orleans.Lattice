using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="RemoteSnapshotMetadata"/>: the readonly
/// record-struct shape, the alias slot, and equality semantics. The
/// struct is the snapshot cut-point carried across a
/// <see cref="IRemoteSnapshotTransport.GetMetadataAsync"/> call; mis-set
/// slots would break the receiver's
/// <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
/// handoff.
/// </summary>
[TestFixture]
public class RemoteSnapshotMetadataTests
{
    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void Init_assigns_every_slot()
    {
        var frontier = new VersionVector();
        frontier.Tick("site-a");

        var metadata = new RemoteSnapshotMetadata
        {
            TreeName = "orders",
            SourceClusterId = "site-a",
            AsOfHlc = Hlc(500, 3),
            CausalStableFrontier = frontier,
        };

        Assert.Multiple(() =>
        {
            Assert.That(metadata.TreeName, Is.EqualTo("orders"));
            Assert.That(metadata.SourceClusterId, Is.EqualTo("site-a"));
            Assert.That(metadata.AsOfHlc, Is.EqualTo(Hlc(500, 3)));
            Assert.That(metadata.CausalStableFrontier, Is.SameAs(frontier));
        });
    }

    [Test]
    public void Default_value_carries_default_slot_values()
    {
        var metadata = default(RemoteSnapshotMetadata);

        Assert.Multiple(() =>
        {
            Assert.That(metadata.TreeName, Is.Null);
            Assert.That(metadata.SourceClusterId, Is.Null);
            Assert.That(metadata.AsOfHlc, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(metadata.CausalStableFrontier, Is.Null);
        });
    }

    [Test]
    public void Equality_treats_record_struct_value_semantics()
    {
        var frontier = new VersionVector();

        var a = new RemoteSnapshotMetadata
        {
            TreeName = "t",
            SourceClusterId = "s",
            AsOfHlc = Hlc(1),
            CausalStableFrontier = frontier,
        };

        var b = new RemoteSnapshotMetadata
        {
            TreeName = "t",
            SourceClusterId = "s",
            AsOfHlc = Hlc(1),
            CausalStableFrontier = frontier,
        };

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a == b, Is.True);
    }

    [Test]
    public void Equality_distinguishes_different_as_of_hlc()
    {
        var frontier = new VersionVector();

        var a = new RemoteSnapshotMetadata
        {
            TreeName = "t",
            SourceClusterId = "s",
            AsOfHlc = Hlc(1),
            CausalStableFrontier = frontier,
        };

        var b = a with { AsOfHlc = Hlc(2) };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Equality_distinguishes_different_source_cluster_id()
    {
        var a = new RemoteSnapshotMetadata
        {
            TreeName = "t",
            SourceClusterId = "site-a",
            AsOfHlc = Hlc(1),
            CausalStableFrontier = new VersionVector(),
        };

        var b = a with { SourceClusterId = "site-b" };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Alias_constant_is_olr_sm()
    {
        // Wire-format identity: the alias string is part of the wire
        // contract for the metadata RPC envelope. Rename = wire break.
        Assert.That(ReplicationTypeAliases.RemoteSnapshotMetadata, Is.EqualTo("olr.sm"));
    }
}
