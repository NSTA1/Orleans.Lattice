using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationShipEligibility"/>, the shipper's pure cycle-break and
/// legacy scalar-cursor rules (spec/replication/Replication.tla actions <c>Deliver</c> and
/// <c>ShipSkip</c>).
/// </summary>
[TestFixture]
public class ReplicationShipEligibilityTests
{
    private const string Local = "site-a";
    private const string Peer = "site-b";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public void IsShipEligible_accepts_a_local_origin_point_write()
    {
        Assert.That(ReplicationShipEligibility.IsShipEligible(Local, MutationKind.Set, Local), Is.True);
    }

    [Test]
    public void IsShipEligible_rejects_an_entry_applied_from_a_peer()
    {
        // The cycle-break: a peer-origin entry in the local WAL is never re-shipped, neither
        // back to its author nor on to a third cluster.
        Assert.That(ReplicationShipEligibility.IsShipEligible(Peer, MutationKind.Set, Local), Is.False);
    }

    [TestCase(null)]
    [TestCase("")]
    public void IsShipEligible_rejects_an_entry_with_no_origin(string? origin)
    {
        Assert.That(ReplicationShipEligibility.IsShipEligible(origin, MutationKind.Set, Local), Is.False);
    }

    [Test]
    public void IsShipEligible_rejects_an_empty_origin_even_when_the_local_cluster_id_is_empty()
    {
        Assert.That(ReplicationShipEligibility.IsShipEligible(string.Empty, MutationKind.Set, string.Empty), Is.False);
    }

    [Test]
    public void IsShipEligible_rejects_a_local_tombstone_reap_record()
    {
        Assert.That(ReplicationShipEligibility.IsShipEligible(Local, MutationKind.Tombstone, Local), Is.False);
    }

    [Test]
    public void IsShipEligible_accepts_a_local_saga_terminal()
    {
        Assert.That(ReplicationShipEligibility.IsShipEligible(Local, MutationKind.TxCommit, Local), Is.True);
    }

    [Test]
    public void IsShipEligible_compares_origins_ordinally()
    {
        Assert.That(ReplicationShipEligibility.IsShipEligible("SITE-A", MutationKind.Set, Local), Is.False);
    }

    [Test]
    public void IsBelowLegacyScalarCursor_never_drops_once_a_partition_cursor_is_saved()
    {
        // #1060: a new write on another leaf's clock routinely sits below the scalar cursor.
        Assert.That(
            ReplicationShipEligibility.IsBelowLegacyScalarCursor(false, false, Hlc(1), Hlc(5)),
            Is.False);
    }

    [Test]
    public void IsBelowLegacyScalarCursor_drops_an_entry_at_or_below_the_cursor_on_the_legacy_tick()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationShipEligibility.IsBelowLegacyScalarCursor(true, false, Hlc(5), Hlc(5)), Is.True);
            Assert.That(ReplicationShipEligibility.IsBelowLegacyScalarCursor(true, false, Hlc(4), Hlc(5)), Is.True);
            Assert.That(ReplicationShipEligibility.IsBelowLegacyScalarCursor(true, false, Hlc(6), Hlc(5)), Is.False);
        });
    }

    [Test]
    public void IsBelowLegacyScalarCursor_never_drops_a_zero_hlc_range_delete()
    {
        Assert.That(
            ReplicationShipEligibility.IsBelowLegacyScalarCursor(true, false, HybridLogicalClock.Zero, Hlc(5)),
            Is.False);
    }

    [Test]
    public void IsBelowLegacyScalarCursor_never_drops_a_saga_prepare_phase_entry()
    {
        Assert.That(
            ReplicationShipEligibility.IsBelowLegacyScalarCursor(true, true, Hlc(1), Hlc(5)),
            Is.False);
    }
}
