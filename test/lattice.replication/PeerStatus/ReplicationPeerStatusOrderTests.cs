namespace Orleans.Lattice.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="ReplicationPeerStatusOrder"/>: the total read order
/// over effective tree ids, and cursor construction.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatusOrderTests
{
    [Test]
    public void Compare_orders_tree_then_peer_then_direction()
    {
        const ReplicationContactDirection Out = ReplicationContactDirection.Outbound;
        const ReplicationContactDirection In = ReplicationContactDirection.Inbound;

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationPeerStatusOrder.Compare("a", "z", In, "b", "a", Out), Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare("b", "a", Out, "a", "z", In), Is.Positive);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", "a", In, "a", "b", Out), Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", "a", Out, "a", "a", In), Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", "a", In, "a", "a", In), Is.Zero);
        });
    }

    [Test]
    public void Compare_orders_a_tenant_tree_by_its_qualified_id_not_its_bare_name()
    {
        // "t/acme/x" and a default-tenant "x" are distinct keys: each orders under its
        // own effective id, so neither collides with nor shadows the other.
        Assert.Multiple(() =>
        {
            Assert.That(
                ReplicationPeerStatusOrder.Compare(
                    "t/acme/x", "p", ReplicationContactDirection.Outbound,
                    "x", "p", ReplicationContactDirection.Outbound),
                Is.Negative);
            Assert.That(
                ReplicationPeerStatusOrder.Compare(
                    "t/acme/x", "p", ReplicationContactDirection.Outbound,
                    "t/acme/x", "p", ReplicationContactDirection.Outbound),
                Is.Zero);
        });
    }

    [Test]
    public void Compare_is_ordinal_not_culture_sensitive()
    {
        // Ordinal: upper-case sorts before lower-case.
        Assert.That(
            ReplicationPeerStatusOrder.Compare(
                "B", "p", ReplicationContactDirection.Outbound,
                "a", "p", ReplicationContactDirection.Outbound),
            Is.Negative);
    }

    [Test]
    public void Compare_row_against_cursor_and_row_against_row_agree()
    {
        var row = new ReplicationPeerStatusRow("t/acme/x", "p", ReplicationContactDirection.Inbound, 0, 0, 0, 1d, 0);
        var other = new ReplicationPeerStatusRow("x", "p", ReplicationContactDirection.Outbound, 0, 0, 0, 1d, 0);
        var cursor = ReplicationPeerStatusOrder.CursorAfter(other);
        var againstCursor = ReplicationPeerStatusOrder.Compare(row.Tree, row.Peer, row.Direction, cursor);

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationPeerStatusOrder.Compare(row, other), Is.Negative);
            Assert.That(againstCursor, Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare(other, row), Is.Positive);
            Assert.That(ReplicationPeerStatusOrder.Compare(other.Tree, other.Peer, other.Direction, cursor), Is.Zero);
        });
    }

    [Test]
    public void CursorAfter_keys_on_the_effective_tree_id()
    {
        var row = new ReplicationPeerStatusRow(
            "t/acme/a/crm/contacts", "east", ReplicationContactDirection.Inbound, 0, 0, 0, double.NaN, 0);

        var cursor = ReplicationPeerStatusOrder.CursorAfter(row);

        Assert.Multiple(() =>
        {
            Assert.That(cursor, Is.EqualTo(new ReplicationPeerStatusCursor(
                "t/acme/a/crm/contacts", "east", ReplicationContactDirection.Inbound)));
            Assert.That(cursor.Tree, Is.SameAs(row.Tree), "the cursor reuses the row's id rather than rendering a copy");
        });
    }
}
