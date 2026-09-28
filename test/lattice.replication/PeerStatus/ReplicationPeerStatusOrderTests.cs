namespace Orleans.Lattice.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="ReplicationPeerStatusOrder"/>: tenant rendering of
/// tree ids, the total read order, and cursor construction.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatusOrderTests
{
    [Test]
    public void DisplayTree_removes_the_callers_own_tenant_qualification()
    {
        var display = ReplicationPeerStatusOrder.DisplayTree("t/acme/a/crm/contacts", "t/acme/", out var stripped).ToString();

        Assert.Multiple(() =>
        {
            Assert.That(display, Is.EqualTo("a/crm/contacts"));
            Assert.That(stripped, Is.True);
        });
    }

    [TestCase("t/other/a/crm/contacts", "t/acme/")]
    [TestCase("a/crm/contacts", "t/acme/")]
    [TestCase("t/acme/", "t/acme/")]
    [TestCase("t/acme/orders", null)]
    [TestCase("t/acme/orders", "")]
    public void DisplayTree_leaves_other_ids_whole(string tree, string? strip)
    {
        var display = ReplicationPeerStatusOrder.DisplayTree(tree, strip, out var stripped).ToString();

        Assert.Multiple(() =>
        {
            Assert.That(display, Is.EqualTo(tree));
            Assert.That(stripped, Is.False);
        });
    }

    [Test]
    public void DisplayTreeString_returns_the_same_reference_when_nothing_is_removed()
    {
        const string tree = "orders";

        var display = ReplicationPeerStatusOrder.DisplayTreeString(tree, "t/acme/", out var stripped);

        Assert.Multiple(() =>
        {
            Assert.That(display, Is.SameAs(tree));
            Assert.That(stripped, Is.False);
        });
    }

    [Test]
    public void DisplayTreeString_allocates_the_rendered_id_when_stripping()
    {
        var display = ReplicationPeerStatusOrder.DisplayTreeString("t/acme/orders", "t/acme/", out var stripped);

        Assert.Multiple(() =>
        {
            Assert.That(display, Is.EqualTo("orders"));
            Assert.That(stripped, Is.True);
        });
    }

    [Test]
    public void Compare_orders_tree_then_own_tenant_then_peer_then_direction()
    {
        const ReplicationContactDirection Out = ReplicationContactDirection.Outbound;
        const ReplicationContactDirection In = ReplicationContactDirection.Inbound;

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationPeerStatusOrder.Compare("a", false, "z", In, "b", false, "a", Out), Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", true, "z", In, "a", false, "a", Out), Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", false, "a", Out, "a", true, "a", Out), Is.Positive);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", false, "a", In, "a", false, "b", Out), Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", false, "a", Out, "a", false, "a", In), Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare("a", false, "a", In, "a", false, "a", In), Is.Zero);
        });
    }

    [Test]
    public void Compare_is_ordinal_not_culture_sensitive()
    {
        // Ordinal: upper-case sorts before lower-case.
        Assert.That(
            ReplicationPeerStatusOrder.Compare(
                "B", false, "p", ReplicationContactDirection.Outbound,
                "a", false, "p", ReplicationContactDirection.Outbound),
            Is.Negative);
    }

    [Test]
    public void Compare_row_against_cursor_and_row_against_row_agree()
    {
        var row = new ReplicationPeerStatusRow("t/acme/x", "p", ReplicationContactDirection.Inbound, 0, 0, 0, 1d, 0);
        var other = new ReplicationPeerStatusRow("x", "p", ReplicationContactDirection.Outbound, 0, 0, 0, 1d, 0);
        var cursor = ReplicationPeerStatusOrder.CursorAfter(other, "t/acme/");
        var display = ReplicationPeerStatusOrder.DisplayTree(row.Tree, "t/acme/", out var stripped);
        var againstCursor = ReplicationPeerStatusOrder.Compare(display, stripped, row.Peer, row.Direction, cursor);

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationPeerStatusOrder.Compare(row, other, "t/acme/"), Is.Negative);
            Assert.That(againstCursor, Is.Negative);
            Assert.That(ReplicationPeerStatusOrder.Compare(other, row, "t/acme/"), Is.Positive);
        });
    }

    [Test]
    public void CursorAfter_keys_on_the_display_id_and_never_the_composed_id()
    {
        var row = new ReplicationPeerStatusRow(
            "t/acme/a/crm/contacts", "east", ReplicationContactDirection.Inbound, 0, 0, 0, double.NaN, 0);

        var cursor = ReplicationPeerStatusOrder.CursorAfter(row, "t/acme/");

        Assert.That(cursor, Is.EqualTo(new ReplicationPeerStatusCursor(
            "a/crm/contacts", Stripped: true, "east", ReplicationContactDirection.Inbound)));
    }
}
