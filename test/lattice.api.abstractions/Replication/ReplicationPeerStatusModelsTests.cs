using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Abstractions.Tests.Replication;

/// <summary>
/// Exercises the hand-written constructors, guards, defaults and helpers of the
/// replication peer-status contract DTOs. The serialization fixture only
/// round-trips uninitialised instances, so this logic is otherwise uncovered.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatusModelsTests
{
    private static ReplicationPeerStatusEntry Entry(string tree = "a/crm/contacts") =>
        new(
            tree,
            "east",
            ReplicationLinkDirection.Inbound,
            entriesBehind: 1,
            bytesBehind: 2,
            consecutiveErrors: 3,
            timeSinceLastContact: TimeSpan.FromSeconds(4),
            inFlight: 5,
            health: ReplicationLinkHealth.Lagging);

    [Test]
    public void ReplicationPeerStatusEntry_ctor_captures_every_field()
    {
        var entry = Entry();

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo("a/crm/contacts"));
            Assert.That(entry.PeerRegionId, Is.EqualTo("east"));
            Assert.That(entry.Direction, Is.EqualTo(ReplicationLinkDirection.Inbound));
            Assert.That(entry.EntriesBehind, Is.EqualTo(1));
            Assert.That(entry.BytesBehind, Is.EqualTo(2));
            Assert.That(entry.ConsecutiveErrors, Is.EqualTo(3));
            Assert.That(entry.TimeSinceLastContact, Is.EqualTo(TimeSpan.FromSeconds(4)));
            Assert.That(entry.InFlight, Is.EqualTo(5));
            Assert.That(entry.Health, Is.EqualTo(ReplicationLinkHealth.Lagging));
        });
    }

    [Test]
    public void ReplicationPeerStatusEntry_ctor_accepts_a_link_never_contacted()
    {
        var entry = new ReplicationPeerStatusEntry(
            "orders", "east", ReplicationLinkDirection.Outbound, 0, 0, 0, null, 0, ReplicationLinkHealth.Unknown);

        Assert.That(entry.TimeSinceLastContact, Is.Null);
    }

    [Test]
    public void ReplicationPeerStatusEntry_ctor_throws_for_null_ids()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ReplicationPeerStatusEntry(null!, "east", default, 0, 0, 0, null, 0, default),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ReplicationPeerStatusEntry("orders", null!, default, 0, 0, 0, null, 0, default),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void ReplicationPeerStatusPage_ctor_captures_state()
    {
        var peers = new[] { Entry() };

        var page = new ReplicationPeerStatusPage("west", peers, "token");

        Assert.Multiple(() =>
        {
            Assert.That(page.LocalRegionId, Is.EqualTo("west"));
            Assert.That(page.Peers, Is.SameAs(peers));
            Assert.That(page.ContinuationToken, Is.EqualTo("token"));
        });
    }

    [Test]
    public void ReplicationPeerStatusPage_ctor_throws_for_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ReplicationPeerStatusPage(null!, Array.Empty<ReplicationPeerStatusEntry>(), null),
                Throws.ArgumentNullException);
            Assert.That(() => new ReplicationPeerStatusPage("west", null!, null), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void ReplicationPeerStatusPage_Empty_is_a_final_page_with_no_rows()
    {
        var page = ReplicationPeerStatusPage.Empty("west");

        Assert.Multiple(() =>
        {
            Assert.That(page.LocalRegionId, Is.EqualTo("west"));
            Assert.That(page.Peers, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public void ReplicationPeerStatusPage_Empty_throws_for_a_null_region()
    {
        Assert.That(() => ReplicationPeerStatusPage.Empty(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ReplicationPeerStatusQuery_All_reads_everything_from_the_start()
    {
        var all = ReplicationPeerStatusQuery.All;

        Assert.Multiple(() =>
        {
            Assert.That(all.TreeId, Is.Null);
            Assert.That(all.PeerRegionId, Is.Null);
            Assert.That(all.ContinuationToken, Is.Null);
            Assert.That(all.ResolvePageSize(), Is.EqualTo(ReplicationPeerStatusQuery.DefaultPageSize));
        });
    }

    [TestCase(0, ReplicationPeerStatusQuery.DefaultPageSize)]
    [TestCase(1, 1)]
    [TestCase(250, 250)]
    [TestCase(ReplicationPeerStatusQuery.MaxPageSize, ReplicationPeerStatusQuery.MaxPageSize)]
    [TestCase(ReplicationPeerStatusQuery.MaxPageSize + 1, ReplicationPeerStatusQuery.MaxPageSize)]
    [TestCase(int.MaxValue, ReplicationPeerStatusQuery.MaxPageSize)]
    public void ReplicationPeerStatusQuery_ResolvePageSize_defaults_and_clamps(int requested, int expected)
    {
        Assert.That(new ReplicationPeerStatusQuery { PageSize = requested }.ResolvePageSize(), Is.EqualTo(expected));
    }

    [Test]
    public void ReplicationPeerStatusQuery_ResolvePageSize_rejects_a_negative_size()
    {
        Assert.That(
            () => new ReplicationPeerStatusQuery { PageSize = -1 }.ResolvePageSize(),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReplicationPeerStatusQuery_ToString_does_not_throw_for_a_negative_size()
    {
        Assert.That(() => new ReplicationPeerStatusQuery { PageSize = -1 }.ToString(), Throws.Nothing);
    }

    [Test]
    public void ReplicationLinkHealth_default_is_Unknown_and_values_are_pinned()
    {
        Assert.Multiple(() =>
        {
            Assert.That(default(ReplicationLinkHealth), Is.EqualTo(ReplicationLinkHealth.Unknown));
            Assert.That((int)ReplicationLinkHealth.Healthy, Is.EqualTo(1));
            Assert.That((int)ReplicationLinkHealth.Lagging, Is.EqualTo(2));
            Assert.That((int)ReplicationLinkHealth.Stalled, Is.EqualTo(3));
        });
    }

    [Test]
    public void ReplicationLinkDirection_values_are_pinned()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)ReplicationLinkDirection.Outbound, Is.Zero);
            Assert.That((int)ReplicationLinkDirection.Inbound, Is.EqualTo(1));
        });
    }
}
