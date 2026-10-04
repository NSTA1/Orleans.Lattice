using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// Boundary tests for <see cref="ReplicationLinkHealthClassifier"/>. The
/// classifier reads no clock - a row's elapsed time is fixed when it is read - so
/// every boundary is asserted exactly, with no wall-clock dependence.
/// </summary>
[TestFixture]
public sealed class ReplicationLinkHealthClassifierTests
{
    private static readonly LatticeReplicationStatusOptions Defaults = new();

    private static ReplicationPeerStatusRow Outbound(long entries = 0, long errors = 0, double contactSeconds = 1) =>
        new("orders", "east", ReplicationContactDirection.Outbound, entries, 0, errors, contactSeconds, 0);

    private static ReplicationPeerStatusRow Inbound(long errors = 0, double contactSeconds = 1) =>
        new("orders", "east", ReplicationContactDirection.Inbound, 0, 0, errors, contactSeconds, 0);

    private static ReplicationLinkHealth Classify(ReplicationPeerStatusRow row, LatticeReplicationStatusOptions? options = null) =>
        ReplicationLinkHealthClassifier.Classify(row, options ?? Defaults);

    [Test]
    public void Peer_awaiting_a_reseed_after_a_trim_lost_records_is_stalled_however_healthy_its_counters()
    {
        // #4534: the sender withholds saga records until the peer re-seeds.
        Assert.Multiple(() =>
        {
            Assert.That(Classify(Outbound() with { ReseedRequiredSeconds = 0 }), Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(Classify(Outbound(contactSeconds: double.NaN) with { ReseedRequiredSeconds = 5 }),
                Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(Classify(Outbound()), Is.EqualTo(ReplicationLinkHealth.Healthy));
        });
    }

    [TestCase(0L, ReplicationLinkHealth.Healthy)]
    [TestCase(LatticeReplicationStatusOptions.DefaultLaggingEntriesBehind, ReplicationLinkHealth.Healthy)]
    [TestCase(LatticeReplicationStatusOptions.DefaultLaggingEntriesBehind + 1, ReplicationLinkHealth.Lagging)]
    [TestCase(LatticeReplicationStatusOptions.DefaultStalledEntriesBehind, ReplicationLinkHealth.Lagging)]
    [TestCase(LatticeReplicationStatusOptions.DefaultStalledEntriesBehind + 1, ReplicationLinkHealth.Stalled)]
    public void Outbound_backlog_boundaries_are_strictly_greater_than(long entries, ReplicationLinkHealth expected)
    {
        Assert.That(Classify(Outbound(entries: entries)), Is.EqualTo(expected));
    }

    [TestCase(30d, ReplicationLinkHealth.Healthy)]
    [TestCase(30.001d, ReplicationLinkHealth.Lagging)]
    [TestCase(300d, ReplicationLinkHealth.Lagging)]
    [TestCase(300.001d, ReplicationLinkHealth.Stalled)]
    public void Outbound_contact_boundaries_are_strictly_greater_than(double seconds, ReplicationLinkHealth expected)
    {
        Assert.That(Classify(Outbound(contactSeconds: seconds)), Is.EqualTo(expected));
    }

    [TestCase(LatticeReplicationStatusOptions.DefaultLaggingConsecutiveErrors, ReplicationLinkHealth.Healthy)]
    [TestCase(LatticeReplicationStatusOptions.DefaultLaggingConsecutiveErrors + 1, ReplicationLinkHealth.Lagging)]
    [TestCase(LatticeReplicationStatusOptions.DefaultStalledConsecutiveErrors, ReplicationLinkHealth.Lagging)]
    [TestCase(LatticeReplicationStatusOptions.DefaultStalledConsecutiveErrors + 1, ReplicationLinkHealth.Stalled)]
    public void Error_streak_boundaries_apply_in_both_directions(long errors, ReplicationLinkHealth expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(Classify(Outbound(errors: errors)), Is.EqualTo(expected));
            Assert.That(Classify(Inbound(errors: errors)), Is.EqualTo(expected));
        });
    }

    [Test]
    public void The_worst_signal_wins()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Classify(Outbound(entries: 2_000, contactSeconds: 400)), Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(Classify(Outbound(entries: 20_000, errors: 6)), Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(Classify(Outbound(entries: 2_000, errors: 6, contactSeconds: 40)), Is.EqualTo(ReplicationLinkHealth.Lagging));
        });
    }

    [Test]
    public void Inbound_silence_is_healthy_by_default()
    {
        Assert.That(Classify(Inbound(contactSeconds: 86_400)), Is.EqualTo(ReplicationLinkHealth.Healthy));
    }

    [Test]
    public void Inbound_silence_bounds_apply_when_configured()
    {
        var options = new LatticeReplicationStatusOptions
        {
            InboundLaggingAfterNoContact = TimeSpan.FromMinutes(1),
            InboundStalledAfterNoContact = TimeSpan.FromMinutes(10),
        };

        Assert.Multiple(() =>
        {
            Assert.That(Classify(Inbound(contactSeconds: 60), options), Is.EqualTo(ReplicationLinkHealth.Healthy));
            Assert.That(Classify(Inbound(contactSeconds: 61), options), Is.EqualTo(ReplicationLinkHealth.Lagging));
            Assert.That(Classify(Inbound(contactSeconds: 601), options), Is.EqualTo(ReplicationLinkHealth.Stalled));
        });
    }

    [Test]
    public void Outbound_contact_bounds_do_not_apply_to_inbound_links()
    {
        Assert.That(Classify(Inbound(contactSeconds: 1_000)), Is.EqualTo(ReplicationLinkHealth.Healthy));
    }

    [Test]
    public void A_link_never_contacted_that_crosses_no_bound_is_unknown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Classify(Outbound(contactSeconds: double.NaN)), Is.EqualTo(ReplicationLinkHealth.Unknown));
            Assert.That(Classify(Outbound(errors: 5, contactSeconds: double.NaN)), Is.EqualTo(ReplicationLinkHealth.Unknown));
            Assert.That(Classify(Inbound(contactSeconds: double.NaN)), Is.EqualTo(ReplicationLinkHealth.Unknown));
        });
    }

    [Test]
    public void A_link_never_contacted_is_judged_on_the_signals_it_does_have()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Classify(Outbound(errors: 6, contactSeconds: double.NaN)), Is.EqualTo(ReplicationLinkHealth.Lagging));
            Assert.That(Classify(Outbound(entries: 20_000, contactSeconds: double.NaN)), Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(Classify(Inbound(errors: 51, contactSeconds: double.NaN)), Is.EqualTo(ReplicationLinkHealth.Stalled));
        });
    }

    [Test]
    public void A_null_bound_disables_its_signal()
    {
        var options = new LatticeReplicationStatusOptions
        {
            LaggingEntriesBehind = null,
            StalledEntriesBehind = null,
            LaggingAfterNoContact = null,
            StalledAfterNoContact = null,
            LaggingConsecutiveErrors = null,
            StalledConsecutiveErrors = null,
        };

        Assert.That(
            Classify(Outbound(entries: long.MaxValue, errors: long.MaxValue, contactSeconds: 1e9), options),
            Is.EqualTo(ReplicationLinkHealth.Healthy));
    }

    [Test]
    public void Only_the_lagging_bound_may_be_set()
    {
        var options = new LatticeReplicationStatusOptions { StalledEntriesBehind = null };

        Assert.That(Classify(Outbound(entries: long.MaxValue), options), Is.EqualTo(ReplicationLinkHealth.Lagging));
    }

    [Test]
    public void Classify_null_options_throws()
    {
        Assert.That(() => ReplicationLinkHealthClassifier.Classify(Outbound(), null!), Throws.ArgumentNullException);
    }
}
