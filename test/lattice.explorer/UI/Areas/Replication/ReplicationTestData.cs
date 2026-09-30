using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>Builders for replication status and enrolment rows.</summary>
internal static class ReplicationTestData
{
    public static ReplicationPeerStatusEntry Link(
        string tree,
        string peer,
        ReplicationLinkHealth health = ReplicationLinkHealth.Healthy,
        ReplicationLinkDirection direction = ReplicationLinkDirection.Outbound,
        long entries = 0,
        long bytes = 0,
        long errors = 0,
        TimeSpan? contact = null,
        long inFlight = 0,
        bool neverContacted = false) =>
        new(tree, peer, direction, entries, bytes, errors, neverContacted ? null : contact ?? TimeSpan.FromSeconds(2), inFlight, health);

    public static ReplicationTreeConfigEntry Tree(
        string tree,
        bool enabled = true,
        LatticeMergeMode? mode = LatticeMergeMode.LwwRegister,
        ReplicationEnrollmentSource source = ReplicationEnrollmentSource.Runtime,
        bool ambiguous = false) =>
        new(tree, enabled, mode, ambiguous) { Source = source };

    /// <summary>
    /// A small estate: us-east (healthy both ways), ap-south (outbound stalled,
    /// inbound lagging) and sa-east (unknown), across a cluster tree and two app trees.
    /// </summary>
    public static IEnumerable<ReplicationPeerStatusEntry> Estate() =>
    [
        Link("orders", "us-east", ReplicationLinkHealth.Healthy),
        Link("orders", "us-east", ReplicationLinkHealth.Healthy, ReplicationLinkDirection.Inbound),
        Link("a/crm/contacts", "ap-south", ReplicationLinkHealth.Stalled, entries: 1204, bytes: 3_355_443, errors: 7, contact: TimeSpan.FromMinutes(14)),
        Link("a/crm/contacts", "ap-south", ReplicationLinkHealth.Lagging, ReplicationLinkDirection.Inbound, entries: 40, bytes: 2048),
        Link("a/billing/invoices", "ap-south", ReplicationLinkHealth.Healthy),
        Link("a/billing/invoices", "sa-east", ReplicationLinkHealth.Unknown, neverContacted: true),
    ];
}
