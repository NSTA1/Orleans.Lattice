namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The health of one replication link, derived by the status facade from the
/// link's backlog, error streak and time since last contact against configured
/// thresholds. Reported on <see cref="ReplicationPeerStatusEntry.Health"/>.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationLinkHealth)]
public enum ReplicationLinkHealth
{
    /// <summary>
    /// Not enough is known to judge the link: it has never made a successful
    /// contact and no threshold has been crossed. Also the value a peer that
    /// predates this field decodes to.
    /// </summary>
    Unknown = 0,

    /// <summary>Every signal is within its lagging threshold.</summary>
    Healthy = 1,

    /// <summary>At least one signal is past its lagging threshold and none is past its stalled threshold.</summary>
    Lagging = 2,

    /// <summary>At least one signal is past its stalled threshold.</summary>
    Stalled = 3,
}
