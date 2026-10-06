using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Thrown by <see cref="ILatticeReplicationPeerDecommissioner.DecommissionPeerAsync"/>
/// when the requested peer cluster id is still present in the cluster-wide
/// <see cref="IReplicationTopology.CurrentPeers">configured peer set</see>.
/// Decommissioning is a permanent, fail-closed verb (unlike removing a peer
/// from a tree's enrolment, which is a reversible detach): it is refused while
/// the peer remains reachable through live configuration, because marking a
/// still-configured peer decommissioned would let a subsequent shipping or
/// bootstrap cycle re-enrol it under an id the operator just declared gone for
/// good. The caller must first remove the peer from
/// <c>LatticeReplicationOptions.ReplicationPeers</c> (a configuration change,
/// not an API call this package exposes) before decommissioning it.
/// <para>
/// It derives from <see cref="System.InvalidOperationException"/> for
/// backwards compatibility, but that inheritance is a hazard rather than a
/// convenience: a broad <c>catch (InvalidOperationException)</c> absorbs the
/// rejection and typically retries, which cannot succeed until the peer is
/// removed from configuration. This type therefore implements
/// <see cref="ILatticeDomainFault"/>, so a broad handler declines it with
/// <c>catch (InvalidOperationException ex) when (ex is not ILatticeDomainFault)</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.LatticeReplicationPeerStillConfiguredException)]
public sealed class LatticeReplicationPeerStillConfiguredException : InvalidOperationException, ILatticeDomainFault
{
    /// <summary>
    /// The peer cluster id whose decommission was rejected. Empty on the
    /// context-free constructors.
    /// </summary>
    [Id(0)]
    public string PeerClusterId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeReplicationPeerStillConfiguredException()
    {
        PeerClusterId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected decommission.</param>
    public LatticeReplicationPeerStillConfiguredException(string message) : base(message)
    {
        PeerClusterId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected decommission.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeReplicationPeerStillConfiguredException(string message, Exception innerException)
        : base(message, innerException)
    {
        PeerClusterId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the peer cluster id whose
    /// decommission was rejected. The primary production throw shape.
    /// </summary>
    /// <param name="message">Actionable context instructing the operator to remove the peer from configuration first.</param>
    /// <param name="peerClusterId">The peer cluster id whose decommission was rejected.</param>
    public LatticeReplicationPeerStillConfiguredException(string message, string peerClusterId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(peerClusterId);
        PeerClusterId = peerClusterId;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeReplicationPeerStillConfiguredException"/>. Orleans deep-copies a grain
/// result across an in-process (co-located) boundary instead of serialising it, and
/// the generated copier for a <c>[GenerateSerializer]</c> exception deriving from a
/// BCL exception subclass requests a copier for that base type, which Orleans does
/// not provide - so a same-silo throw would fail with an opaque
/// <c>KeyNotFoundException</c> ("Could not find a base type copier for ...") and mask
/// the real, actionable fault. An exception is immutable once constructed, so
/// returning the same instance is a correct deep copy and keeps the typed exception
/// intact (the cross-silo serialise path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class LatticeReplicationPeerStillConfiguredExceptionCopier : IDeepCopier<LatticeReplicationPeerStillConfiguredException>
{
    /// <inheritdoc />
    public LatticeReplicationPeerStillConfiguredException DeepCopy(LatticeReplicationPeerStillConfiguredException input, CopyContext context) => input;
}
