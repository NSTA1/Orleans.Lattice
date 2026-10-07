using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Thrown by <see cref="ILatticeReplicationPeerAdmin.DecommissionPeerAsync"/> when
/// the caller is authorized but no replication engine is hosted in this
/// process, so there is no <c>ILatticeReplicationPeerDecommissioner</c> to
/// carry the decommission out. This is a hosting-topology fault, not an
/// authorization or validation failure: the facade package
/// (<c>AddLatticeReplicationApi</c>) can be registered without the engine
/// package (<c>AddLatticeReplication</c>), in which case the control resolves
/// with no decommissioner and must fail closed rather than null-reference.
/// </summary>
/// <remarks>
/// Derives from <see cref="InvalidOperationException"/> for the same reason
/// <c>LatticeReplicationPeerStillConfiguredException</c> does: a caller that
/// broadly catches <see cref="InvalidOperationException"/> and retries cannot
/// succeed by retrying alone here either, since no amount of retrying hosts an
/// engine that was never registered. Implementing
/// <see cref="ILatticeDomainFault"/> lets a handler decline to absorb it via
/// <c>catch (InvalidOperationException ex) when (ex is not ILatticeDomainFault)</c>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.LatticeReplicationEngineNotHostedException)]
public sealed class LatticeReplicationEngineNotHostedException : InvalidOperationException, ILatticeDomainFault
{
    /// <summary>Initializes a new <see cref="LatticeReplicationEngineNotHostedException"/>.</summary>
    public LatticeReplicationEngineNotHostedException()
        : base("No replication engine is hosted in this process, so the peer could not be decommissioned.")
    {
    }

    /// <summary>Initializes a new <see cref="LatticeReplicationEngineNotHostedException"/> with the given message.</summary>
    /// <param name="message">The exception message.</param>
    public LatticeReplicationEngineNotHostedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new <see cref="LatticeReplicationEngineNotHostedException"/> with the given message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public LatticeReplicationEngineNotHostedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Same-silo deep copier for <see cref="LatticeReplicationEngineNotHostedException"/>.
/// Orleans registers a same-silo deep copier for <see cref="System.Exception"/>
/// but not for its BCL subclasses, so a <c>[GenerateSerializer]</c> exception
/// deriving from <see cref="InvalidOperationException"/> fails a co-located
/// grain-result copy with an opaque <see cref="KeyNotFoundException"/>
/// ("Could not find a base type copier for ...") unless a copier is registered
/// explicitly. The exception is immutable once constructed, so returning the
/// same instance is a correct deep copy.
/// </summary>
[RegisterCopier]
internal sealed class LatticeReplicationEngineNotHostedExceptionCopier
    : IDeepCopier<LatticeReplicationEngineNotHostedException>
{
    public LatticeReplicationEngineNotHostedException DeepCopy(
        LatticeReplicationEngineNotHostedException input, CopyContext context) => input;
}
