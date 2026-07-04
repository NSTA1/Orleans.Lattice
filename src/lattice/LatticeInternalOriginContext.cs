using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient marker that positively identifies the current inbound grain
/// turn as originating from <em>inside</em> the cluster trust boundary (a
/// silo-to-silo or grain-to-grain hop) rather than from an external Orleans
/// client. It backs the defense-in-depth internal-origin assertion on the
/// physical shard and leaf grains, which enforce no policy of their own: all
/// access-gate enforcement lives on the <c>LatticeGrain</c> facade, so a direct
/// in-cluster call to a shard or leaf key would otherwise bypass policy.
/// </summary>
/// <remarks>
/// <para>
/// The marker is <b>re-derived fresh at every silo hop</b> by
/// <see cref="LatticeCapabilityStrippingCallFilter"/> from the actual caller
/// identity (<c>IGrainCallContext.SourceId</c>), never trusted from the wire, so
/// a malicious client cannot forge it: the same filter strips the marker (and
/// every other reserved capability key) from any call that arrives from an
/// external client. Every legitimate internal caller of a shard or leaf grain -
/// the facade, replication-apply, structural maintenance, the atomic-write saga,
/// bulk-load, and background services - is silo-sourced, so the filter stamps the
/// marker on its call and the assertion passes.
/// </para>
/// <para>
/// <b>Zero-cost default.</b> The marker is only ever set when the authorization
/// layer registered the stripping filter; a no-auth cluster never sets it, and
/// the shard / leaf guards short-circuit unless the
/// <see cref="LatticeInternalOriginEnforcementMarker"/> sentinel (registered
/// beside the filter by <c>AddLatticeAuth</c>) is present, so the physical
/// grains pay nothing.
/// </para>
/// </remarks>
internal static class LatticeInternalOriginContext
{
    /// <summary>
    /// Gets a value indicating whether the current turn originated inside the
    /// cluster trust boundary: the internal-origin marker is stamped on the
    /// ambient <see cref="RequestContext"/>, or the turn is otherwise gate-bypassed
    /// (a system-origin infrastructure call or an authorised view maintenance
    /// read / write). The default outside any internal scope is <c>false</c>.
    /// </summary>
    public static bool IsInternalGrainOrigin =>
        (RequestContext.Get(LatticeEventConstants.InternalGrainOriginRequestContextKey) is bool active && active)
        || LatticeAccessGateContext.IsGateBypassed;

    /// <summary>
    /// Stamps the internal-origin marker on the ambient
    /// <see cref="RequestContext"/> for the current turn. Called only by
    /// <see cref="LatticeCapabilityStrippingCallFilter"/> after it has confirmed
    /// the inbound call is silo-sourced (not from an external client).
    /// </summary>
    public static void MarkInternalGrainOrigin() =>
        RequestContext.Set(LatticeEventConstants.InternalGrainOriginRequestContextKey, true);

    /// <summary>
    /// Asserts that the current shard / leaf mutation turn originated inside the
    /// cluster trust boundary, throwing <see cref="LatticeAuthorizationDeniedException"/>
    /// when the turn is <em>not</em> internal - the signature of a direct external
    /// grain call that tried to bypass the facade's access gate. The caller is
    /// responsible for invoking this only when internal-origin enforcement is
    /// active (the capability-stripping filter is registered, signalled by
    /// <see cref="LatticeInternalOriginEnforcementMarker"/>); a no-auth cluster,
    /// or a cluster with a custom gate but no filter, never calls it and pays
    /// nothing. Legitimate internal callers (the facade, replication-apply,
    /// structural maintenance, the atomic-write saga, and bulk-load) are
    /// silo-sourced and carry the re-derived internal-origin marker, so only a
    /// direct external client call is rejected.
    /// </summary>
    /// <param name="treeId">The tree id of the grain being mutated, for the thrown exception.</param>
    /// <param name="operation">The operation being attempted, for the thrown exception.</param>
    /// <exception cref="LatticeAuthorizationDeniedException">
    /// The turn is not internal-origin.
    /// </exception>
    public static void EnsureInternalGrainOrigin(
        string treeId,
        LatticeOperation operation)
    {
        if (IsInternalGrainOrigin)
        {
            return;
        }

        throw new LatticeAuthorizationDeniedException(
            treeId,
            operation,
            LatticeSubject.AnonymousSubjectId,
            "Direct external call to an internal shard / leaf grain is refused. All access-gate "
            + "enforcement lives on the ILattice facade; the physical grains it delegates to require "
            + "an internal-origin marker that is established only inside the cluster trust boundary and "
            + "is stripped from any call arriving from an external client.");
    }

    /// <summary>
    /// Convenience overload that first resolves whether internal-origin enforcement
    /// is active (the capability-stripping filter is registered, signalled by the
    /// <see cref="LatticeInternalOriginEnforcementMarker"/> sentinel in the
    /// activation's service provider) and returns without cost when it is not.
    /// Intended for the internal coordinator / saga grains (the atomic-write saga
    /// and the structural lifecycle coordinators), whose mutating entry points are
    /// reachable by a direct external grain call that would bypass the
    /// <c>ILattice</c> facade's access gate. A single singleton service lookup per
    /// entry call is negligible for these low-frequency coordinators, so - unlike
    /// the shard / leaf hot path - they need not cache the enforcement flag.
    /// </summary>
    /// <param name="activationServices">
    /// The calling grain activation's <see cref="IServiceProvider"/>
    /// (<c>IGrainContext.ActivationServices</c>).
    /// </param>
    /// <param name="treeId">The tree id of the coordinator being driven, for the thrown exception.</param>
    /// <param name="operation">The operation being attempted, for the thrown exception.</param>
    /// <exception cref="LatticeAuthorizationDeniedException">
    /// Enforcement is active and the turn is not internal-origin.
    /// </exception>
    public static void EnsureInternalGrainOrigin(
        IServiceProvider activationServices,
        string treeId,
        LatticeOperation operation)
    {
        if (activationServices.GetService<LatticeInternalOriginEnforcementMarker>() is null)
        {
            return;
        }

        EnsureInternalGrainOrigin(treeId, operation);
    }
}
