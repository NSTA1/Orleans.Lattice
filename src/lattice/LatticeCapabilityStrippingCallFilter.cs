using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// A silo-wide <see cref="IIncomingGrainCallFilter"/> that enforces the trust
/// boundary for the reserved internal <see cref="RequestContext"/> capability
/// keys. The authorization model relies on those keys - the system-origin
/// gate-bypass marker, the view read / write scopes, the internal-origin marker,
/// and the replication / maintenance origin markers - being established only
/// <em>inside</em> the cluster and never asserted by an external caller. This
/// filter guarantees that invariant on every inbound call. The caller credential
/// is deliberately exempt (it is an authenticated input, not a bypass
/// capability); see <see cref="ReservedCapabilityKeys"/>.
/// </summary>
/// <remarks>
/// <para>
/// The trust origin is re-derived from the actual caller identity
/// (<c>IGrainCallContext.SourceId</c>) on every silo hop, so it can never be
/// forged from the wire:
/// </para>
/// <list type="bullet">
/// <item>
/// A <b>client-sourced</b> call from a genuine external Orleans client (one that is
/// neither a silo nor this cluster's own in-silo hosted client) may assert no
/// internal bypass capability, so every reserved capability key it may have seeded
/// is stripped before the grain body runs. A malicious client that manually sets,
/// for example,
/// <see cref="LatticeEventConstants.AccessGateSystemOriginRequestContextKey"/> or
/// the internal-origin marker therefore cannot smuggle a forged system-origin or
/// internal-origin into a grain call; enforcement treats it as its real
/// (resolved, non-system) identity. The caller credential
/// (<see cref="LatticeEventConstants.CredentialRequestContextKey"/>) is
/// intentionally <em>not</em> stripped: it is an authentication input the silo
/// always re-validates through the registered credential authenticator, so a
/// forged credential resolves to the anonymous subject rather than escalating.
/// </item>
/// <item>
/// A <b>silo-sourced</b> call, or a call from this cluster's own in-silo hosted
/// client (the co-hosted gRPC gateway and other in-silo infrastructure, identified
/// by the Orleans <c>hosted-</c> client-id prefix), is inside the trust boundary,
/// so the internal-origin marker is stamped fresh (never trusted from the wire) for
/// the shard / leaf internal-origin assertion to consult. Legitimate silo-to-silo
/// propagation is preserved because the marker is re-established at each hop from
/// the caller identity rather than carried as trusted state.
/// </item>
/// </list>
/// <para>
/// Registered only by the authorization layer (<c>AddLatticeAuth</c>), so a
/// no-auth cluster never installs it and pays nothing. Deliberately does not
/// strip the public request-parameter contexts (idempotency, HLC override, vector
/// clock, origin cluster id): those are legitimately supplied by an external
/// caller and confer no trust capability.
/// </para>
/// </remarks>
internal sealed class LatticeCapabilityStrippingCallFilter : IIncomingGrainCallFilter
{
    /// <summary>
    /// The reserved <see cref="RequestContext"/> keys that confer a trust,
    /// bypass, or internal-origin capability - each causes access-gate
    /// enforcement to be <em>skipped</em> without any authentication - and
    /// therefore may never be asserted by an external client. The credential
    /// context (<see cref="LatticeEventConstants.CredentialRequestContextKey"/>)
    /// is deliberately NOT in this set: it is an authentication <em>input</em>,
    /// not a capability. It is always run through the registered credential
    /// authenticator, so a client that forges it cannot escalate (a credential
    /// that fails authentication resolves to the anonymous subject); stripping it
    /// would instead break the supported client-side credential-assertion API.
    /// The public request-parameter contexts (idempotency, HLC override, vector
    /// clock, origin cluster id) are likewise excluded - they carry no capability
    /// and are legitimately client-supplied.
    /// </summary>
    private static readonly string[] ReservedCapabilityKeys =
    [
        LatticeEventConstants.AccessGateSystemOriginRequestContextKey,
        LatticeEventConstants.ViewWriteRequestContextKey,
        LatticeEventConstants.ViewReadRequestContextKey,
        LatticeEventConstants.InternalGrainOriginRequestContextKey,
        LatticeEventConstants.MaintenanceRequestContextKey,
        LatticeEventConstants.CommitLogSourceRequestContextKey,
        LatticeEventConstants.ApplyOffsetRequestContextKey,
        LatticeEventConstants.ApplyOffsetPartitionRequestContextKey,
    ];

    /// <inheritdoc />
    public Task Invoke(IIncomingGrainCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.SourceId is { } sourceId && sourceId.IsClient() && !IsInSiloHostedClient(sourceId))
        {
            // Genuine external Orleans client: it may assert no internal capability,
            // so strip every reserved capability key it may have seeded before the
            // grain body runs. The in-silo hosted client (the co-hosted gRPC gateway
            // and other in-silo infrastructure) is excluded below - it is inside the
            // trust boundary and legitimately establishes system-origin scopes.
            StripReservedCapabilityKeys();
        }
        else
        {
            // Grain-sourced call, or a call from this cluster's in-silo hosted client
            // (gateway / infrastructure): inside the trust boundary. Stamp the
            // internal-origin marker, re-derived on this hop, for the shard / leaf
            // assertion. This never grants a data-plane bypass (the facade still
            // enforces the access gate); it only satisfies the defense-in-depth
            // internal-origin check on the shard / leaf mutation entry points.
            LatticeInternalOriginContext.MarkInternalGrainOrigin();
        }

        return context.Invoke();
    }

    /// <summary>
    /// Determines whether a client-sourced call originates from this cluster's own
    /// in-silo hosted client rather than a genuine external Orleans client. Orleans
    /// assigns the in-silo hosted client a grain-id key of the form
    /// <c>hosted-{siloAddress}</c> (see <c>HostedClient.CreateHostedClientGrainId</c>),
    /// whereas an external client is assigned a random key. In-silo infrastructure
    /// (the co-hosted gRPC data gateway, the auth initializer, and similar hosted
    /// services) issues grain calls through this hosted client, so it is inside the
    /// trust boundary and must be allowed to carry the internal capability keys it
    /// legitimately establishes.
    /// </summary>
    private static bool IsInSiloHostedClient(GrainId sourceId)
        => sourceId.Key.AsSpan().StartsWith(HostedClientKeyPrefix);

    private static readonly byte[] HostedClientKeyPrefix =
        System.Text.Encoding.UTF8.GetBytes("hosted-");

    private static void StripReservedCapabilityKeys()
    {
        foreach (var key in ReservedCapabilityKeys)
        {
            RequestContext.Remove(key);
        }
    }
}
