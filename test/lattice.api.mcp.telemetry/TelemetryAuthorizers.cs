using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Builds the <see cref="TelemetryAccessAuthorizer"/> the telemetry tool handlers
/// consult, in the two postures a fixture needs: a cluster whose caller holds the
/// cluster-wide <c>Telemetry</c> capability, and one whose caller does not.
/// </summary>
/// <remarks>
/// <see cref="Allowed"/> constructs the authorizer with no access gate at all,
/// which is exactly the authorization-off posture: the capability check
/// short-circuits to allow, so a fixture that is testing payload mapping, range
/// guardrails, or metric-access filtering observes the behaviour it observed
/// before the capability check existed.
/// </remarks>
internal static class TelemetryAuthorizers
{
    /// <summary>An authorizer that admits every caller (no gate registered).</summary>
    public static TelemetryAccessAuthorizer Allowed() => new();

    /// <summary>An authorizer backed by a gate that denies every request.</summary>
    public static TelemetryAccessAuthorizer Denied() => new(new DenyingGate());

    /// <summary>
    /// An authorizer backed by a gate that allows the request but attaches a
    /// per-key filter. Cluster telemetry is attached to no key, so a filtered
    /// allow has not authorized the whole scope and must be refused.
    /// </summary>
    public static TelemetryAccessAuthorizer Filtered() => new(new FilteringGate());

    /// <summary>
    /// An authorizer backed by a gate that allows Telemetry only over a tenant's
    /// capability scope (#5582) and denies it cluster-wide, modelling a caller
    /// holding a tenant-confined Telemetry grant.
    /// </summary>
    public static TelemetryAccessAuthorizer TenantCapabilityOnly() => new(new TenantCapabilityGate());

    private sealed class TenantCapabilityGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(request.Operation == LatticeOperation.Telemetry
                && request.TreeId.StartsWith(LatticeTenantAdminScope.TenantScopePrefix, StringComparison.Ordinal)
                    ? LatticeAccessDecision.Allow()
                    : LatticeAccessDecision.Deny("The caller holds Telemetry only over a tenant capability scope."));
    }

    private sealed class DenyingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny("The caller does not hold the Telemetry capability."));
    }

    private sealed class FilteringGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Filtered(static _ => true));
    }
}
