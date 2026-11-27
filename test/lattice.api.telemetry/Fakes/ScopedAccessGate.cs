namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A scripted <see cref="ILatticeAccessGate"/> that answers per tree id and
/// operation, so a test can grant <see cref="LatticeOperation.Telemetry"/> on one
/// tenant's capability scope without granting it cluster-wide (issue #5582). Every
/// unscripted pair is denied, fail-closed.
/// </summary>
internal sealed class ScopedAccessGate : ILatticeAccessGate
{
    private readonly Dictionary<(string TreeId, LatticeOperation Operation), LatticeAccessDecision> _decisions = [];

    /// <summary>Every tree id and operation the gate was asked about, in call order.</summary>
    public List<(string TreeId, LatticeOperation Operation)> Requests { get; } = [];

    /// <summary>Scripts <paramref name="decision"/> for <paramref name="operation"/> on <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree id the request targets.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="decision">The decision to return.</param>
    /// <returns>This gate, for chaining.</returns>
    public ScopedAccessGate For(string treeId, LatticeOperation operation, LatticeAccessDecision decision)
    {
        _decisions[(treeId, operation)] = decision;
        return this;
    }

    /// <summary>Grants <see cref="LatticeOperation.Telemetry"/> on <paramref name="tenant"/>'s capability scope.</summary>
    /// <param name="tenant">The tenant id.</param>
    /// <returns>This gate, for chaining.</returns>
    public ScopedAccessGate AllowingTenantTelemetry(string tenant) =>
        For(CapabilityId(tenant), LatticeOperation.Telemetry, LatticeAccessDecision.Allow());

    /// <summary>The capability id of <paramref name="tenant"/>, <c>_lattice_tenant_admin_{tenant}</c>.</summary>
    /// <param name="tenant">The tenant id.</param>
    /// <returns>The id.</returns>
    public static string CapabilityId(string tenant) =>
        LatticeTenantAdminScope.ForTenant(TenantId.Parse(tenant)).TreeScope;

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request, CancellationToken cancellationToken = default)
    {
        Requests.Add((request.TreeId, request.Operation));
        return new ValueTask<LatticeAccessDecision>(
            _decisions.TryGetValue((request.TreeId, request.Operation), out var decision)
                ? decision
                : LatticeAccessDecision.Deny("no rule"));
    }
}
