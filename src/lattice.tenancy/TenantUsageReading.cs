namespace Orleans.Lattice.Tenancy;

/// <summary>
/// One tenant's usage-against-quota reading: the
/// <see cref="TenantObservabilitySnapshot"/> projection joined with the
/// <see cref="TenantEnforcementScope"/> the reading was taken under. The scope is
/// load-bearing, not decoration: it says whether <see cref="TenantObservabilitySnapshot.Usage"/>
/// is the converged cross-cluster fold
/// (<see cref="TenantEnforcementScope.GlobalConverged"/>) or only this cluster's
/// own slot (<see cref="TenantEnforcementScope.PerCluster"/>), so a figure is
/// never reported without the qualifier that makes it honest.
/// </summary>
/// <remarks>
/// A transient, derived read result. It is never persisted and never crosses a
/// grain boundary as a payload, so - like the
/// <see cref="TenantObservabilitySnapshot"/> it wraps - it carries no Orleans
/// serialization attributes. A <see langword="readonly"/> record struct so a
/// reading is copied by value and a usage read allocates nothing for the payload
/// itself.
/// </remarks>
public readonly record struct TenantUsageReading
{
    /// <summary>Initializes a new <see cref="TenantUsageReading"/>.</summary>
    /// <param name="snapshot">The tenant's observability projection, whose usage is the aggregate selected by <paramref name="scope"/>.</param>
    /// <param name="scope">The enforcement scope the reading was taken under.</param>
    public TenantUsageReading(TenantObservabilitySnapshot snapshot, TenantEnforcementScope scope)
    {
        Snapshot = snapshot;
        Scope = scope;
    }

    /// <summary>
    /// The tenant's observability projection - identity, usage, quotas, and
    /// converged durable metered overage - with the live burst/overage signal
    /// derived on demand from <see cref="TenantObservabilitySnapshot.InstantaneousOverage"/>.
    /// </summary>
    public TenantObservabilitySnapshot Snapshot { get; init; }

    /// <summary>
    /// The enforcement scope the reading was taken under, qualifying whether
    /// <see cref="TenantObservabilitySnapshot.Usage"/> is a converged
    /// cross-cluster sum or this cluster's local view.
    /// </summary>
    public TenantEnforcementScope Scope { get; init; }
}
