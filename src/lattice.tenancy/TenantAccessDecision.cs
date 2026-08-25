namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The decision the <see cref="ITenantPolicyEngine"/> returns for a tenant-policy
/// query: allow, or deny with a human-readable reason. Mirrors the ergonomics of
/// the authorization <c>LatticeAccessDecision</c> - a plain allow carries no
/// reason, while a denial always carries the cause.
/// </summary>
/// <remarks>
/// This is an in-process decision value. It is deliberately a plain
/// <c>readonly struct</c> that never crosses a grain boundary and carries no
/// Orleans serialization attributes. The <see cref="Allow()"/> factory returns a
/// cached singleton value, so the warm allow path produces a decision without
/// allocating.
/// </remarks>
public readonly struct TenantAccessDecision
{
    private static readonly TenantAccessDecision AllowDecision = new(true, reason: null);

    private TenantAccessDecision(bool allowed, string? reason)
    {
        Allowed = allowed;
        Reason = reason;
    }

    /// <summary>
    /// <c>true</c> when the query is allowed; <c>false</c> when it is denied.
    /// </summary>
    public bool Allowed { get; }

    /// <summary>
    /// A human-readable reason for the decision. Set for a denial (the cause);
    /// <c>null</c> for a plain allow.
    /// </summary>
    public string? Reason { get; }

    /// <summary>
    /// The cached "allow" decision. Allocation-free.
    /// </summary>
    /// <returns>A decision whose <see cref="Allowed"/> is <c>true</c>.</returns>
    public static TenantAccessDecision Allow() => AllowDecision;

    /// <summary>
    /// Creates a denial decision carrying the supplied reason.
    /// </summary>
    /// <param name="reason">The reason the query is denied. Must not be <c>null</c> or empty.</param>
    /// <returns>A decision whose <see cref="Allowed"/> is <c>false</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is <c>null</c> or empty.</exception>
    public static TenantAccessDecision Deny(string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        return new TenantAccessDecision(false, reason);
    }
}
