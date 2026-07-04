namespace Orleans.Lattice;

/// <summary>
/// Sentinel service registered by the authorization layer (<c>AddLatticeAuth</c>)
/// alongside the capability-stripping incoming call filter. Its presence in a
/// grain's activation services is the positive signal that the filter is active,
/// and therefore that the internal-origin marker is being re-derived on every
/// silo hop - the precondition the shard and leaf internal-origin assertion needs
/// to enforce safely (issue #1103).
/// </summary>
/// <remarks>
/// The assertion is keyed on this marker rather than merely on "a non-null access
/// gate is registered" so that the guard activates <em>exactly</em> when the
/// filter that establishes the marker is present. A cluster that registers a
/// custom <see cref="ILatticeAccessGate"/> without the filter (and hence without
/// this marker) never sets the internal-origin marker, so keying the guard on the
/// marker keeps it from rejecting that cluster's own legitimate facade-to-shard
/// hops. A no-auth cluster registers neither, so the guard stays inert and pays
/// nothing.
/// </remarks>
internal sealed class LatticeInternalOriginEnforcementMarker
{
}
