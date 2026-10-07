namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free rule deciding whether a stateless routing activation
/// (<c>LatticeGrain</c>) publishes the (physical copy, map) pair it has just
/// resolved from the registry as its cached routing. Extracted so the decision
/// <c>LatticeGrain.GetRoutingSlowAsync</c> executes is the artifact the
/// shard-ownership Coyote model drives, with no possibility of drift.
/// <para>
/// Calls that touch routing interleave on one activation, so a slow resolve can
/// finish after a newer one, or after an invalidation that a stale-routing signal
/// raised. Publishing either would put back a pair the activation already knew to
/// be stale, and nothing would signal it stale again until the next refusal
/// (#4357). So a resolve is published only when no invalidation has happened
/// since it started and nothing newer is already published. Every alias swap
/// re-versions the map above the row's previous one, so a lower map version is an
/// older registry row.
/// </para>
/// <para>
/// The caller still uses the pair it read for its own call, which is
/// self-consistent either way: alias and map come from one registry row.
/// </para>
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no wall-clock, and no Orleans types,
/// and allocates nothing.
/// </para>
/// </summary>
internal static class RoutingPairPublishGate
{
    /// <summary>
    /// Whether a resolved routing pair becomes the activation's cached routing.
    /// </summary>
    /// <param name="epochAtResolveStart">The activation's routing epoch when the resolve began.</param>
    /// <param name="currentEpoch">The activation's routing epoch now; every invalidation increments it.</param>
    /// <param name="publishedMapVersion">The map version of the pair already published, or <see langword="null"/> when none is.</param>
    /// <param name="resolvedMapVersion">The map version of the pair just resolved.</param>
    public static bool ShouldPublish(
        long epochAtResolveStart,
        long currentEpoch,
        long? publishedMapVersion,
        long resolvedMapVersion) =>
        epochAtResolveStart == currentEpoch
        && (publishedMapVersion is null || publishedMapVersion.Value <= resolvedMapVersion);
}
