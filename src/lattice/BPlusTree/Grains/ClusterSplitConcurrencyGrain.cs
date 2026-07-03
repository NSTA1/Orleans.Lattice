using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cluster-wide singleton admission gate for autonomic shard splits.
/// <para>
/// Caps the aggregate number of concurrently in-flight autonomic splits across
/// every tree at the configured <see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>
/// ceiling. The gate is only ever reached when an operator opts in; with the
/// option left at its <c>null</c> default no monitor consults it.
/// </para>
/// <para>
/// It is driven by per-tree heartbeats: each enabled monitor reports its
/// authoritative in-flight split count (from shard <c>IsSplitting</c>) every
/// sampling pass and is granted new slots against the remaining headroom. Each
/// footprint carries a time-to-live, so a silo that crashes and stops reporting
/// has its share reclaimed on expiry - a crashed split can never permanently
/// consume cluster budget.
/// </para>
/// Key format: singleton integer key <c>0</c>.
/// </summary>
internal sealed class ClusterSplitConcurrencyGrain(
    IGrainContext context,
    [PersistentState("cluster-split-concurrency", LatticeOptions.StorageProviderName)]
    IPersistentState<ClusterSplitConcurrencyState> state,
    ILogger<ClusterSplitConcurrencyGrain> logger) : IClusterSplitConcurrencyGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task<int> AcquireSlotsAsync(string treeId, int currentInFlight, int desiredNew, int clusterCap, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (currentInFlight < 0) currentInFlight = 0;
        if (desiredNew < 0) desiredNew = 0;

        var nowUtc = DateTime.UtcNow;
        var footprints = state.State.Footprints;

        // Reconcile: drop this tree's prior footprint (about to be re-reported)
        // and any expired footprints (crashed silos), summing the surviving
        // other-tree in-flight counts as we go.
        int? oldInFlight = null;
        var otherInFlight = 0;
        var expiredRemoved = false;
        for (int i = footprints.Count - 1; i >= 0; i--)
        {
            var fp = footprints[i];
            if (fp.TreeId == treeId)
            {
                oldInFlight = fp.InFlight;
                footprints.RemoveAt(i);
                continue;
            }
            if (fp.ExpiryUtc <= nowUtc)
            {
                footprints.RemoveAt(i);
                expiredRemoved = true;
                continue;
            }
            otherInFlight += fp.InFlight;
        }

        // Headroom is the ceiling less every other tree's in-flight splits and
        // this tree's own already-running drains. Pre-existing over-subscription
        // (drains that were admitted before the cap tightened) yields a negative
        // headroom and simply grants nothing new - it never retroactively aborts
        // a running split.
        var headroom = clusterCap - otherInFlight - currentInFlight;
        var grant = desiredNew;
        if (grant > headroom) grant = headroom;
        if (grant < 0) grant = 0;

        var footprint = currentInFlight + grant;
        if (footprint > 0)
            footprints.Add(new TreeSplitFootprint(treeId, footprint, nowUtc + ttl));

        // Persist only when the material content changed (a footprint appeared,
        // disappeared, or changed count, or a stale entry was reclaimed). A
        // steady-state heartbeat that merely refreshes an unchanged footprint's
        // expiry is kept in memory - losing that refresh across a rare gate
        // reactivation only causes a one-pass undercount that self-heals.
        var contentChanged = expiredRemoved || (oldInFlight ?? 0) != footprint;
        if (contentChanged) await state.WriteStateAsync();

        if (grant > 0)
            logger.LogDebug(
                "Cluster split gate granted {Grant}/{Desired} new split(s) to tree {TreeId} (in-flight now {Footprint}, other trees {OtherInFlight}, cap {ClusterCap})",
                grant, desiredNew, treeId, footprint, otherInFlight, clusterCap);

        return grant;
    }

    /// <inheritdoc />
    public Task<int> GetClusterInFlightAsync()
    {
        var nowUtc = DateTime.UtcNow;
        var total = 0;
        foreach (var fp in state.State.Footprints)
            if (fp.ExpiryUtc > nowUtc) total += fp.InFlight;
        return Task.FromResult(total);
    }
}
