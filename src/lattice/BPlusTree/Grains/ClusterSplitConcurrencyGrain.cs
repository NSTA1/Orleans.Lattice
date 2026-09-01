using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cluster-wide singleton admission gate for autonomic shard splits.
/// <para>
/// Caps the aggregate number of concurrently in-flight autonomic splits across
/// every tree at the configured <see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>
/// ceiling. The ceiling is only ever applied when an operator opts in; with the
/// option left at its <c>null</c> default no monitor requests a slot, so nothing
/// is ever denied.
/// </para>
/// <para>
/// It is driven by per-tree heartbeats: each enabled monitor reports its
/// authoritative in-flight split count (from shard <c>IsSplitting</c>) every
/// sampling pass and is granted new slots against the remaining headroom. Each
/// footprint carries a time-to-live, so a silo that crashes and stops reporting
/// has its share reclaimed on expiry - a crashed split can never permanently
/// consume cluster budget.
/// </para>
/// <para>
/// The same footprints double as the cluster's readable split-activity source.
/// Monitors publish through <see cref="ReportInFlightAsync"/> even with no
/// ceiling configured (edge-triggered, so an idle tree calls nothing), and
/// <see cref="GetActivityAsync"/> reduces them into the snapshot that
/// <c>ILatticeAdmin.GetSplitActivityAsync</c> serves. Those observation-only
/// footprints live in their own list and never consume admission headroom.
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

        var grant = await RecordFootprintAsync(treeId, currentInFlight, desiredNew, clusterCap, ttl);

        if (grant > 0)
            logger.LogDebug(
                "Cluster split gate granted {Grant}/{Desired} new split(s) to tree {TreeId} (in-flight now {Footprint}, cap {ClusterCap})",
                grant, desiredNew, treeId, currentInFlight + grant, clusterCap);

        return grant;
    }

    /// <inheritdoc />
    public Task ReportInFlightAsync(string treeId, int inFlight, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (inFlight < 0) inFlight = 0;

        // desiredNew: 0 makes this a pure heartbeat - the footprint is refreshed
        // (or cleared, when inFlight is zero) and nothing can be granted, so the
        // ceiling argument is irrelevant on this path.
        return RecordFootprintAsync(treeId, inFlight, desiredNew: 0, clusterCap: 0, ttl, observationOnly: true);
    }

    /// <summary>
    /// Reconciles the footprint lists (dropping the caller's prior entry from
    /// both and any expired ones), records the caller's refreshed footprint in
    /// the list matching <paramref name="observationOnly"/>, and returns how many
    /// of <paramref name="desiredNew"/> fit under the remaining headroom.
    /// <para>
    /// Headroom is computed from the admission list alone. Observation-only
    /// footprints come from trees that never opted into the cluster ceiling, so
    /// counting them would let an uncapped tree throttle a capped one; they are
    /// tracked purely so the readable split-activity queries see the whole
    /// cluster. The caller's prior entry is removed from *both* lists so a tree
    /// that switches modes (an operator setting or clearing the ceiling) cannot
    /// leave a duplicate behind.
    /// </para>
    /// </summary>
    private async Task<int> RecordFootprintAsync(
        string treeId,
        int currentInFlight,
        int desiredNew,
        int clusterCap,
        TimeSpan ttl,
        bool observationOnly = false)
    {
        var nowUtc = DateTime.UtcNow;
        var admission = state.State.Footprints;
        var observed = state.State.ObservedFootprints;

        // Reconcile: drop this tree's prior footprint (about to be re-reported)
        // and any expired footprints (crashed silos), summing the surviving
        // other-tree in-flight counts from the admission list as we go.
        var oldInFlight = 0;
        var hadOld = false;
        var otherInFlight = 0;
        var changed = false;

        for (int i = admission.Count - 1; i >= 0; i--)
        {
            var fp = admission[i];
            if (fp.TreeId == treeId)
            {
                oldInFlight = fp.InFlight;
                hadOld = true;
                admission.RemoveAt(i);
                continue;
            }
            if (fp.ExpiryUtc <= nowUtc)
            {
                admission.RemoveAt(i);
                changed = true;
                continue;
            }
            otherInFlight += fp.InFlight;
        }

        for (int i = observed.Count - 1; i >= 0; i--)
        {
            var fp = observed[i];
            if (fp.TreeId == treeId)
            {
                // A mode switch leaves the tree's prior entry in the other list;
                // fold it into the old-count comparison so the persist decision
                // still sees a genuine content change.
                oldInFlight += fp.InFlight;
                hadOld = true;
                observed.RemoveAt(i);
                continue;
            }
            if (fp.ExpiryUtc <= nowUtc)
            {
                observed.RemoveAt(i);
                changed = true;
            }
        }

        // Headroom is the ceiling less every other *capped* tree's in-flight
        // splits and this tree's own already-running drains. Pre-existing
        // over-subscription (drains that were admitted before the cap tightened)
        // yields a negative headroom and simply grants nothing new - it never
        // retroactively aborts a running split.
        var headroom = clusterCap - otherInFlight - currentInFlight;
        var grant = desiredNew;
        if (grant > headroom) grant = headroom;
        if (grant < 0) grant = 0;

        var footprint = currentInFlight + grant;
        if (footprint > 0)
        {
            var entry = new TreeSplitFootprint(treeId, footprint, nowUtc + ttl);
            (observationOnly ? observed : admission).Add(entry);
        }

        // Persist only when the material content changed (a footprint appeared,
        // disappeared, or changed count, or a stale entry was reclaimed). A
        // steady-state heartbeat that merely refreshes an unchanged footprint's
        // expiry is kept in memory - losing that refresh across a rare gate
        // reactivation only causes a one-pass undercount that self-heals.
        var contentChanged = changed || (hadOld ? oldInFlight : 0) != footprint;
        if (contentChanged) await state.WriteStateAsync();

        return grant;
    }

    /// <inheritdoc />
    public Task<int> GetClusterInFlightAsync()
    {
        var nowUtc = DateTime.UtcNow;
        var total = 0;
        foreach (var fp in state.State.Footprints)
            if (fp.ExpiryUtc > nowUtc) total += fp.InFlight;
        foreach (var fp in state.State.ObservedFootprints)
            if (fp.ExpiryUtc > nowUtc) total += fp.InFlight;
        return Task.FromResult(total);
    }

    /// <inheritdoc />
    public Task<SplitActivityReport> GetActivityAsync()
    {
        var nowUtc = DateTime.UtcNow;
        var total = 0;
        var trees = 0;
        Accumulate(state.State.Footprints);
        Accumulate(state.State.ObservedFootprints);

        return Task.FromResult(new SplitActivityReport
        {
            InFlight = total,
            ReportingTrees = trees,
            ObservedAt = new DateTimeOffset(nowUtc, TimeSpan.Zero),
        });

        void Accumulate(List<TreeSplitFootprint> footprints)
        {
            foreach (var fp in footprints)
            {
                if (fp.ExpiryUtc <= nowUtc || fp.InFlight <= 0) continue;
                total += fp.InFlight;
                trees++;
            }
        }
    }
}
