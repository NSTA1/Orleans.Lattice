using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// Creates, exercises and tears down the driver-owned fleet of trees.
/// <para>
/// Every tree this type creates carries the driver's own prefix, and teardown
/// only ever addresses ids carrying that prefix. That is not tidiness: the rig
/// runs against a throwaway container precisely so a teardown bug cannot reach a
/// real estate, and the prefix is the second, in-process guard behind that.
/// </para>
/// </summary>
/// <param name="grains">The Orleans grain factory for the joined cluster.</param>
/// <param name="prefix">The tree-id prefix this driver owns.</param>
internal sealed class TreeFleet(IGrainFactory grains, string prefix)
{
    /// <summary>The tree-id prefix this fleet owns.</summary>
    public string Prefix { get; } = prefix;

    /// <summary>Builds the id of the <paramref name="index"/>th tree in the fleet.</summary>
    /// <param name="index">The zero-based tree index.</param>
    /// <returns>The tree id.</returns>
    public string TreeId(int index) => $"{Prefix}{index:D4}";

    /// <summary>The ids of the first <paramref name="count"/> trees in the fleet.</summary>
    /// <param name="count">How many ids to produce.</param>
    /// <returns>The ids.</returns>
    public IReadOnlyList<string> TreeIds(int count) =>
        Enumerable.Range(0, count).Select(TreeId).ToArray();

    /// <summary>
    /// Creates <paramref name="count"/> trees. Creating a tree means REGISTER
    /// plus a first write: registration alone leaves an entry that no per-tree
    /// background service ever attaches to, so a registration-only fleet would
    /// not reproduce the fan-in the rig exists to measure.
    /// </summary>
    /// <param name="count">How many trees to create.</param>
    /// <param name="census">The census to record each call against.</param>
    /// <param name="parallelism">How many creations to run at once.</param>
    /// <param name="cancellationToken">Cancels the creation sweep.</param>
    /// <returns>The created tree ids.</returns>
    public async Task<IReadOnlyList<string>> CreateAsync(
        int count,
        CallCensus census,
        int parallelism,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(census);

        var ids = TreeIds(count);
        await Parallel.ForEachAsync(
            ids,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Max(1, parallelism),
                CancellationToken = cancellationToken,
            },
            async (treeId, ct) =>
            {
                var registry = grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                await census.MeasureAsync(
                    "ILatticeRegistry.RegisterAsync",
                    () => registry.RegisterAsync(treeId)).ConfigureAwait(false);

                var lattice = grains.GetGrain<ILattice>(treeId);
                await census.MeasureAsync(
                    "ILattice.SetAsync",
                    () => lattice.SetAsync("seed/0000", Payload(treeId, 0), ct)).ConfigureAwait(false);
            }).ConfigureAwait(false);

        return ids;
    }

    /// <summary>
    /// Deletes and unregisters every tree in the fleet, in that order.
    /// <para>
    /// <see cref="ILattice.DeleteTreeAsync"/> first, then
    /// <see cref="ILatticeRegistry.UnregisterAsync"/>: unregistering first would
    /// strand the tree's data behind a registry entry that no longer resolves.
    /// Teardown is best-effort per tree - a tree whose deletion faults is
    /// reported and the sweep continues, because abandoning the sweep on the
    /// first fault leaves MORE residue, not less.
    /// </para>
    /// </summary>
    /// <param name="count">How many trees to tear down.</param>
    /// <param name="census">The census to record each call against.</param>
    /// <param name="parallelism">How many teardowns to run at once.</param>
    /// <param name="cancellationToken">Cancels the teardown sweep.</param>
    /// <returns>A per-tree note for every tree that did not tear down cleanly.</returns>
    public async Task<IReadOnlyList<string>> TeardownAsync(
        int count,
        CallCensus census,
        int parallelism,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(census);

        var residue = new List<string>();
        var gate = new Lock();

        await Parallel.ForEachAsync(
            TreeIds(count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Max(1, parallelism),
                CancellationToken = cancellationToken,
            },
            async (treeId, ct) =>
            {
                var lattice = grains.GetGrain<ILattice>(treeId);
                var deleted = await census.MeasureAsync(
                    "ILattice.DeleteTreeAsync",
                    () => lattice.DeleteTreeAsync(ct)).ConfigureAwait(false);

                var registry = grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                var unregistered = await census.MeasureAsync(
                    "ILatticeRegistry.UnregisterAsync",
                    () => registry.UnregisterAsync(treeId)).ConfigureAwait(false);

                if (deleted.Outcome == CallOutcome.Ok && unregistered.Outcome == CallOutcome.Ok)
                {
                    return;
                }

                lock (gate)
                {
                    residue.Add(
                        $"{treeId}: delete={deleted.Outcome}/{deleted.FaultType ?? "-"} " +
                        $"unregister={unregistered.Outcome}/{unregistered.FaultType ?? "-"}");
                }
            }).ConfigureAwait(false);

        return residue;
    }

    /// <summary>Builds a deterministic payload for a tree and sequence number.</summary>
    /// <param name="treeId">The tree the payload belongs to.</param>
    /// <param name="sequence">The write sequence number.</param>
    /// <returns>The payload bytes.</returns>
    public static byte[] Payload(string treeId, long sequence) =>
        System.Text.Encoding.UTF8.GetBytes($"{treeId}#{sequence}");
}
