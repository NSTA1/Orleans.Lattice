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

    /// <summary>
    /// Populates <paramref name="count"/> trees to a target leaf depth.
    /// <para>
    /// Leaf count is a CONTROLLED AXIS of this rig, not an incidental
    /// consequence of creating trees, and that distinction decides whether the
    /// measurement means anything. The live estate carries 18,124 leaf
    /// snapshots averaging 200.9 KB - 99.3% of a 4.47 GiB grain store - so a
    /// fleet of freshly-registered trees holding one seed key each has roughly
    /// two orders of magnitude less state to fault in at cold start. If the
    /// storm is driven by storage starvation during snapshot replay rather than
    /// by registry fan-in, such a fleet would show no storm at ANY tree count
    /// and the rig would report a clean scaling law that is purely an artefact
    /// of having no data. That is a false green, and worse than no measurement.
    /// </para>
    /// <para>
    /// A leaf holds up to <c>LatticeConstants.DefaultMaxLeafKeys</c> (128) keys,
    /// so a target of L leaves is written as L x 128 entries. Sizing the value
    /// at ~1.6 KB reproduces the live estate's ~200 KB leaf snapshot, which is
    /// what the replay actually reads.
    /// </para>
    /// </summary>
    /// <param name="count">How many trees to populate.</param>
    /// <param name="leavesPerTree">The target leaf count per tree.</param>
    /// <param name="keysPerLeaf">Keys per leaf; the tree's leaf capacity.</param>
    /// <param name="valueBytes">The size of each value.</param>
    /// <param name="batchSize">How many entries per write call.</param>
    /// <param name="census">The census to record each call against.</param>
    /// <param name="parallelism">How many trees to populate at once.</param>
    /// <param name="cancellationToken">Cancels the sweep.</param>
    /// <returns>The number of entries written.</returns>
    public async Task<long> PopulateAsync(
        int count,
        int leavesPerTree,
        int keysPerLeaf,
        int valueBytes,
        int batchSize,
        CallCensus census,
        int parallelism,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(census);

        var written = 0L;

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
                var target = (long)leavesPerTree * keysPerLeaf;

                // SetManyAsync rather than BulkLoadAsync deliberately.
                // BulkLoadAsync is faster, but it requires every shard to be
                // empty and throws on the second call, so it cannot TOP UP a
                // fleet - and topping up is what a depth sweep does when it
                // moves from one leaf target to the next without recreating the
                // estate. Choosing the faster primitive would mean every depth
                // point had to rebuild from nothing, which costs far more than
                // the per-call difference saves.
                var batch = new List<KeyValuePair<string, byte[]>>(batchSize);
                for (var i = 0L; i < target; i++)
                {
                    batch.Add(new KeyValuePair<string, byte[]>(
                        SizedKey(i),
                        SizedPayload(treeId, i, valueBytes)));

                    if (batch.Count < batchSize && i < target - 1)
                    {
                        continue;
                    }

                    var toWrite = batch.ToList();
                    var result = await census.MeasureAsync(
                        "ILattice.SetManyAsync",
                        () => lattice.SetManyAsync(toWrite, ct)).ConfigureAwait(false);

                    if (result.Outcome == CallOutcome.Ok)
                    {
                        Interlocked.Add(ref written, toWrite.Count);
                    }

                    batch.Clear();
                }
            }).ConfigureAwait(false);

        return Interlocked.Read(ref written);
    }

    /// <summary>
    /// Counts the entries in each of the first <paramref name="count"/> trees.
    /// <para>
    /// Reported alongside every measurement because a storm figure without the
    /// estate's depth is uninterpretable: the same timeout count means opposite
    /// things on an empty estate and on a loaded one, and reading one without
    /// the other is how the original live observation was nearly misread.
    /// </para>
    /// </summary>
    /// <param name="count">How many trees to count.</param>
    /// <param name="keysPerLeaf">Keys per leaf, for the implied leaf count.</param>
    /// <param name="census">The census to record each call against.</param>
    /// <param name="parallelism">How many counts to run at once.</param>
    /// <param name="cancellationToken">Cancels the sweep.</param>
    /// <returns>The estate census.</returns>
    public async Task<EstateCensus> CountAsync(
        int count,
        int keysPerLeaf,
        CallCensus census,
        int parallelism,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(census);

        var perTree = new Dictionary<string, int>(StringComparer.Ordinal);
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
                var result = await census.MeasureAsync(
                    "ILattice.CountAsync",
                    () => lattice.CountAsync(ct)).ConfigureAwait(false);

                lock (gate)
                {
                    perTree[treeId] = result.Outcome == CallOutcome.Ok ? result.Value : -1;
                }
            }).ConfigureAwait(false);

        return EstateCensus.From(perTree, keysPerLeaf);
    }

    /// <summary>Builds a fixed-width, lexicographically ordered key.</summary>
    /// <param name="sequence">The entry sequence number.</param>
    /// <returns>The key.</returns>
    public static string SizedKey(long sequence) => $"k/{sequence:D12}";

    /// <summary>
    /// Builds a payload of exactly <paramref name="valueBytes"/> bytes.
    /// <para>
    /// The filler is pseudo-random rather than a repeated character, and that
    /// matters for this particular measurement: SQLite pages and the snapshot
    /// path both handle highly-compressible bytes differently from
    /// incompressible ones, so a run of zeroes would understate the stored size
    /// per leaf - which is the very quantity the depth axis is trying to
    /// control. The generator is seeded from the tree and sequence so the
    /// payload stays deterministic and a re-run writes identical bytes.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree the payload belongs to.</param>
    /// <param name="sequence">The write sequence number.</param>
    /// <param name="valueBytes">The exact payload size.</param>
    /// <returns>The payload bytes.</returns>
    public static byte[] SizedPayload(string treeId, long sequence, int valueBytes)
    {
        var size = Math.Max(1, valueBytes);
        var buffer = new byte[size];
        var seed = HashCode.Combine(treeId, sequence);
        var random = new Random(seed);
        random.NextBytes(buffer);

        // Stamp the identity over the head so a value read back can be
        // attributed without decoding the whole payload.
        var header = System.Text.Encoding.UTF8.GetBytes($"{treeId}#{sequence}|");
        var headerLength = Math.Min(header.Length, size);
        Array.Copy(header, buffer, headerLength);
        return buffer;
    }

    /// <summary>Builds a deterministic payload for a tree and sequence number.</summary>
    /// <param name="treeId">The tree the payload belongs to.</param>
    /// <param name="sequence">The write sequence number.</param>
    /// <returns>The payload bytes.</returns>
    public static byte[] Payload(string treeId, long sequence) =>
        System.Text.Encoding.UTF8.GetBytes($"{treeId}#{sequence}");
}
