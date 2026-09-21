namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// How much state the driver-owned estate actually holds.
/// <para>
/// Reported alongside every measurement, because a timeout count on its own is
/// uninterpretable. The live estate that produced the observed storm carries
/// 18,124 leaf snapshots averaging 200.9 KB - 99.3% of a 4.47 GiB grain store -
/// so the same timeout count means opposite things depending on whether the
/// estate under test held comparable state or almost none. Two candidate
/// mechanisms both fit the observed signature, and they scale with DIFFERENT
/// quantities:
/// </para>
/// <list type="bullet">
/// <item>
/// fan-in, where calls queue behind the registry activation itself, scales with
/// the number of trees and their per-tree background services;
/// </item>
/// <item>
/// storage starvation, where the registry's reads starve behind the cold-start
/// replay of thousands of leaf snapshots out of one SQLite file, scales with
/// LEAF COUNT and store size and not with tree count at all.
/// </item>
/// </list>
/// <para>
/// A rig that reported only tree count could not tell them apart, and on an
/// empty estate would show no storm at any tree count and report a clean scaling
/// law that was purely an artefact of having no data.
/// </para>
/// </summary>
/// <param name="Trees">How many trees were counted.</param>
/// <param name="TotalEntries">The total entry count across them.</param>
/// <param name="KeysPerLeaf">The leaf capacity used to imply a leaf count.</param>
/// <param name="ImpliedLeaves">
/// The implied total leaf count. It is a LOWER BOUND, not an exact figure: it
/// assumes every leaf is full, whereas a leaf that split is about half full, so
/// the true count is higher. Reported as implied rather than measured because
/// the driver sees entries and only the offline store census sees leaves.
/// </param>
/// <param name="FailedTrees">Trees whose count call did not return.</param>
/// <param name="MinEntries">The smallest per-tree entry count.</param>
/// <param name="MaxEntries">The largest per-tree entry count.</param>
internal readonly record struct EstateCensus(
    int Trees,
    long TotalEntries,
    int KeysPerLeaf,
    long ImpliedLeaves,
    int FailedTrees,
    int MinEntries,
    int MaxEntries)
{
    /// <summary>
    /// Builds a census from per-tree entry counts.
    /// </summary>
    /// <param name="perTree">Entry count per tree; a negative value marks a failed count.</param>
    /// <param name="keysPerLeaf">The tree's leaf capacity.</param>
    /// <returns>The census.</returns>
    public static EstateCensus From(IReadOnlyDictionary<string, int> perTree, int keysPerLeaf)
    {
        ArgumentNullException.ThrowIfNull(perTree);

        var capacity = Math.Max(1, keysPerLeaf);
        var counted = perTree.Values.Where(v => v >= 0).ToArray();
        var total = counted.Sum(v => (long)v);

        return new EstateCensus(
            perTree.Count,
            total,
            capacity,
            // Integer ceiling rather than truncation: a partial leaf is still a
            // leaf that has to be faulted in at cold start, and truncating would
            // under-report exactly the quantity the depth axis controls.
            (total + capacity - 1) / capacity,
            perTree.Values.Count(v => v < 0),
            counted.Length == 0 ? 0 : counted.Min(),
            counted.Length == 0 ? 0 : counted.Max());
    }
}
