namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// Names the single internal lattice tree that holds every grain index's
/// bookkeeping.
/// <para>
/// The registry is a tree rather than a grain because a tree is cheap to
/// enumerate - listing every index is one scan - and because it consolidates all
/// per-index bookkeeping in one store: the persisted definition and its
/// fingerprint, the activation-path markers that record which grains an index
/// has already seen, and the backfill resume checkpoints. There is deliberately
/// no registry grain; the startup reconciler reads and writes this tree
/// directly.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// The type is internal, and so is the tree name it holds, because the registry
/// is an implementation detail: no public type exposes it, so a host cannot
/// address it as one of its own trees. The name still sits under
/// <see cref="GrainIndexTreeNames.ReservedPrefix"/> so that
/// <see cref="GrainIndexTreeNames.IsIndexOwned(string)"/> screens it out
/// alongside the per-index trees - a custom replication resolver that skips the
/// reserved namespace skips the registry too.
/// </para>
/// <para>
/// The leading dot in the reserved segment keeps the registry out of the
/// per-index tree namespace: an index named <c>x</c> is backed by
/// <c>__grainindex/x</c>, so only an index literally named <c>.registry</c>
/// could collide, and the reconciler refuses to start in that case rather than
/// letting an index scribble over the registry.
/// </para>
/// </remarks>
internal static class GrainIndexRegistryTrees
{
    /// <summary>
    /// The reserved segment, inside
    /// <see cref="GrainIndexTreeNames.ReservedPrefix"/>, that names the registry
    /// tree. Leading with a dot marks it as bookkeeping rather than an index.
    /// </summary>
    internal const string RegistrySegment = ".registry";

    /// <summary>
    /// The lattice tree every grain index's bookkeeping is stored in.
    /// </summary>
    internal const string RegistryTree = GrainIndexTreeNames.ReservedPrefix + RegistrySegment;
}
