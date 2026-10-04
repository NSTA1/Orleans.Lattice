namespace Orleans.Lattice.Backup;

/// <summary>
/// The dependency-free decisions a cross-tree-consistent backup set's capture
/// window takes from its registry observations: whether the drain gate may pass,
/// and whether a completed capture attempt may be accepted.
/// <see cref="LatticeBackupCaptureService"/> routes its drain gate and its
/// post-capture re-observation through it, and the backup Coyote model drives the
/// same methods, so the window the specification checks
/// (<c>spec/backup/BackupCapture.tla</c>) is the one that runs.
/// </summary>
internal static class CrossTreeFenceWindow
{
    /// <summary>
    /// The drain gate: the window may open only at a moment when no set tree
    /// holds a cross-tree delegation row.
    /// </summary>
    /// <param name="totalInFlight">The summed in-flight count across every set tree.</param>
    /// <returns><c>true</c> when the drain gate passes.</returns>
    public static bool IsDrained(int totalInFlight) => totalInFlight == 0;

    /// <summary>
    /// The post-capture re-observation of one set tree: the attempt stays
    /// acceptable only if the tree's registration epoch has not moved since the
    /// drained moment (no cross-tree saga registered during the window, even one
    /// that also completed inside it) and nothing is in flight now.
    /// </summary>
    /// <param name="epochAtDrain">The tree's epoch observed at the drained moment.</param>
    /// <param name="epochNow">The tree's epoch at the re-observation.</param>
    /// <param name="inFlightNow">The tree's in-flight count at the re-observation.</param>
    /// <returns><c>true</c> when the tree's part of the window was stable.</returns>
    public static bool IsStable(long epochAtDrain, long epochNow, int inFlightNow) =>
        epochNow == epochAtDrain && inFlightNow == 0;
}
