namespace Orleans.Lattice.Backup;

/// <summary>
/// The dependency-free decisions a cross-tree-consistent backup set's capture
/// window takes from its registry observations: whether the drain gate may pass,
/// whether the re-check under the decision gate is clean, and whether a completed
/// capture attempt may be accepted. <see cref="LatticeBackupCaptureService"/>
/// routes its drain gate, its gated re-check and its post-capture re-observation
/// through it, and the backup Coyote model drives the
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
    /// The re-check of one set tree under the decision gate (issue #4485): the
    /// attempt may capture only if the tree holds no live cross-tree delegation
    /// row and none it cannot resolve. A row still live under the gate is a saga
    /// that may be finalized on one member and not another, and an unresolvable
    /// one is a saga whose outcome the capture cannot know.
    /// </summary>
    /// <param name="inFlightUnderGate">The tree's in-flight count observed under the gate.</param>
    /// <param name="unresolvableUnderGate">The tree's count of delegations it could not resolve under the gate.</param>
    /// <returns><c>true</c> when the tree's re-check is clean.</returns>
    public static bool IsRecheckClean(int inFlightUnderGate, int unresolvableUnderGate) =>
        inFlightUnderGate == 0 && unresolvableUnderGate == 0;

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
