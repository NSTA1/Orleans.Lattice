namespace Orleans.Lattice.Replication;

/// <summary>
/// A snapshot export the source refused for now (issue #4684): a precondition
/// on serving it is not met yet, and the receiver's bootstrap retries it as a
/// transient fault. Never crosses a grain boundary.
/// </summary>
internal sealed class LatticeSnapshotExportDeferredException(string message) : Exception(message);
