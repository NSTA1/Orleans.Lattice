namespace Orleans.Lattice.Backup;

/// <summary>
/// Marker singleton signalling that <c>AddLatticeBackup</c> has already performed
/// its one-time structural wiring, so a repeat call layers only an additional
/// options delegate rather than re-registering services. Mirrors the sibling
/// membership and authorization registration markers.
/// </summary>
internal sealed class BackupRegistrationMarker;
