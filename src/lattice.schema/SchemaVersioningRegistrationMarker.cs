namespace Orleans.Lattice.Schema;

/// <summary>
/// Idempotency marker for <c>AddLatticeSchemaVersioning</c>: its presence in the
/// service collection signals the one-time structural wiring already ran, so a
/// repeat call layers only the supplied configuration delegates. Mirrors
/// <c>SchemaEnforcementRegistrationMarker</c>.
/// </summary>
internal sealed class SchemaVersioningRegistrationMarker;
