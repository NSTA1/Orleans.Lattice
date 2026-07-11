namespace Orleans.Lattice.Schema;

/// <summary>
/// Idempotency sentinel: its presence in the container signals that
/// <see cref="LatticeSchemaEnforcementServiceCollectionExtensions.AddLatticeSchemaEnforcement"/>
/// has already performed the one-time structural wiring, so a repeat call layers
/// only additional options configuration.
/// </summary>
internal sealed class SchemaEnforcementRegistrationMarker;
