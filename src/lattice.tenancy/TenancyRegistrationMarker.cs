namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Marker singleton whose presence in the container signals that
/// <c>AddLatticeTenancy(...)</c> has already performed its one-time structural
/// wiring, so a repeat call layers only a supplied configure delegate. Mirrors
/// the registration-marker pattern used by the other Lattice add-ons.
/// </summary>
internal sealed class TenancyRegistrationMarker
{
}
