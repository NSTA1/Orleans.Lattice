namespace Orleans.Lattice.Membership;

/// <summary>
/// Internal singleton whose sole purpose is to make a repeated
/// <see cref="LatticeMembershipServiceCollectionExtensions.AddLatticeMembership"/>
/// call a no-op for the structural wiring while still layering any supplied
/// options delegate.
/// </summary>
internal sealed class MembershipRegistrationMarker;
