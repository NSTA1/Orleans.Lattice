namespace Orleans.Lattice.Membership;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Membership</c> package. Mirrors the core
/// <c>TypeAliases</c> table: every constant uses the <c>olm.</c> prefix, is at
/// most 6 characters, and is unique - invariants enforced by
/// <c>MembershipTypeAliasesTests</c>.
/// </summary>
internal static class MembershipTypeAliases
{
    /// <summary>Alias for <see cref="LatticePrincipal"/>.</summary>
    internal const string LatticePrincipal = "olm.pr";
}
