namespace Orleans.Lattice.Auth;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Auth</c> package. Mirrors the core <c>TypeAliases</c>
/// table and the sibling <c>MembershipTypeAliases</c>: every constant uses the
/// <c>olz.</c> prefix, is at most 6 characters, and is unique - invariants
/// enforced by <c>AuthTypeAliasesTests</c>.
/// </summary>
internal static class AuthTypeAliases
{
    /// <summary>Alias for <see cref="LatticeAuthorizationRule"/>.</summary>
    internal const string LatticeAuthorizationRule = "olz.ar";

    /// <summary>Alias for <see cref="LatticeSubjectSelector"/>.</summary>
    internal const string LatticeSubjectSelector = "olz.ss";

    /// <summary>Alias for <see cref="LatticeScope"/>.</summary>
    internal const string LatticeScope = "olz.sc";
}
