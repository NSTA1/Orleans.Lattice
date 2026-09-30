namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// Who a circuit memo was read for: the sign-in, the endpoint and the asserted
/// tenant, plus the sign-in generation. Every per-circuit memo of a cluster answer
/// is filed under one and served only while <see cref="ShellCaller.Current"/>
/// still equals it, so an answer read for one caller is never served to the next.
/// </summary>
/// <remarks>
/// Compared by value, and allocation-free to read. The generation moves on every
/// sign-in, sign-out and connection change, so two identities that happen to
/// share a display name still never share a memo.
/// </remarks>
/// <param name="Authenticated">Whether the caller is signed in.</param>
/// <param name="Scheme">The sign-in scheme, or <see langword="null"/> when anonymous.</param>
/// <param name="User">The signed-in user's name, or <see langword="null"/>.</param>
/// <param name="Endpoint">The configured cluster endpoint, or <see langword="null"/> when none.</param>
/// <param name="Tenant">The tenant the circuit's calls assert, or <see langword="null"/> when none.</param>
/// <param name="Generation">How many sign-in or connection changes the circuit has seen.</param>
internal readonly record struct ShellCallerKey(
    bool Authenticated,
    string? Scheme,
    string? User,
    string? Endpoint,
    string? Tenant,
    long Generation)
{
    /// <summary>
    /// This key without its tenant: the identity at an endpoint. For state that
    /// is already filed per tenant and must survive a tenant switch, yet never a
    /// change of identity.
    /// </summary>
    public ShellCallerKey Identity => this with { Tenant = null };
}
