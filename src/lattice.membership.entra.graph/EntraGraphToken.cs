namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// An acquired app-only access token and its absolute expiry. Produced by an
/// <see cref="IEntraGraphTokenAcquirer"/> and cached by
/// <see cref="EntraGraphTokenProvider"/>. In-process only; never serialized.
/// </summary>
/// <param name="AccessToken">The bearer access token.</param>
/// <param name="ExpiresOn">The absolute time the token expires.</param>
internal readonly record struct EntraGraphToken(string AccessToken, DateTimeOffset ExpiresOn);
