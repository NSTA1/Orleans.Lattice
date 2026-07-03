namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// A pluggable sign-in mechanism for the explorer: it declares the scheme it
/// implements, decides whether it can satisfy an endpoint's advertised scheme,
/// and runs the interactive challenge that yields the credentials attached to
/// every state-API call. Each scheme (Basic, Entra, generic OIDC, or a bespoke
/// one) is a separate implementation, so new mechanisms plug in without changing
/// the explorer core.
/// </summary>
public interface IExplorerAuthMethod
{
    /// <summary>
    /// The stable scheme id this method implements (for example
    /// <see cref="ExplorerAuthSchemes.Basic"/> or
    /// <see cref="ExplorerAuthSchemes.Entra"/>). Selected against the endpoint's
    /// advertised scheme.
    /// </summary>
    string SchemeId { get; }

    /// <summary>
    /// Decides whether this method can satisfy the endpoint's
    /// <paramref name="advertisedScheme"/>. The default matching is an
    /// ordinal-ignore-case comparison against <see cref="SchemeId"/>; a method
    /// may accept aliases or a family of scheme names.
    /// </summary>
    /// <param name="advertisedScheme">The scheme the endpoint advertised.</param>
    /// <returns><see langword="true"/> when this method handles the scheme.</returns>
    bool CanHandle(string advertisedScheme);

    /// <summary>
    /// Runs the interactive challenge described by <paramref name="context"/> and
    /// returns the resulting sign-in. Implementations validate their inputs,
    /// drive the flow (a Basic header, an Entra browser redirect, a device-code
    /// prompt), and, for token schemes, wire up transparent refresh before
    /// returning.
    /// </summary>
    /// <param name="context">The challenge input (scheme, advertised parameters, user inputs, clock).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The sign-in to apply to the connection.</returns>
    Task<ExplorerAuthSignIn> ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default);
}
