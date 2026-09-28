using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// A registered auth method with a chosen scheme id and scheme matcher, standing
/// in for Entra, an OIDC method from #1802, or a bespoke one.
/// </summary>
/// <param name="schemeId">The method's scheme id.</param>
/// <param name="canHandle">Which advertised schemes it services; defaults to an ordinal, case-insensitive match on <paramref name="schemeId"/>.</param>
internal sealed class FakeAuthMethod(string schemeId, Func<string, bool>? canHandle = null) : IExplorerAuthMethod
{
    /// <inheritdoc />
    public string SchemeId { get; } = schemeId;

    /// <inheritdoc />
    public bool CanHandle(string advertisedScheme) =>
        canHandle?.Invoke(advertisedScheme) ?? string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public Task<ExplorerAuthSignIn> ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("The session chrome signs in through IExplorerAuthSession, never a method directly.");
}
