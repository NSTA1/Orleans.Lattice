namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// The dependency-free default <see cref="IEntraGroupResolver"/>. It performs no
/// out-of-band lookup: it echoes back the group ids the token still carried
/// (empty on a pure overage). Registering it makes the overage path explicit and
/// deterministic without pulling in Microsoft Graph; the resolved membership is
/// still merged with the local directory upstream by the subject mapper. This is
/// exactly the behaviour the authenticator falls back to when
/// <see cref="EntraGroupResolutionMode.ResolveOnOverage"/> is configured but no
/// resolver is registered.
/// </summary>
public sealed class TokenOnlyEntraGroupResolver : IEntraGroupResolver
{
    private static readonly IReadOnlyCollection<string> Empty = Array.Empty<string>();

    /// <inheritdoc />
    public ValueTask<IReadOnlyCollection<string>> ResolveGroupsAsync(
        EntraGroupResolutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new ValueTask<IReadOnlyCollection<string>>(context.TokenAssertedGroups ?? Empty);
    }
}
