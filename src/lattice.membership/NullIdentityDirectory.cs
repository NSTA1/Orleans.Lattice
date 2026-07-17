namespace Orleans.Lattice.Membership;

/// <summary>
/// The default no-op <see cref="ILatticeIdentityDirectory"/>: no external identity
/// source is configured, so search returns an empty page, resolve returns
/// <c>null</c> for every id, and <see cref="Explanation"/> tells the operator that
/// ids are accepted without validation. Registered as the default in
/// <see cref="LatticeMembershipServiceCollectionExtensions.AddLatticeMembership(Orleans.Hosting.ISiloBuilder, System.Action{LatticeMembershipOptions})"/>,
/// so the facade always resolves an instance and any real provider overrides it
/// with a last-wins registration.
/// </summary>
public sealed class NullIdentityDirectory : ILatticeIdentityDirectory
{
    /// <summary>The stable <see cref="ProviderId"/> of the no-op provider.</summary>
    public const string NullProviderId = "null";

    private static readonly Task<DirectorySearchPage> EmptyPageResult = Task.FromResult(DirectorySearchPage.Empty);
    private static readonly Task<DirectoryPrincipal?> NullPrincipalResult = Task.FromResult<DirectoryPrincipal?>(null);

    /// <inheritdoc />
    public string ProviderId => NullProviderId;

    /// <inheritdoc />
    public string Explanation =>
        "No identity directory is configured - ids are accepted without validation.";

    /// <inheritdoc />
    public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default) =>
        EmptyPageResult;

    /// <inheritdoc />
    public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principalId);
        return NullPrincipalResult;
    }
}
