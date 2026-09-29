namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// Resolves the ambient <see cref="LatticeCredentialContext"/> principal as the subject, with the group
/// closure <see cref="Groups"/> holds (empty by default).
/// </summary>
internal sealed class RepoContextAppMembershipContext : ILatticeMembershipContext
{
    /// <summary>The groups the resolved principal is a member of.</summary>
    public HashSet<string> Groups { get; } = new(StringComparer.Ordinal);

    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        TryResolveCurrent(out var subject);
        return new ValueTask<LatticeSubject>(subject);
    }

    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        var principal = LatticeCredentialContext.Current?.PrincipalId;
        subject = string.IsNullOrEmpty(principal) ? LatticeSubject.Anonymous : new LatticeSubject(principal, Groups.ToArray());
        return true;
    }
}