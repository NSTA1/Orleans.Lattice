namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>Resolves the ambient <see cref="LatticeCredentialContext"/> principal as the gate subject.</summary>
internal sealed class RepoContextAppMembershipContext : ILatticeMembershipContext
{
    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        TryResolveCurrent(out var subject);
        return new ValueTask<LatticeSubject>(subject);
    }

    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        var principal = LatticeCredentialContext.Current?.PrincipalId;
        subject = string.IsNullOrEmpty(principal) ? LatticeSubject.Anonymous : new LatticeSubject(principal);
        return true;
    }
}
