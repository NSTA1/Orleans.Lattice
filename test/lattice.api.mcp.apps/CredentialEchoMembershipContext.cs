namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Resolves the ambient <see cref="LatticeCredentialContext"/> principal as the subject id, so a
/// fixture can prove the source evaluated the gate under the caller's own credential.
/// </summary>
internal sealed class CredentialEchoMembershipContext : ILatticeMembershipContext
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
