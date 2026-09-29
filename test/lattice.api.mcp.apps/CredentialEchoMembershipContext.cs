namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Resolves the ambient <see cref="LatticeCredentialContext"/> principal as the subject id, so a
/// fixture can prove the source evaluated the caller under its own credential. The subject's group
/// closure is whatever <see cref="Groups"/> maps the principal to (none by default), and a set
/// <see cref="Fault"/> makes every resolution throw it.
/// </summary>
internal sealed class CredentialEchoMembershipContext : ILatticeMembershipContext
{
    /// <summary>The group closure each principal resolves with; a principal absent here belongs to no group.</summary>
    public Dictionary<string, string[]> Groups { get; } = new(StringComparer.Ordinal);

    /// <summary>A fault every resolution throws, or null.</summary>
    public Exception? Fault { get; set; }

    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        if (Fault is not null)
            return ValueTask.FromException<LatticeSubject>(Fault);
        Resolve(out var subject);
        return new ValueTask<LatticeSubject>(subject);
    }

    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        if (Fault is not null)
        {
            subject = default;
            return false;
        }

        Resolve(out subject);
        return true;
    }

    private void Resolve(out LatticeSubject subject)
    {
        var principal = LatticeCredentialContext.Current?.PrincipalId;
        subject = string.IsNullOrEmpty(principal)
            ? LatticeSubject.Anonymous
            : new LatticeSubject(principal, Groups.TryGetValue(principal, out var groups) ? groups : null);
    }
}