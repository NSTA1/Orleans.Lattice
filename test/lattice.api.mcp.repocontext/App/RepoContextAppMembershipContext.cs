namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// Resolves the ambient <see cref="LatticeCredentialContext"/> principal as the gate subject, with the groups a
/// fixture joined it to as its group closure, so a fixture grants an app role by binding.
/// </summary>
internal sealed class RepoContextAppMembershipContext : ILatticeMembershipContext
{
    private readonly Dictionary<string, HashSet<string>> _groups = new(StringComparer.Ordinal);

    /// <summary>Adds <paramref name="subject"/> to <paramref name="group"/>.</summary>
    public RepoContextAppMembershipContext Join(string subject, string group)
    {
        if (!_groups.TryGetValue(subject, out var groups))
        {
            _groups.Add(subject, groups = new HashSet<string>(StringComparer.Ordinal));
        }

        groups.Add(group);
        return this;
    }

    /// <summary>Removes every subject from every group.</summary>
    public void LeaveAll() => _groups.Clear();

    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        TryResolveCurrent(out var subject);
        return new ValueTask<LatticeSubject>(subject);
    }

    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        var principal = LatticeCredentialContext.Current?.PrincipalId;
        subject = string.IsNullOrEmpty(principal)
            ? LatticeSubject.Anonymous
            : new LatticeSubject(
                principal,
                _groups.TryGetValue(principal, out var groups) ? new HashSet<string>(groups, StringComparer.Ordinal) : null);
        return true;
    }
}
