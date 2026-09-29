namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Resolves the ambient <see cref="LatticeCredentialContext"/> principal as the subject id, with the groups a
/// fixture joined it to as its group closure, so a fixture can prove the source evaluated roles under the
/// caller's own credential and by binding.
/// </summary>
internal sealed class CredentialEchoMembershipContext : ILatticeMembershipContext
{
    private readonly Dictionary<string, HashSet<string>> _groups = new(StringComparer.Ordinal);

    /// <summary>How many times a subject was resolved.</summary>
    public int Resolutions { get; private set; }

    /// <summary>A fault every resolution throws, or null.</summary>
    public Exception? Fault { get; set; }

    /// <summary>Adds <paramref name="subject"/> to <paramref name="group"/>.</summary>
    public CredentialEchoMembershipContext Join(string subject, string group)
    {
        if (!_groups.TryGetValue(subject, out var groups))
            _groups.Add(subject, groups = new HashSet<string>(StringComparer.Ordinal));
        groups.Add(group);
        return this;
    }

    /// <summary>Removes every subject from every group.</summary>
    public void LeaveAll() => _groups.Clear();

    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        Resolutions++;
        if (Fault is not null)
            throw Fault;
        return new ValueTask<LatticeSubject>(Current());
    }

    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        if (Fault is not null)
        {
            subject = default;
            return false;
        }

        Resolutions++;
        subject = Current();
        return true;
    }

    private LatticeSubject Current()
    {
        var principal = LatticeCredentialContext.Current?.PrincipalId;
        if (string.IsNullOrEmpty(principal))
            return LatticeSubject.Anonymous;
        return new LatticeSubject(
            principal,
            _groups.TryGetValue(principal, out var groups) ? new HashSet<string>(groups, StringComparer.Ordinal) : null);
    }
}
