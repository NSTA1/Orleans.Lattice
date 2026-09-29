namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// An <see cref="ILatticeAccessGate"/> that allows exactly the (subject, tree, operation)
/// triples it was granted - or, with <see cref="AllowByDefault"/>, everything not explicitly
/// denied. An app role is granted by a binding (group membership); this gate can only take a
/// bound role away, through <see cref="DenyEverywhere"/>, which wins over every grant.
/// </summary>
internal sealed class RepoContextAppGrantingGate : ILatticeAccessGate
{
    private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _grants = new();
    private readonly HashSet<string> _deniedEverywhere = new(StringComparer.Ordinal);

    /// <summary>
    /// When true, a request neither granted nor denied is allowed - the policy as it stands once
    /// an install's compiled app rules are live and nothing denies the caller.
    /// </summary>
    public bool AllowByDefault { get; set; }

    public RepoContextAppGrantingGate Grant(string subject, string tree, LatticeOperation operations)
    {
        foreach (var value in Enum.GetValues<LatticeOperation>())
        {
            if (value != LatticeOperation.None && (operations & value) == value)
            {
                _grants.Add((subject, tree, value));
            }
        }

        return this;
    }

    /// <summary>
    /// Denies <paramref name="subject"/> every operation on every tree, as a cluster-wide deny rule does. It wins
    /// over every grant and over <see cref="AllowByDefault"/>.
    /// </summary>
    public RepoContextAppGrantingGate DenyEverywhere(string subject)
    {
        _deniedEverywhere.Add(subject);
        return this;
    }

    public void RevokeAll() => _grants.Clear();

    public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
    {
        if (_deniedEverywhere.Contains(request.Subject.SubjectId))
        {
            return new(LatticeAccessDecision.Deny("explicitly denied"));
        }

        return new(AllowByDefault || _grants.Contains((request.Subject.SubjectId, request.TreeId, request.Operation))
            ? LatticeAccessDecision.Allow()
            : LatticeAccessDecision.Deny("not granted"));
    }
}
