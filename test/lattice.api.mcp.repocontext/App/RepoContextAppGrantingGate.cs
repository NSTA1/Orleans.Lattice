namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// An <see cref="ILatticeAccessGate"/> that allows exactly the (subject, tree, operation)
/// triples it was granted, so a fixture decides deterministically which app roles a
/// caller holds.
/// </summary>
internal sealed class RepoContextAppGrantingGate : ILatticeAccessGate
{
    private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _grants = new();

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

    public void RevokeAll() => _grants.Clear();

    public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        => new(_grants.Contains((request.Subject.SubjectId, request.TreeId, request.Operation))
            ? LatticeAccessDecision.Allow()
            : LatticeAccessDecision.Deny("not granted"));
}
