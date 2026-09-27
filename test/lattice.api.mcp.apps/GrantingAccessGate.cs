namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// An <see cref="ILatticeAccessGate"/> that allows exactly the (tree, operation) pairs it was
/// granted, for the subject id it was granted to, and records every request it saw.
/// </summary>
internal sealed class GrantingAccessGate : ILatticeAccessGate
{
    private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _grants = new();

    public List<LatticeAccessRequest> Requests { get; } = new();

    public Func<LatticeAccessRequest, LatticeAccessDecision?>? Override { get; set; }

    public Exception? Fault { get; set; }

    public GrantingAccessGate Grant(string subject, string tree, LatticeOperation operations)
    {
        foreach (var value in Enum.GetValues<LatticeOperation>())
        {
            if (value != LatticeOperation.None && (operations & value) == value)
                _grants.Add((subject, tree, value));
        }

        return this;
    }

    public void RevokeAll() => _grants.Clear();

    public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
    {
        Requests.Add(request);
        if (Fault is not null)
            throw Fault;
        if (Override?.Invoke(request) is { } decision)
            return new ValueTask<LatticeAccessDecision>(decision);

        return new ValueTask<LatticeAccessDecision>(
            _grants.Contains((request.Subject.SubjectId, request.TreeId, request.Operation))
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not granted"));
    }
}
