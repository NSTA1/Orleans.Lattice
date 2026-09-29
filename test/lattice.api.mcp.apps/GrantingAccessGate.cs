using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// An <see cref="ILatticeAccessGate"/> that allows exactly the (tree, operation) pairs it was
/// granted, for the subject id it was granted to, and records every request it saw. Explicit
/// denies win over grants and over <see cref="AllowByDefault"/>, as a deny rule does in the real
/// gate; a deny on <see cref="LatticeScope.ClusterWideTreeId"/> applies to every tree.
/// </summary>
internal sealed class GrantingAccessGate : ILatticeAccessGate
{
    private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _grants = new();
    private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _denies = new();

    public List<LatticeAccessRequest> Requests { get; } = new();

    public Func<LatticeAccessRequest, LatticeAccessDecision?>? Override { get; set; }

    public Exception? Fault { get; set; }

    /// <summary>
    /// When true, a request neither granted nor denied is allowed - the policy as it stands once an
    /// install's compiled app rules are live and nothing denies the caller.
    /// </summary>
    public bool AllowByDefault { get; set; }

    public GrantingAccessGate Grant(string subject, string tree, LatticeOperation operations)
    {
        Add(_grants, subject, tree, operations);
        return this;
    }

    /// <summary>Adds an explicit deny, which wins over every grant.</summary>
    public GrantingAccessGate Deny(string subject, string tree, LatticeOperation operations)
    {
        Add(_denies, subject, tree, operations);
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

        var subject = request.Subject.SubjectId;
        if (_denies.Contains((subject, request.TreeId, request.Operation))
            || _denies.Contains((subject, LatticeScope.ClusterWideTreeId, request.Operation)))
        {
            return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("explicitly denied"));
        }

        return new ValueTask<LatticeAccessDecision>(
            AllowByDefault || _grants.Contains((subject, request.TreeId, request.Operation))
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not granted"));
    }

    private static void Add(
        HashSet<(string Subject, string Tree, LatticeOperation Operation)> set,
        string subject,
        string tree,
        LatticeOperation operations)
    {
        foreach (var value in Enum.GetValues<LatticeOperation>())
        {
            if (value != LatticeOperation.None && (operations & value) == value)
                set.Add((subject, tree, value));
        }
    }
}
