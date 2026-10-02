namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// An access gate that reproduces the decision shape of a tree-wide allow carrying
/// exact-key deny carve-outs, as the policy engine evaluates it: a point request
/// for a carved-out key is denied and any other point is allowed, while a
/// collection (range or whole-tree) request returns a filtered allow that excludes
/// the carved-out keys. It records every request it was asked about so a test can
/// assert which shape reached the gate.
/// </summary>
internal sealed class CarveOutAccessGate(params string[] deniedKeys) : ILatticeAccessGate
{
    private readonly HashSet<string> _denied = new(deniedKeys, StringComparer.Ordinal);

    /// <summary>Every request the gate was asked to decide, in order.</summary>
    public List<LatticeAccessRequest> Requests { get; } = [];

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        lock (Requests)
        {
            Requests.Add(request);
        }

        if (request.Key is not null)
        {
            return new(_denied.Contains(request.Key)
                ? LatticeAccessDecision.Deny($"key '{request.Key}' is carved out")
                : LatticeAccessDecision.Allow());
        }

        var denied = _denied;
        return new(LatticeAccessDecision.Filtered(k => !denied.Contains(k), "filtered per key by carve-outs"));
    }
}
