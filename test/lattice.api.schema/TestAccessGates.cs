using Orleans.Lattice;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Test <see cref="ILatticeAccessGate"/> doubles shared by the facade tests: a
/// recording gate that returns a configurable decision, and an operation-scoped
/// gate that allows only a fixed set of <see cref="LatticeOperation"/> values so
/// the read / manage split can be exercised without a cluster.
/// </summary>
internal sealed class RecordingAccessGate(Func<LatticeAccessRequest, LatticeAccessDecision>? decide = null)
    : ILatticeAccessGate
{
    private readonly Func<LatticeAccessRequest, LatticeAccessDecision> _decide =
        decide ?? (_ => LatticeAccessDecision.Allow());

    /// <summary>The most recent request the gate observed.</summary>
    public LatticeAccessRequest Last { get; private set; }

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request, CancellationToken cancellationToken = default)
    {
        Last = request;
        return new ValueTask<LatticeAccessDecision>(_decide(request));
    }

    /// <summary>A gate that allows everything.</summary>
    public static RecordingAccessGate Allow() => new();

    /// <summary>A gate that denies everything with <paramref name="reason"/>.</summary>
    public static RecordingAccessGate Deny(string reason = "denied") =>
        new(_ => LatticeAccessDecision.Deny(reason));
}

/// <summary>
/// A gate that allows only the listed operations and denies all others. Used to
/// model a caller with read authority but no schema-admin authority (and the
/// reverse).
/// </summary>
internal sealed class OperationScopedGate(params LatticeOperation[] allowed) : ILatticeAccessGate
{
    private readonly HashSet<LatticeOperation> _allowed = new(allowed);

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request, CancellationToken cancellationToken = default)
    {
        var decision = _allowed.Contains(request.Operation)
            ? LatticeAccessDecision.Allow()
            : LatticeAccessDecision.Deny($"{request.Operation} not granted");
        return new ValueTask<LatticeAccessDecision>(decision);
    }
}
