namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A scripted <see cref="ILatticeAccessGate"/> that answers each
/// <see cref="LatticeOperation"/> from a fixed decision table, so a test can grant
/// the telemetry capability without granting platform-operator authority (and the
/// reverse), which is exactly the distinction the facade turns on.
/// </summary>
internal sealed class StubAccessGate : ILatticeAccessGate
{
    private readonly Dictionary<LatticeOperation, LatticeAccessDecision> _decisions = [];

    /// <summary>Every tree id the gate was asked about, in call order.</summary>
    public List<string> AuthorizedTrees { get; } = [];

    /// <summary>The decision returned for an operation with no scripted entry.</summary>
    public LatticeAccessDecision Fallback { get; set; } = LatticeAccessDecision.Deny("no rule");

    /// <summary>Scripts <paramref name="decision"/> for <paramref name="operation"/>.</summary>
    /// <param name="operation">The operation to script.</param>
    /// <param name="decision">The decision to return.</param>
    /// <returns>This gate, for chaining.</returns>
    public StubAccessGate For(LatticeOperation operation, LatticeAccessDecision decision)
    {
        _decisions[operation] = decision;
        return this;
    }

    /// <summary>Scripts an allow for <paramref name="operation"/>.</summary>
    /// <param name="operation">The operation to allow.</param>
    /// <returns>This gate, for chaining.</returns>
    public StubAccessGate Allowing(LatticeOperation operation) =>
        For(operation, LatticeAccessDecision.Allow());

    /// <summary>A gate that grants the telemetry capability and nothing else.</summary>
    /// <returns>The gate.</returns>
    public static StubAccessGate TelemetryOnly() =>
        new StubAccessGate().Allowing(LatticeOperation.Telemetry);

    /// <summary>A gate that grants telemetry and platform-operator authority.</summary>
    /// <returns>The gate.</returns>
    public static StubAccessGate PlatformOperator() =>
        TelemetryOnly().Allowing(LatticeOperation.Admin);

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request, CancellationToken cancellationToken = default)
    {
        AuthorizedTrees.Add(request.TreeId);
        return new ValueTask<LatticeAccessDecision>(
            _decisions.TryGetValue(request.Operation, out var decision) ? decision : Fallback);
    }
}
