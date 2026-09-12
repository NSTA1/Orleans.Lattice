namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A real, deny-by-default <see cref="ILatticeAccessGate"/> for tests that need a
/// genuinely gated host without taking a dependency on an authorization add-on.
/// </summary>
/// <remarks>
/// This is a real gate, not an inert one. <c>LatticeAccessGateEnforcement</c>
/// short-circuits enforcement only when the turn is system-origin or the
/// registered gate is the <c>NullLatticeAccessGate</c>, so a host wired with this
/// gate exercises the same enforcement path a production deny-by-default policy
/// does. That is what makes it valid for issue #2608's requirement that the
/// regression test register a gate whose default effect is deny.
/// </remarks>
internal sealed class DenyByDefaultAccessGate : ILatticeAccessGate
{
    private int _consultations;

    /// <summary>The number of times the gate was consulted.</summary>
    public int Consultations => Volatile.Read(ref _consultations);

    /// <summary>
    /// Operations to allow. Anything absent is denied, so the gate's default
    /// effect is deny in the same sense a production policy's is.
    /// </summary>
    public HashSet<LatticeOperation> Allowed { get; } = [];

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _consultations);
        return new ValueTask<LatticeAccessDecision>(
            Allowed.Contains(request.Operation)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("Denied by the test gate's default-deny effect."));
    }
}
