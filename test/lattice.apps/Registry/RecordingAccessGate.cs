namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An <see cref="ILatticeAccessGate"/> test double that returns a configured decision and
/// records every request it was asked to authorize, together with whether system-origin
/// was active at the time.
/// </summary>
internal sealed class RecordingAccessGate : ILatticeAccessGate
{
    private readonly Func<LatticeAccessRequest, LatticeAccessDecision> _decide;

    public RecordingAccessGate(Func<LatticeAccessRequest, LatticeAccessDecision> decide) => _decide = decide;

    /// <summary>A gate that allows every request.</summary>
    public static RecordingAccessGate AllowAll() => new(_ => LatticeAccessDecision.Allow());

    /// <summary>A gate that denies every request.</summary>
    public static RecordingAccessGate DenyAll() => new(_ => LatticeAccessDecision.Deny("denied by test gate"));

    /// <summary>The requests authorized so far, in order.</summary>
    public List<LatticeAccessRequest> Requests { get; } = new();

    /// <summary>Whether system-origin was active for each recorded request.</summary>
    public List<bool> SystemOriginObserved { get; } = new();

    public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
    {
        Requests.Add(request);
        SystemOriginObserved.Add(LatticeSystemOrigin.IsActive);
        return new ValueTask<LatticeAccessDecision>(_decide(request));
    }
}
