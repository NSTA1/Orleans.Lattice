namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>An access gate returning a configurable decision and recording every request.</summary>
internal sealed class RecordingAccessGate : ILatticeAccessGate
{
    public LatticeAccessDecision Decision { get; set; } = LatticeAccessDecision.Allow();

    public List<LatticeAccessRequest> Requests { get; } = [];

    public Exception? Fault { get; set; }

    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        Requests.Add(request);
        if (Fault is not null)
        {
            throw Fault;
        }

        return new ValueTask<LatticeAccessDecision>(Decision);
    }
}
