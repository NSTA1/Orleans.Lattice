using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Baseline;

/// <summary>
/// Persistent state for <see cref="BaselinePartGrain"/>. Holds the
/// arrival-order fact log plus the incrementally maintained
/// <see cref="ComplianceState"/> under the naïve fold.
/// </summary>
[GenerateSerializer]
public sealed record BaselinePartState
{
    /// <summary>Facts applied so far, in the order they arrived at the grain.</summary>
    [Id(0)] public List<Fact> FactsInArrivalOrder { get; init; } = [];

    /// <summary>Current state under the arrival-order (baseline) fold.</summary>
    [Id(1)] public ComplianceState State { get; set; } = ComplianceState.Nominal;

    /// <summary>Retest-armed flag threaded through the state transitions.</summary>
    [Id(2)] public bool RetestArmed { get; set; }
}
