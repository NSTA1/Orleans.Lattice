namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Naïve fold: applies facts in the order supplied, without re-sorting by
/// hybrid logical clock. Used by the baseline backend to model the behaviour
/// of a system that trusts arrival order — so under chaos-induced reorder
/// it disagrees with <see cref="ComplianceFold"/>, which is the whole point
/// of the demo.
/// </summary>
public static class NaiveFold
{
    /// <summary>Applies every fact, in the order they are enumerated, to the initial state.</summary>
    public static ComplianceState Fold(IEnumerable<Fact> factsInArrivalOrder)
    {
        ArgumentNullException.ThrowIfNull(factsInArrivalOrder);

        var state = ComplianceState.Nominal;
        var retestArmed = false;

        foreach (var fact in factsInArrivalOrder)
        {
            (state, retestArmed) = StateTransitions.Apply(state, retestArmed, fact);
        }

        return state;
    }

    /// <summary>Advances a pre-computed state by a single new fact (for incremental backends).</summary>
    public static (ComplianceState State, bool RetestArmed) Step(
        ComplianceState current,
        bool retestArmed,
        Fact fact) =>
        StateTransitions.Apply(current, retestArmed, fact);
}
