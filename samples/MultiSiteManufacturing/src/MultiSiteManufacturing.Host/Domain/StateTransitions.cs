namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Per-fact state transition rules, factored out of <see cref="ComplianceFold"/>
/// so the lattice (HLC-ordered) and baseline (arrival-ordered) folds share
/// the same semantics — the <em>only</em> difference between them is the
/// order in which facts are applied.
/// </summary>
/// <remarks>
/// Returning the updated retest flag as part of the tuple keeps the function
/// pure: callers thread the flag through the sequence they're folding without
/// the helper owning any state of its own.
/// </remarks>
internal static class StateTransitions
{
    /// <summary>
    /// Applies a single <paramref name="fact"/> to the supplied
    /// <paramref name="current"/> state. Returns the new state plus the
    /// updated retest-armed flag. When <paramref name="current"/> is
    /// <see cref="ComplianceState.Scrap"/> the state is returned unchanged
    /// (Scrap is terminal).
    /// </summary>
    public static (ComplianceState State, bool RetestArmed) Apply(
        ComplianceState current,
        bool retestArmed,
        Fact fact)
    {
        if (current == ComplianceState.Scrap)
        {
            return (current, retestArmed);
        }

        switch (fact)
        {
            case ProcessStepCompleted:
                return (current, retestArmed);

            case InspectionRecorded { Outcome: InspectionOutcome.Pass }:
                return current == ComplianceState.Rework
                    ? (current, true)
                    : (current, retestArmed);

            case InspectionRecorded { Outcome: InspectionOutcome.Fail }:
                return (Max(current, ComplianceState.FlaggedForReview), false);

            case NonConformanceRaised nc:
                var escalated = nc.Severity switch
                {
                    NcSeverity.Minor => ComplianceState.FlaggedForReview,
                    NcSeverity.Major => ComplianceState.Rework,
                    NcSeverity.Critical => ComplianceState.Scrap,
                    _ => current,
                };
                return (Max(current, escalated), false);

            case MrbDisposition md:
                return ApplyDisposition(current, retestArmed, md.Disposition);

            case ReworkCompleted { RetestPassed: true }:
                return current == ComplianceState.Rework
                    ? (current, true)
                    : (current, retestArmed);

            case ReworkCompleted { RetestPassed: false }:
                // A failed retest is observable defect evidence. Even if an
                // earlier UseAsIs (possibly from a race trio) demoted the
                // part to Nominal, recording a failed retest must escalate
                // it back into the review/rework band so the operator's
                // click has a visible effect.
                return (Max(current, ComplianceState.FlaggedForReview), false);

            case FinalAcceptance:
                return (current, retestArmed);

            default:
                return (current, retestArmed);
        }
    }

    private static (ComplianceState State, bool RetestArmed) ApplyDisposition(
        ComplianceState current,
        bool retestArmed,
        MrbDispositionKind disposition)
    {
        switch (disposition)
        {
            case MrbDispositionKind.UseAsIs:
                if (current == ComplianceState.FlaggedForReview)
                {
                    return (ComplianceState.Nominal, retestArmed);
                }
                if (current == ComplianceState.Rework && retestArmed)
                {
                    return (ComplianceState.Nominal, false);
                }
                return (current, retestArmed);

            case MrbDispositionKind.Rework:
                return (Max(current, ComplianceState.Rework), false);

            case MrbDispositionKind.Scrap:
            case MrbDispositionKind.ReturnToVendor:
                return (ComplianceState.Scrap, false);

            default:
                return (current, retestArmed);
        }
    }

    private static ComplianceState Max(ComplianceState a, ComplianceState b) =>
        (ComplianceState)Math.Max((int)a, (int)b);
}
