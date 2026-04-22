namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Deterministic fold from an unordered bag of facts to the current
/// <see cref="ComplianceState"/> of a part. The fold orders facts by their
/// hybrid logical clock before application, so concurrent fact producers
/// converge on the same state.
/// </summary>
/// <remarks>
/// <para>Transitions (see <c>plan.md §3.4</c>):</para>
/// <list type="bullet">
///   <item><c>ProcessStepCompleted</c> → no state change.</item>
///   <item><c>InspectionRecorded(Pass)</c> → no state change; arms the retest
///   flag when the part is in <see cref="ComplianceState.Rework"/>.</item>
///   <item><c>InspectionRecorded(Fail)</c> → escalates to <see cref="ComplianceState.FlaggedForReview"/>.</item>
///   <item><c>NonConformanceRaised</c> → escalates per severity
///   (Minor→Flagged, Major→Rework, Critical→Scrap).</item>
///   <item><c>MrbDisposition(UseAsIs)</c> → demotes <see cref="ComplianceState.FlaggedForReview"/>
///   back to <see cref="ComplianceState.Nominal"/>; also demotes
///   <see cref="ComplianceState.Rework"/> if a prior retest pass armed the flag.</item>
///   <item><c>MrbDisposition(Rework)</c> → escalates to <see cref="ComplianceState.Rework"/>.</item>
///   <item><c>MrbDisposition(Scrap | ReturnToVendor)</c> → terminal <see cref="ComplianceState.Scrap"/>.</item>
///   <item><c>ReworkCompleted(retestPassed=true)</c> → arms the retest flag
///   when in <see cref="ComplianceState.Rework"/>.</item>
///   <item><c>FinalAcceptance</c> → asserts completion; no state change
///   (its presence without outstanding severity is the "done" signal).</item>
/// </list>
/// <para><see cref="ComplianceState.Scrap"/> is terminal: later facts are ignored.</para>
/// </remarks>
public static class ComplianceFold
{
    /// <summary>Computes the compliance state from an unordered fact set.</summary>
    public static ComplianceState Fold(IEnumerable<Fact> facts)
    {
        ArgumentNullException.ThrowIfNull(facts);

        var ordered = facts
            .OrderBy(f => f.Hlc.WallClockTicks)
            .ThenBy(f => f.Hlc.Counter)
            .ThenBy(f => f.FactId);

        var state = ComplianceState.Nominal;
        var retestArmed = false;

        foreach (var fact in ordered)
        {
            if (state == ComplianceState.Scrap)
            {
                // Terminal; later facts cannot revive a scrapped part.
                continue;
            }

            switch (fact)
            {
                case ProcessStepCompleted:
                    break;

                case InspectionRecorded { Outcome: InspectionOutcome.Pass }:
                    if (state == ComplianceState.Rework)
                    {
                        retestArmed = true;
                    }
                    break;

                case InspectionRecorded { Outcome: InspectionOutcome.Fail }:
                    state = Max(state, ComplianceState.FlaggedForReview);
                    retestArmed = false;
                    break;

                case NonConformanceRaised nc:
                    state = Max(state, nc.Severity switch
                    {
                        NcSeverity.Minor => ComplianceState.FlaggedForReview,
                        NcSeverity.Major => ComplianceState.Rework,
                        NcSeverity.Critical => ComplianceState.Scrap,
                        _ => state,
                    });
                    retestArmed = false;
                    break;

                case MrbDisposition md:
                    state = ApplyDisposition(state, md.Disposition, ref retestArmed);
                    break;

                case ReworkCompleted { RetestPassed: true }:
                    if (state == ComplianceState.Rework)
                    {
                        retestArmed = true;
                    }
                    break;

                case ReworkCompleted { RetestPassed: false }:
                    // Retest failed — stays in Rework, flag stays disarmed.
                    retestArmed = false;
                    break;

                case FinalAcceptance:
                    break;
            }
        }

        return state;
    }

    private static ComplianceState ApplyDisposition(
        ComplianceState current,
        MrbDispositionKind disposition,
        ref bool retestArmed)
    {
        switch (disposition)
        {
            case MrbDispositionKind.UseAsIs:
                if (current == ComplianceState.FlaggedForReview)
                {
                    return ComplianceState.Nominal;
                }
                if (current == ComplianceState.Rework && retestArmed)
                {
                    retestArmed = false;
                    return ComplianceState.Nominal;
                }
                return current;

            case MrbDispositionKind.Rework:
                retestArmed = false;
                return Max(current, ComplianceState.Rework);

            case MrbDispositionKind.Scrap:
            case MrbDispositionKind.ReturnToVendor:
                return ComplianceState.Scrap;

            default:
                return current;
        }
    }

    private static ComplianceState Max(ComplianceState a, ComplianceState b) =>
        (ComplianceState)Math.Max((int)a, (int)b);
}
