using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Operator;

/// <summary>
/// Classification of the single suggested next action for a part, based
/// purely on its observable fact log. Drives the primary next-action
/// button on the part-detail page.
/// </summary>
public enum NextAction
{
    /// <summary>Part is terminal (FAI accepted or MRB Scrap / ReturnToVendor). No further actions.</summary>
    Terminal,

    /// <summary>Advance from Forge to Heat Treat.</summary>
    CompleteHeatTreat,

    /// <summary>Advance from Heat Treat to Machining.</summary>
    CompleteMachining,

    /// <summary>Machining complete; record the CMM dimensional inspection.</summary>
    RecordCmmInspection,

    /// <summary>CMM passed; record the NDT inspection. Branch: Pass / Fail.</summary>
    RecordNdtInspection,

    /// <summary>All stages + inspections passed and no open NCR; sign off the FAI.</summary>
    SignOffFai,

    /// <summary>MRB ordered rework; operator records rework completion. Branch: retest pass / fail.</summary>
    CompleteRework,

    /// <summary>An NCR is open and awaits an MRB disposition. Branch: UseAsIs / Rework / Scrap / ReturnToVendor.</summary>
    IssueMrb,
}

/// <summary>
/// Resolved next-action suggestion. For the <see cref="NextAction.IssueMrb"/>
/// branch, <see cref="OpenNcNumber"/> identifies the non-conformance that
/// needs dispositioning.
/// </summary>
/// <param name="Action">The suggested next action.</param>
/// <param name="OpenNcNumber">NC number awaiting disposition, or <see langword="null"/>.</param>
public readonly record struct NextActionSuggestion(NextAction Action, string? OpenNcNumber);

/// <summary>
/// Inspects a part's fact log and returns the single suggested next action.
/// Happy-path lifecycle:
/// <c>Forge → HeatTreat → Machining → CMM → NDT → FAI</c>, with branch
/// points at NDT outcome, rework retest, and MRB disposition that require
/// operator choice.
/// </summary>
public static class NextActionResolver
{
    private static readonly Inspection[] NdtInspections =
        [Inspection.FPI, Inspection.EddyCurrent, Inspection.XRay];

    /// <summary>Resolves the suggested next action from the part's facts. Facts may be in any order.</summary>
    /// <param name="facts">All facts recorded against the part.</param>
    /// <exception cref="ArgumentNullException"><paramref name="facts"/> is <see langword="null"/>.</exception>
    public static NextActionSuggestion Resolve(IReadOnlyList<Fact> facts)
    {
        ArgumentNullException.ThrowIfNull(facts);

        // HLC-sorted replay matches the lattice fold's view.
        var ordered = facts
            .OrderBy(f => f.Hlc.WallClockTicks)
            .ThenBy(f => f.Hlc.Counter)
            .ToArray();

        // 1. Terminal — FAI accepted or MRB Scrap / ReturnToVendor.
        var hasFai = ordered.OfType<FinalAcceptance>().Any();
        var hasTerminalMrb = ordered.OfType<MrbDisposition>()
            .Any(m => m.Disposition is MrbDispositionKind.Scrap or MrbDispositionKind.ReturnToVendor);
        if (hasFai || hasTerminalMrb)
        {
            return new NextActionSuggestion(NextAction.Terminal, null);
        }

        // 2. Open NCR — raised, not yet dispositioned via MRB.
        var dispositionedNcs = ordered.OfType<MrbDisposition>()
            .Select(m => m.NcNumber)
            .ToHashSet(StringComparer.Ordinal);
        var openNc = ordered.OfType<NonConformanceRaised>()
            .FirstOrDefault(n => !dispositionedNcs.Contains(n.NcNumber));
        if (openNc is not null)
        {
            return new NextActionSuggestion(NextAction.IssueMrb, openNc.NcNumber);
        }

        // 3. Rework ordered by MRB but not yet completed.
        var latestReworkOrderIdx = LastIndexOf(ordered,
            f => f is MrbDisposition { Disposition: MrbDispositionKind.Rework });
        var latestReworkDoneIdx = LastIndexOf(ordered, f => f is ReworkCompleted);
        if (latestReworkOrderIdx >= 0 && latestReworkDoneIdx < latestReworkOrderIdx)
        {
            return new NextActionSuggestion(NextAction.CompleteRework, null);
        }

        // 4. Rework completed — retest outcome determines next step.
        if (latestReworkDoneIdx >= 0)
        {
            var reworkDone = (ReworkCompleted)ordered[latestReworkDoneIdx];
            return reworkDone.RetestPassed
                ? new NextActionSuggestion(NextAction.SignOffFai, null)
                : new NextActionSuggestion(NextAction.RecordNdtInspection, null);
        }

        // 5. Main-path stage progression.
        ProcessStage? maxStage = null;
        foreach (var p in ordered.OfType<ProcessStepCompleted>())
        {
            if (maxStage is null || (int)p.Stage > (int)maxStage.Value)
            {
                maxStage = p.Stage;
            }
        }
        if (maxStage is null or ProcessStage.Forge)
        {
            return new NextActionSuggestion(NextAction.CompleteHeatTreat, null);
        }
        if (maxStage == ProcessStage.HeatTreat)
        {
            return new NextActionSuggestion(NextAction.CompleteMachining, null);
        }

        // 6. Machining done — require CMM pass, then NDT pass, then FAI.
        var hasCmmPass = ordered.OfType<InspectionRecorded>()
            .Any(i => i.Inspection == Inspection.CMM && i.Outcome == InspectionOutcome.Pass);
        if (!hasCmmPass)
        {
            return new NextActionSuggestion(NextAction.RecordCmmInspection, null);
        }

        var hasNdtPass = ordered.OfType<InspectionRecorded>()
            .Any(i => NdtInspections.Contains(i.Inspection) && i.Outcome == InspectionOutcome.Pass);
        if (!hasNdtPass)
        {
            return new NextActionSuggestion(NextAction.RecordNdtInspection, null);
        }

        return new NextActionSuggestion(NextAction.SignOffFai, null);
    }

    private static int LastIndexOf(Fact[] facts, Func<Fact, bool> predicate)
    {
        for (var i = facts.Length - 1; i >= 0; i--)
        {
            if (predicate(facts[i]))
            {
                return i;
            }
        }
        return -1;
    }
}

