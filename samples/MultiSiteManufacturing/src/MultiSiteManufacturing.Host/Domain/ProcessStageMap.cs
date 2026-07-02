namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Maps a <see cref="Fact"/> to the <see cref="ProcessStage"/> it advances a
/// part into, so a part's "latest stage" reflects its furthest-along lifecycle
/// milestone rather than only the last <see cref="ProcessStepCompleted"/>. A
/// FinalAcceptance-signed part reads as <see cref="ProcessStage.FAI"/>, an
/// inspection as <see cref="ProcessStage.NDT"/>, and any MRB / rework activity
/// as <see cref="ProcessStage.MRB"/>.
/// </summary>
/// <remarks>
/// Shared by <see cref="Lattice.ComplianceFoldProjection"/> (which records the
/// stage of the newest folded fact into its accumulator) and the dashboard
/// broadcaster's per-part summary build, so both paths derive "latest stage"
/// from one mapping and cannot drift.
/// </remarks>
public static class ProcessStageMap
{
    /// <summary>
    /// Returns the <see cref="ProcessStage"/> the supplied <paramref name="fact"/>
    /// advances the part into, or <see langword="null"/> for a fact that carries
    /// no stage signal.
    /// </summary>
    public static ProcessStage? Of(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);

        return fact switch
        {
            ProcessStepCompleted step => step.Stage,
            InspectionRecorded => ProcessStage.NDT,
            NonConformanceRaised => ProcessStage.MRB,
            MrbDisposition => ProcessStage.MRB,
            ReworkCompleted => ProcessStage.MRB,
            FinalAcceptance => ProcessStage.FAI,
            _ => null,
        };
    }
}
