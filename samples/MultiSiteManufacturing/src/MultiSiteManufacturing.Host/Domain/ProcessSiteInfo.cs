namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Metadata for <see cref="ProcessSite"/> enum values — display names, stage
/// affinity, and the ordered list used by UI surfaces.
/// </summary>
public static class ProcessSiteInfo
{
    private static readonly IReadOnlyDictionary<ProcessSite, string> DisplayNames =
        new Dictionary<ProcessSite, string>
        {
            [ProcessSite.OhioForge] = "Ohio Forge",
            [ProcessSite.NagoyaHeatTreat] = "Nagoya Heat Treatment",
            [ProcessSite.StuttgartMachining] = "Stuttgart Machining",
            [ProcessSite.StuttgartCmmLab] = "Stuttgart CMM Lab",
            [ProcessSite.ToulouseNdtLab] = "Toulouse NDT Lab",
            [ProcessSite.CincinnatiMrb] = "Cincinnati MRB",
            [ProcessSite.BristolFai] = "Bristol FAI",
        };

    private static readonly IReadOnlyDictionary<ProcessSite, ProcessStage> PrimaryStages =
        new Dictionary<ProcessSite, ProcessStage>
        {
            [ProcessSite.OhioForge] = ProcessStage.Forge,
            [ProcessSite.NagoyaHeatTreat] = ProcessStage.HeatTreat,
            [ProcessSite.StuttgartMachining] = ProcessStage.Machining,
            [ProcessSite.StuttgartCmmLab] = ProcessStage.Machining,
            [ProcessSite.ToulouseNdtLab] = ProcessStage.NDT,
            [ProcessSite.CincinnatiMrb] = ProcessStage.MRB,
            [ProcessSite.BristolFai] = ProcessStage.FAI,
        };

    /// <summary>Sites in the lifecycle order a part normally visits.</summary>
    public static IReadOnlyList<ProcessSite> InLifecycleOrder { get; } =
    [
        ProcessSite.OhioForge,
        ProcessSite.NagoyaHeatTreat,
        ProcessSite.StuttgartMachining,
        ProcessSite.StuttgartCmmLab,
        ProcessSite.ToulouseNdtLab,
        ProcessSite.CincinnatiMrb,
        ProcessSite.BristolFai,
    ];

    /// <summary>Human-readable name suitable for UI surfaces.</summary>
    public static string GetDisplayName(ProcessSite site) =>
        DisplayNames.TryGetValue(site, out var name) ? name : site.ToString();

    /// <summary>The lifecycle stage this site is primarily associated with.</summary>
    public static ProcessStage GetPrimaryStage(ProcessSite site) =>
        PrimaryStages.TryGetValue(site, out var stage) ? stage : ProcessStage.Forge;
}
