using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Human-readable metadata for <see cref="ChaosPreset"/> values — used by
/// the chaos fly-out to render operator-friendly labels and tooltips for
/// each canned preset button (plan §7.2).
/// </summary>
public static class ChaosPresetInfo
{
    private static readonly IReadOnlyDictionary<ChaosPreset, (string DisplayName, string Description)> Info =
        new Dictionary<ChaosPreset, (string, string)>
        {
            [ChaosPreset.ClearAll] = (
                "Clear all",
                "Reset every site and backend to nominal. Any queued facts are released."),
            [ChaosPreset.TransoceanicBackhaulOutage] = (
                "Transoceanic backhaul outage",
                "Pause Stuttgart CMM Lab + Toulouse NDT Lab and add 4s of delay — models a transatlantic WAN outage."),
            [ChaosPreset.CustomsHold] = (
                "Customs hold",
                "Add 8s of delay at Nagoya Heat Treatment — models a shipment stuck at customs."),
            [ChaosPreset.MrbWeekend] = (
                "MRB weekend",
                "Pause Cincinnati MRB entirely — models the MRB being closed over a long weekend."),
            [ChaosPreset.LatticeStorageFlakes] = (
                "Lattice storage flakes",
                "Apply a 10% transient failure rate and 50–250 ms jitter to the lattice backend only — drives baseline-vs-lattice divergence."),
            [ChaosPreset.BaselineReorderStorm] = (
                "Baseline reorder storm",
                "Open a 300 ms reorder window on the baseline backend only — writes flush in shuffled arrival order. Pair with the per-row ⚠ Race button to force divergence (baseline flags, lattice stays Nominal via HLC-ordered fold)."),
        };

    /// <summary>All presets in declaration order (suitable for button rows).</summary>
    public static IReadOnlyList<ChaosPreset> All { get; } = Enum.GetValues<ChaosPreset>();

    /// <summary>Short label suitable for a button.</summary>
    public static string GetDisplayName(ChaosPreset preset) =>
        Info.TryGetValue(preset, out var info) ? info.DisplayName : preset.ToString();

    /// <summary>One-sentence description of the real-world scenario the preset models.</summary>
    public static string GetDescription(ChaosPreset preset) =>
        Info.TryGetValue(preset, out var info) ? info.Description : string.Empty;
}
