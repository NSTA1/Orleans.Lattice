using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>A named, reproducible curated load profile that the API can apply with a single call.
/// Scenarios live in <see cref="ScenarioCatalog"/>; the API surfaces them under
/// <c>POST /api/scenarios/{name}</c>.</summary>
[GenerateSerializer, Immutable]
public sealed record ScenarioPreset(
    [property: Id(0)] string Name,
    [property: Id(1)] string DisplayName,
    [property: Id(2)] string Description,
    [property: Id(3)] int VehicleCount,
    [property: Id(4)] string? StartCityId,
    [property: Id(5)] bool ResetFleetFirst);

/// <summary>Static catalogue of well-known scenarios. Add new presets by appending to <see cref="All"/>.</summary>
/// <remarks>
/// Scenarios are intentionally declarative metadata + a fleet count, not a list of pre-baked
/// VehicleSpec objects: the simulator's own random route generator is stable per silo, so a
/// preset that pins exact routes would drift the moment the city graph changes. Keeping each
/// preset to "N vehicles starting at city X" means presets stay valid as the graph evolves.
/// </remarks>
public static class ScenarioCatalog
{
    public static readonly ImmutableArray<ScenarioPreset> All = ImmutableArray.Create(
        new ScenarioPreset(
            Name: "small",
            DisplayName: "Small fleet",
            Description: "Ten vehicles, random starts, useful for quick visual checks.",
            VehicleCount: 10,
            StartCityId: null,
            ResetFleetFirst: true),

        new ScenarioPreset(
            Name: "iberian-rush-hour",
            DisplayName: "Iberian rush hour",
            Description: "100 vehicles all departing Madrid; floods the southern radials.",
            VehicleCount: 100,
            StartCityId: "MAD",
            ResetFleetFirst: true),

        new ScenarioPreset(
            Name: "city-stress",
            DisplayName: "Single-city stress",
            Description: "500 vehicles starting from Madrid; load test for a single hub.",
            VehicleCount: 500,
            StartCityId: "MAD",
            ResetFleetFirst: true),

        new ScenarioPreset(
            Name: "convoy",
            DisplayName: "Coast-to-coast convoy",
            Description: "20 vehicles departing London; long routes across the continent.",
            VehicleCount: 20,
            StartCityId: "LON",
            ResetFleetFirst: true),

        new ScenarioPreset(
            Name: "load",
            DisplayName: "Continuous load",
            Description: "1,000 vehicles, random starts. Replaces the LoadHarness CLI default.",
            VehicleCount: 1000,
            StartCityId: null,
            ResetFleetFirst: true),

        new ScenarioPreset(
            Name: "high-load",
            DisplayName: "High load",
            Description: "5,000 vehicles, random starts. Stress test for fan-out and rendering.",
            VehicleCount: 5000,
            StartCityId: null,
            ResetFleetFirst: true));

    public static bool TryGet(string name, out ScenarioPreset preset)
    {
        foreach (var p in All)
        {
            if (string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                preset = p;
                return true;
            }
        }
        preset = default!;
        return false;
    }
}
