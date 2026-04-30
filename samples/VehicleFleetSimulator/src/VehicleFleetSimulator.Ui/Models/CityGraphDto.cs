namespace VehicleFleetSimulator.Ui.Models;

/// <summary>Subset of the API's <c>CityGraphSnapshot</c> needed to drive the renderer.</summary>
public sealed record CityDto(string Id, string Name);

public sealed record EdgeDto(string FromCityId, string ToCityId, double DistanceKm);

/// <summary>Operator-supplied 2-D coordinates for a city the user has dragged. Mirrors
/// <c>CityGraphSnapshot.PositionOverrides</c> on the API side; consumed by the renderer to
/// pre-seed dragged positions on first load so a refresh keeps the user's last layout.</summary>
public sealed record CityPositionDto(double X, double Y);

public sealed record CityGraphDto(
    IReadOnlyList<CityDto> Cities,
    IReadOnlyList<EdgeDto> Edges,
    IReadOnlyDictionary<string, CityPositionDto>? PositionOverrides = null);

