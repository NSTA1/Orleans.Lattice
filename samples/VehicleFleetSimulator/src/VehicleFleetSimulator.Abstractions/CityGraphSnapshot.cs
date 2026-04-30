using System.Collections.Immutable;

namespace VehicleFleetSimulator.Abstractions;

/// <summary>Serializable snapshot of the configured city graph for API consumers.</summary>
/// <remarks>
/// <see cref="PositionOverrides"/> carries operator-supplied 2-D coordinates for cities the user
/// has dragged around in the UI. When empty (the default for a fresh silo) the client is expected
/// to fall back to its own deterministic layout. When present, the dictionary is the authoritative
/// source for those city ids only — un-overridden cities still take their layout-computed
/// positions, so a partial drag-set is rendered consistently.
/// </remarks>
[GenerateSerializer, Immutable]
public sealed record CityGraphSnapshot(
    [property: Id(0)] ImmutableArray<City> Cities,
    [property: Id(1)] ImmutableArray<RoadSegment> Edges,
    [property: Id(2)] ImmutableDictionary<string, CityPosition>? PositionOverrides = null);

/// <summary>Single city's overridden 2-D position, in the same arbitrary unit space the
/// client's layout uses (x and y are unitless and unbounded; the renderer letterboxes both).</summary>
[GenerateSerializer, Immutable]
public sealed record CityPosition(
    [property: Id(0)] double X,
    [property: Id(1)] double Y);

