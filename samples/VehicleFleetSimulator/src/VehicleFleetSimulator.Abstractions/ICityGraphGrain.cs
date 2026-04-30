namespace VehicleFleetSimulator.Abstractions;

/// <summary>Read-only access to the silo's loaded city graph. Single instance keyed by <see cref="Key"/>.</summary>
public interface ICityGraphGrain : IGrainWithGuidKey
{
    public static readonly Guid Key = Guid.Empty;

    /// <summary>Return a snapshot of all cities, bidirectional road segments, and any persisted
    /// per-city position overrides supplied by the UI's drag-to-move tool.</summary>
    Task<CityGraphSnapshot> GetGraph();

    /// <summary>Persist a 2-D position for a single city. Subsequent <see cref="GetGraph"/> calls
    /// will return the override in <see cref="CityGraphSnapshot.PositionOverrides"/>. Returns
    /// <c>false</c> if the city id is not part of the loaded graph.</summary>
    Task<bool> SetCityPosition(string cityId, double x, double y);

    /// <summary>Discard every position override and revert to the client's computed layout.</summary>
    Task ClearCityPositions();
}
