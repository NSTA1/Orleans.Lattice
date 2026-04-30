namespace VehicleFleetSimulator.Grains.Cities;

/// <summary>Provides the configured <see cref="CityGraph"/> for the running silo.</summary>
public interface ICityGraphProvider
{
    CityGraph Graph { get; }
}

public sealed class StaticCityGraphProvider(CityGraph graph) : ICityGraphProvider
{
    public CityGraph Graph { get; } = graph;
}
