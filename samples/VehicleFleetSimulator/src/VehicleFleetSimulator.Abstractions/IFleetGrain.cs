namespace VehicleFleetSimulator.Abstractions;

/// <summary>
/// Singleton coordination grain for the entire fleet. Use <see cref="Key"/> as the grain key.
/// </summary>
public interface IFleetGrain : IGrainWithGuidKey
{
    public static readonly Guid Key = Guid.Empty;

    /// <summary>Add a single vehicle. Returns the assigned (or pre-supplied) vehicle id.</summary>
    /// <param name="spec">Vehicle specification.</param>
    /// <param name="onDuplicate">Behaviour when <paramref name="spec"/> carries an id already in the fleet.</param>
    Task<Guid> AddVehicle(VehicleSpec spec, DuplicateVehiclePolicy onDuplicate = DuplicateVehiclePolicy.Throw);

    /// <summary>Add many vehicles in chunks with bounded concurrency. Returns the assigned ids in order.</summary>
    /// <param name="specs">Vehicle specifications.</param>
    /// <param name="onDuplicate">Behaviour for each spec whose id is already in the fleet.</param>
    Task<IReadOnlyList<Guid>> AddVehicleBatch(IReadOnlyList<VehicleSpec> specs, DuplicateVehiclePolicy onDuplicate = DuplicateVehiclePolicy.Throw);

    /// <summary>Remove a vehicle (stops it and forgets the id). Returns true if the vehicle was known.</summary>
    Task<bool> RemoveVehicle(Guid vehicleId);

    /// <summary>Remove every vehicle in the fleet (stops each and clears the roster). Returns the number removed.</summary>
    Task<int> RemoveAllVehicles();

    /// <summary>List all known vehicle ids.</summary>
    Task<IReadOnlyList<Guid>> ListVehicles();

    /// <summary>Aggregate fleet stats by walking each vehicle grain.</summary>
    Task<FleetStats> GetFleetStats();

    /// <summary>Issue <see cref="IVehicleGrain.Start"/> to every persisted vehicle. Returns the
    /// number of vehicles successfully started. Mirrors <see cref="RemoveAllVehicles"/>'s
    /// bounded-concurrency fan-out so timing on large rosters stays inside the Orleans default
    /// response timeout.</summary>
    Task<int> StartAllVehicles();

    /// <summary>Issue <see cref="IVehicleGrain.Stop"/> to every persisted vehicle. Returns the
    /// number of vehicles successfully stopped. Vehicles remain in the roster (use
    /// <see cref="RemoveAllVehicles"/> to also forget them).</summary>
    Task<int> StopAllVehicles();
}
