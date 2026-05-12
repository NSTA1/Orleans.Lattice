namespace VehicleFleetSimulator.Abstractions;

/// <summary>Discrete, operator-injected vehicle malfunctions used for visual / behavioural testing.</summary>
/// <remarks>
/// Faults are intentionally coarse: they map to existing simulator state transitions rather than
/// introducing brand-new failure modes. That keeps the simulator's recovery logic (refuel,
/// reroute) responsible for clean-up - an injected <see cref="OutOfFuel"/> simply drains the
/// tank and lets the existing low-fuel handler take over.
/// </remarks>
[GenerateSerializer]
public enum VehicleFault
{
    /// <summary>Drain the tank to zero, forcing the simulator's refuel pathway on the next tick.</summary>
    OutOfFuel = 0,

    /// <summary>Force the vehicle into <see cref="VehicleStatus.Idle"/> with zero speed. The
    /// vehicle stays parked until <c>Start()</c> is invoked again -- equivalent to a roadside
    /// breakdown that requires manual intervention.</summary>
    EngineStall = 1,

    /// <summary>Replace the remaining route with a freshly generated one from the current city.
    /// Models a road closure ahead: the vehicle continues driving but along a different path.</summary>
    RouteBlock = 2,
}
