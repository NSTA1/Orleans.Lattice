namespace VehicleFleetSimulator.Abstractions;

[GenerateSerializer]
public enum VehicleStatus
{
    Idle = 0,
    Driving = 1,
    Refuelling = 2,
    RouteCompleted = 3,
}
