namespace VehicleFleetSimulator.Abstractions;

/// <summary>
/// Client-side observer that receives a stream of fleet telemetry and discrete events relayed
/// from the silo's <c>FleetFanOutGrain</c>. Implementations must be tolerant of out-of-order or
/// duplicate items.
/// </summary>
public interface IFleetStreamObserver : IGrainObserver
{
    Task OnTelemetry(VehicleTelemetryEvent telemetry);
    Task OnEvent(VehicleEvent vehicleEvent);
}
