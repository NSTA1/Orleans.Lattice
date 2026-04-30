namespace VehicleFleetSimulator.Abstractions;

/// <summary>
/// Per-tick telemetry snapshot emitted by a vehicle grain to its stream. A value type so that the
/// 1000 msg/sec producer path doesn't allocate on the heap — pooling a record class would require
/// a return-to-pool hook after the Orleans memory stream consumer drains it, which the provider
/// doesn't expose. As a struct it travels through the stream by value, copied once into the
/// memory stream's serialization buffer (pooled internally) with zero GC pressure on the producer.
/// </summary>
[GenerateSerializer, Immutable]
public readonly record struct VehicleTelemetryEvent(
    [property: Id(0)] Guid VehicleId,
    [property: Id(1)] DateTimeOffset TimestampUtc,
    [property: Id(2)] string FromCityId,
    [property: Id(3)] string ToCityId,
    [property: Id(4)] double SegmentProgressKm,
    [property: Id(5)] double SegmentLengthKm,
    [property: Id(6)] double SpeedKph,
    [property: Id(7)] double FuelLitres,
    [property: Id(8)] VehicleStatus Status,
    // Tank capacity travels alongside the current fuel reading so consumers that need a
    // fuel fraction (e.g. the UI's colour gradient) don't have to reverse-engineer it from a
    // running max of observed FuelLitres values, which is wrong on the very first frame after
    // a fresh subscriber attaches and only self-corrects after a refuel completes. Defaulted
    // to 0 so test call sites that don't care about the gradient stay source-compatible; the
    // grain populates it from VehicleConfig.FuelCapacityLitres on every publish.
    [property: Id(9)] double FuelCapacityLitres = 0);
