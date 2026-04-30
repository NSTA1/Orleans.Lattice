using System.Collections.Immutable;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Cities;

namespace VehicleFleetSimulator.Grains.Vehicles;

/// <summary>
/// Pure tick function that advances <see cref="VehicleState"/>. No I/O, no Orleans dependencies —
/// this is the unit-testable heart of the simulation.
/// </summary>
public static class VehicleSimulator
{
    /// <summary>
    /// Construct the initial state for a new vehicle from a <see cref="VehicleSpec"/>.
    /// Generates a random route via <paramref name="routeGenerator"/> when none is supplied.
    /// </summary>
    public static VehicleState CreateInitialState(
        VehicleSpec spec,
        CityGraph graph,
        RouteGenerator routeGenerator,
        DateTimeOffset nowUtc)
    {
        ArgumentNullException.ThrowIfNull(spec);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(routeGenerator);

        var config = spec.Config ?? VehicleConfig.Default;

        ImmutableArray<string> route;
        if (spec.Route is { } explicitRoute && explicitRoute.Length >= 2)
        {
            if (!graph.IsRouteValid(explicitRoute))
                throw new ArgumentException("Supplied route is invalid for the city graph.", nameof(spec));
            route = explicitRoute;
        }
        else
        {
            route = routeGenerator.Generate(spec.StartCityId);
        }

        var fuel = spec.InitialFuelLitres ?? config.FuelCapacityLitres;
        fuel = Math.Clamp(fuel, 0.0, config.FuelCapacityLitres);

        return new VehicleState(
            VehicleId: spec.VehicleId ?? Guid.NewGuid(),
            Route: route,
            RouteIndex: 0,
            SegmentProgressKm: 0,
            SpeedKph: 0,
            FuelLitres: fuel,
            Status: VehicleStatus.Driving,
            LastUpdatedUtc: nowUtc,
            RefuellingUntilUtc: null,
            SpeedTimeSinceResample: TimeSpan.Zero);
    }

    /// <summary>Advance the vehicle by <paramref name="elapsed"/>, returning the new state and any discrete events.</summary>
    public static TickResult Tick(
        in VehicleState state,
        VehicleConfig config,
        CityGraph graph,
        RouteGenerator routeGenerator,
        TimeSpan elapsed,
        DateTimeOffset nowUtc,
        Random random)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(routeGenerator);
        ArgumentNullException.ThrowIfNull(random);

        if (elapsed <= TimeSpan.Zero)
            return new TickResult(state with { LastUpdatedUtc = nowUtc }, Array.Empty<VehicleEvent>());

        // Allocate the events list only when we actually have something to add. The vast majority
        // of ticks (steady-state driving along a segment) add zero events.
        List<VehicleEvent>? events = null;
        var current = state;

        // 1. Resolve refuelling completion before doing anything else.
        if (current.Status == VehicleStatus.Refuelling)
        {
            // Defensive: a Refuelling vehicle with no deadline (legacy state from a previous code
            // path, or a config that ended up with RefuelDelay=0) would otherwise sit forever in
            // this branch. Treat null-until as "start the timer now" so the vehicle eventually
            // self-heals on the next tick.
            if (current.RefuellingUntilUtc is null)
            {
                current = current with { RefuellingUntilUtc = nowUtc + config.RefuelDelay };
                return new TickResult(current with { LastUpdatedUtc = nowUtc }, Array.Empty<VehicleEvent>());
            }

            if (current.RefuellingUntilUtc is { } until && nowUtc >= until)
            {
                current = current with
                {
                    FuelLitres = config.FuelCapacityLitres,
                    Status = VehicleStatus.Driving,
                    RefuellingUntilUtc = null,
                };
                (events ??= new List<VehicleEvent>(2)).Add(
                    new RefuellingCompleted(current.VehicleId, nowUtc, current.CurrentFromCityId, current.FuelLitres));
            }
            else
            {
                return new TickResult(current with { LastUpdatedUtc = nowUtc }, Array.Empty<VehicleEvent>());
            }
        }

        if (current.Status == VehicleStatus.RouteCompleted || current.Status == VehicleStatus.Idle)
        {
            return new TickResult(current with { LastUpdatedUtc = nowUtc },
                events?.ToArray() ?? Array.Empty<VehicleEvent>());
        }

        // 2. Determine the current segment length.
        if (!graph.TryGetDistance(current.CurrentFromCityId, current.CurrentToCityId, out var segmentLengthKm))
        {
            // Should not happen if state was constructed via valid routes; treat as completed.
            return new TickResult(current with { Status = VehicleStatus.RouteCompleted, LastUpdatedUtc = nowUtc },
                events?.ToArray() ?? Array.Empty<VehicleEvent>());
        }

        // 3. Pre-departure fuel check when leaving a city (progress == 0).
        if (current.SegmentProgressKm <= 0.0)
        {
            var required = FuelModel.FuelRequired(segmentLengthKm, config);

            // Defensive: if the next segment can't be completed even on a full tank, no amount of
            // refuelling will let the vehicle leave. That's a deadlock signature (graph edge longer
            // than the tank's worst-case range). Regenerate the route from the current city so the
            // vehicle picks an alternative path; emit a RouteCompleted event so observers see the
            // forced reroute rather than a silent state mutation.
            if (required > config.FuelCapacityLitres)
            {
                var stuckCity = current.CurrentFromCityId;
                var oldRoute = current.Route;
                var newRoute = routeGenerator.Generate(stuckCity);
                (events ??= new List<VehicleEvent>(2)).Add(
                    new RouteCompleted(current.VehicleId, nowUtc, oldRoute, newRoute));
                current = current with
                {
                    Route = newRoute,
                    RouteIndex = 0,
                    SegmentProgressKm = 0,
                    SpeedKph = 0,
                    SpeedTimeSinceResample = TimeSpan.Zero,
                    Status = VehicleStatus.Driving,
                    LastUpdatedUtc = nowUtc,
                };
                return new TickResult(current, events.ToArray());
            }

            if (current.FuelLitres < required)
            {
                var until = nowUtc + config.RefuelDelay;
                current = current with
                {
                    Status = VehicleStatus.Refuelling,
                    RefuellingUntilUtc = until,
                    SpeedKph = 0,
                    LastUpdatedUtc = nowUtc,
                };
                (events ??= new List<VehicleEvent>(2)).Add(
                    new RefuellingStarted(current.VehicleId, nowUtc, current.CurrentFromCityId, required, state.FuelLitres));
                return new TickResult(current, events.ToArray());
            }

            // Emit DepartedCity at the moment we begin the segment.
            (events ??= new List<VehicleEvent>(2)).Add(
                new DepartedCity(current.VehicleId, nowUtc, current.CurrentFromCityId, current.CurrentToCityId));
        }

        // 4. Update target/smoothed speed.
        var resampleAccum = current.SpeedTimeSinceResample + elapsed;
        double newSpeed = current.SpeedKph;
        if (current.SpeedKph <= 0 || resampleAccum >= config.SpeedResampleInterval)
        {
            var target = SpeedModel.SampleTarget(current.SegmentProgressKm, segmentLengthKm, config, random);
            newSpeed = current.SpeedKph <= 0
                ? target
                : SpeedModel.Smooth(current.SpeedKph, target, config.SpeedSmoothingAlpha);
            resampleAccum = TimeSpan.Zero;
        }
        newSpeed = Math.Clamp(newSpeed, config.MinSpeedKph, config.MaxSpeedKph);

        // 5. Advance position. Average between previous and new speed for stability.
        var avgSpeed = (current.SpeedKph + newSpeed) * 0.5;
        if (current.SpeedKph <= 0) avgSpeed = newSpeed;
        var hours = elapsed.TotalHours;
        var distanceTravelled = avgSpeed * hours;
        var fuelBurned = FuelModel.LitresPerKm(avgSpeed, config) * distanceTravelled;
        var newProgress = current.SegmentProgressKm + distanceTravelled;
        var newFuel = Math.Max(0.0, current.FuelLitres - fuelBurned);

        current = current with
        {
            SegmentProgressKm = newProgress,
            SpeedKph = newSpeed,
            FuelLitres = newFuel,
            SpeedTimeSinceResample = resampleAccum,
            LastUpdatedUtc = nowUtc,
        };

        // 6. Handle arrival. We zero `SegmentProgressKm` on the new segment rather than carrying
        //    overshoot forward; the discontinuity is invisible to the renderer (which interpolates
        //    by segment proportion) and avoids the next segment starting deep inside its own
        //    near-city window when the previous segment's last tick travelled further than the
        //    remaining distance.
        if (newProgress >= segmentLengthKm)
        {
            var arrivedCityId = current.CurrentToCityId;
            (events ??= new List<VehicleEvent>(2)).Add(new ArrivedCity(current.VehicleId, nowUtc, arrivedCityId));

            var nextIndex = current.RouteIndex + 1;
            if (nextIndex >= current.Route.Length - 1)
            {
                // Arrived at the final city of the route → regenerate from the current city.
                var completedRoute = current.Route;
                var newRoute = routeGenerator.Generate(arrivedCityId);
                events.Add(new RouteCompleted(current.VehicleId, nowUtc, completedRoute, newRoute));
                current = current with
                {
                    Route = newRoute,
                    RouteIndex = 0,
                    SegmentProgressKm = 0,
                    SpeedKph = 0,
                    SpeedTimeSinceResample = TimeSpan.Zero,
                    Status = VehicleStatus.Driving,
                };
            }
            else
            {
                // Pass-through arrival at an intermediate city: zero SpeedKph so the next tick
                // takes the `current.SpeedKph <= 0 ? target : Smooth(...)` branch in step 4 and
                // lands directly on a fresh target sampled from the IsNearCity [1..40] kph range
                // (since SegmentProgressKm == 0). This is the visible "pause at cities" -- a brief
                // dip in speed as the vehicle eases through the waypoint before accelerating back
                // to cruise once it leaves the proximity window.
                current = current with
                {
                    RouteIndex = nextIndex,
                    SegmentProgressKm = 0,
                    SpeedKph = 0,
                    SpeedTimeSinceResample = TimeSpan.Zero,
                };
            }
        }

        return new TickResult(current,
            events?.ToArray() ?? Array.Empty<VehicleEvent>());
    }
}

public readonly record struct TickResult(VehicleState State, VehicleEvent[] Events);
