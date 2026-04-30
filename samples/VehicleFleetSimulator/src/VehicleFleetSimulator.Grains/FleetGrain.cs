using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains;

/// <summary>
/// Singleton coordination grain. Tracks every active vehicle id, brokers fleet-wide commands,
/// and aggregates simple stats. Should be addressed via <see cref="IFleetGrain.Key"/>.
/// </summary>
public sealed class FleetGrain : Grain, IFleetGrain, IRemindable
{
    private const int BatchChunkSize = 50;
    private const int BatchConcurrency = 8;
    private const string WakeReminderName = "fleet-wake";
    private static readonly TimeSpan WakePeriod = TimeSpan.FromMinutes(5);

    private readonly IPersistentState<FleetPersistentState> _persistent;
    private readonly ILogger<FleetGrain> _logger;

    public FleetGrain(
        [PersistentState("fleet", "Default")] IPersistentState<FleetPersistentState> persistent,
        ILogger<FleetGrain> logger)
    {
        _persistent = persistent;
        _logger = logger;
    }

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        // M7: register a recurring reminder so this coordinator (and through it, all vehicles)
        // is auto-resumed after silo restarts even if no client traffic arrives.
        try
        {
            var existing = await this.GetReminder(WakeReminderName);
            if (existing is null)
            {
                await this.RegisterOrUpdateReminder(WakeReminderName, WakePeriod, WakePeriod);
            }
        }
        catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException)
        {
            // Reminder service not configured (e.g. unit tests, in-memory cluster); ignore.
            _logger.LogDebug(ex, "Reminder service unavailable; skipping wake reminder registration.");
        }

        await base.OnActivateAsync(cancellationToken);
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != WakeReminderName) return;
        // Touch every persisted vehicle so its grain reactivates and its tick timer resumes.
        foreach (var id in _persistent.State.VehicleIds)
        {
            try
            {
                _ = await GrainFactory.GetGrain<IVehicleGrain>(id).GetSnapshot();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to wake vehicle {VehicleId} from reminder", id);
            }
        }
    }

    public async Task<Guid> AddVehicle(VehicleSpec spec, DuplicateVehiclePolicy onDuplicate = DuplicateVehiclePolicy.Throw)
    {
        ArgumentNullException.ThrowIfNull(spec);

        var vehicleId = spec.VehicleId ?? Guid.NewGuid();

        if (_persistent.State.VehicleIds.Contains(vehicleId))
        {
            if (onDuplicate == DuplicateVehiclePolicy.Throw)
                throw new InvalidOperationException($"Vehicle {vehicleId} is already in the fleet.");
            return vehicleId;
        }

        var effectiveSpec = spec with { VehicleId = vehicleId };
        var grain = GrainFactory.GetGrain<IVehicleGrain>(vehicleId);
        await grain.Initialize(effectiveSpec);
        await grain.Start();

        _persistent.State.VehicleIds.Add(vehicleId);
        await _persistent.WriteStateAsync();
        return vehicleId;
    }

    public async Task<IReadOnlyList<Guid>> AddVehicleBatch(IReadOnlyList<VehicleSpec> specs, DuplicateVehiclePolicy onDuplicate = DuplicateVehiclePolicy.Throw)
    {
        ArgumentNullException.ThrowIfNull(specs);
        if (specs.Count == 0) return [];

        // Pre-resolve ids and screen for duplicates against current state.
        var resolved = new (Guid Id, VehicleSpec Spec, bool IsNew)[specs.Count];
        var seenInBatch = new HashSet<Guid>();
        for (int i = 0; i < specs.Count; i++)
        {
            var s = specs[i] ?? throw new ArgumentException("Batch contains a null spec.", nameof(specs));
            var id = s.VehicleId ?? Guid.NewGuid();
            var alreadyKnown = _persistent.State.VehicleIds.Contains(id) || !seenInBatch.Add(id);
            if (alreadyKnown && onDuplicate == DuplicateVehiclePolicy.Throw)
                throw new InvalidOperationException($"Vehicle {id} is already in the fleet (or duplicated within the batch).");
            resolved[i] = (id, s with { VehicleId = id }, !alreadyKnown);
        }

        var ids = new Guid[specs.Count];
        using var throttle = new SemaphoreSlim(BatchConcurrency);

        foreach (var chunk in Chunk(resolved, BatchChunkSize))
        {
            var tasks = chunk.Select(async pair =>
            {
                await throttle.WaitAsync();
                try
                {
                    var (idx, item) = pair;
                    ids[idx] = item.Id;
                    if (!item.IsNew) return; // skip duplicates under Skip policy
                    var grain = GrainFactory.GetGrain<IVehicleGrain>(item.Id);
                    await grain.Initialize(item.Spec);
                    await grain.Start();
                }
                finally
                {
                    throttle.Release();
                }
            });
            await Task.WhenAll(tasks);
        }

        var changed = false;
        foreach (var item in resolved)
        {
            if (item.IsNew)
                changed |= _persistent.State.VehicleIds.Add(item.Id);
        }
        if (changed)
            await _persistent.WriteStateAsync();

        return ids;

        static IEnumerable<IReadOnlyList<(int Index, (Guid Id, VehicleSpec Spec, bool IsNew) Item)>> Chunk(
            (Guid Id, VehicleSpec Spec, bool IsNew)[] source, int size)
        {
            var buffer = new List<(int, (Guid, VehicleSpec, bool))>(size);
            for (int i = 0; i < source.Length; i++)
            {
                buffer.Add((i, source[i]));
                if (buffer.Count == size)
                {
                    yield return buffer;
                    buffer = new List<(int, (Guid, VehicleSpec, bool))>(size);
                }
            }
            if (buffer.Count > 0) yield return buffer;
        }
    }

    public async Task<bool> RemoveVehicle(Guid vehicleId)
    {
        var removed = _persistent.State.VehicleIds.Remove(vehicleId);
        if (removed)
        {
            try
            {
                await GrainFactory.GetGrain<IVehicleGrain>(vehicleId).Stop();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to stop vehicle {VehicleId} during remove.", vehicleId);
            }
            await _persistent.WriteStateAsync();
        }
        return removed;
    }

    public async Task<int> RemoveAllVehicles()
    {
        var ids = _persistent.State.VehicleIds.ToArray();
        if (ids.Length == 0) return 0;

        // Clear and persist the roster up-front so the call returns within Orleans' default
        // response timeout (30s) even when the caller has thousands of vehicles. The fleet-wake
        // reminder walks _persistent.State.VehicleIds, so once it's empty grains aren't re-touched.
        // The caller (API endpoint) is responsible for awaiting the per-vehicle Stop() fan-out
        // so it can report completion to its own client without hitting the per-grain-call timeout.
        _persistent.State.VehicleIds.Clear();
        await _persistent.WriteStateAsync();
        return ids.Length;
    }

    public Task<IReadOnlyList<Guid>> ListVehicles() =>
        Task.FromResult<IReadOnlyList<Guid>>(_persistent.State.VehicleIds.ToArray());

    public async Task<FleetStats> GetFleetStats()
    {
        var ids = _persistent.State.VehicleIds.ToArray();
        if (ids.Length == 0) return new FleetStats(0, 0, 0, 0, 0);

        var snapshots = await Task.WhenAll(
            ids.Select(id => GrainFactory.GetGrain<IVehicleGrain>(id).GetSnapshot().AsTask()));

        int driving = 0, refuelling = 0, idle = 0, completed = 0;
        foreach (var s in snapshots)
        {
            if (s is null) continue;
            switch (s.Status)
            {
                case VehicleStatus.Driving: driving++; break;
                case VehicleStatus.Refuelling: refuelling++; break;
                case VehicleStatus.Idle: idle++; break;
                case VehicleStatus.RouteCompleted: completed++; break;
            }
        }
        return new FleetStats(ids.Length, driving, refuelling, idle, completed);
    }

    public Task<int> StartAllVehicles() => FanOutAsync(static g => g.Start());

    public Task<int> StopAllVehicles() => FanOutAsync(static g => g.Stop());

    private async Task<int> FanOutAsync(Func<IVehicleGrain, Task> op)
    {
        var ids = _persistent.State.VehicleIds.ToArray();
        if (ids.Length == 0) return 0;

        // Bounded concurrency mirrors the AddVehicleBatch path so a 10k-vehicle bulk command
        // doesn't spam the silo with thousands of in-flight Start()/Stop() calls. Failures are
        // counted as no-op rather than thrown so a single dead grain doesn't tank the whole batch.
        using var throttle = new SemaphoreSlim(BatchConcurrency);
        int succeeded = 0;
        var tasks = ids.Select(async id =>
        {
            await throttle.WaitAsync();
            try
            {
                await op(GrainFactory.GetGrain<IVehicleGrain>(id));
                Interlocked.Increment(ref succeeded);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Bulk fleet operation failed for vehicle {VehicleId}.", id);
            }
            finally
            {
                throttle.Release();
            }
        });
        await Task.WhenAll(tasks);
        return succeeded;
    }
}
