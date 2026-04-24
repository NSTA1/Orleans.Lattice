using Orleans.Runtime;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Implementation of <see cref="IBackendChaosGrain"/>. Thin wrapper
/// around a single <see cref="BackendChaosConfig"/> persisted to
/// <c>msmfgGrainState</c>.
/// </summary>
internal sealed class BackendChaosGrain(
    [PersistentState("state", "msmfgGrainState")] IPersistentState<BackendChaosStateRecord> state)
    : Grain, IBackendChaosGrain
{
    public Task<BackendChaosConfig> GetConfigAsync() =>
        Task.FromResult(state.State.Config);

    public async Task<BackendChaosConfig> ConfigureAsync(BackendChaosConfig config)
    {
        ValidateRange(config.TransientFailureRate, nameof(config.TransientFailureRate));
        ValidateRange(config.WriteAmplificationRate, nameof(config.WriteAmplificationRate));
        if (config.JitterMsMin < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(config), config.JitterMsMin, "JitterMsMin must be non-negative.");
        }
        if (config.JitterMsMax < config.JitterMsMin)
        {
            throw new ArgumentOutOfRangeException(
                nameof(config), config.JitterMsMax, "JitterMsMax must be >= JitterMsMin.");
        }
        if (config.ReorderWindowMs < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(config), config.ReorderWindowMs, "ReorderWindowMs must be non-negative.");
        }

        state.State.Config = config;
        await state.WriteStateAsync();
        return config;
    }

    private static void ValidateRange(double value, string name)
    {
        if (value is < 0.0 or > 1.0 || double.IsNaN(value))
        {
            throw new ArgumentOutOfRangeException(name, value, "Must be in [0, 1].");
        }
    }
}

/// <summary>Persistent state for <see cref="BackendChaosGrain"/>.</summary>
[GenerateSerializer]
public sealed record BackendChaosStateRecord
{
    /// <summary>Current chaos configuration.</summary>
    [Id(0)] public BackendChaosConfig Config { get; set; }
}
