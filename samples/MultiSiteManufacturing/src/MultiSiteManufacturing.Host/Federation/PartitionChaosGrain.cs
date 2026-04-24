namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Singleton grain tracking whether the M12c silo-partition chaos
/// preset is currently active. Writes go to this grain from the chaos
/// fly-out / preset dispatcher; the <see cref="FederationRouter"/>
/// caches the value and re-reads on ChaosConfigChanged.
/// </summary>
public interface IPartitionChaosGrain : IGrainWithIntegerKey
{
    /// <summary>Fixed integer key used to address the singleton grain.</summary>
    public const long SingletonKey = 0;

    /// <summary>Returns whether a cluster-split partition is currently in effect.</summary>
    Task<bool> IsPartitionedAsync();

    /// <summary>Sets the partition flag and returns the resulting value.</summary>
    Task<bool> SetPartitionedAsync(bool partitioned);
}

/// <summary>
/// Trivial implementation — a single boolean flag, persisted so a silo
/// restart doesn''t accidentally "heal" the partition without an
/// operator asking for it.
/// </summary>
internal sealed class PartitionChaosGrain(
    [PersistentState(stateName: "state", storageName: "msmfgGrainState")]
    IPersistentState<PartitionChaosState> state) : Grain, IPartitionChaosGrain
{
    public Task<bool> IsPartitionedAsync() => Task.FromResult(state.State.IsPartitioned);

    public async Task<bool> SetPartitionedAsync(bool partitioned)
    {
        if (state.State.IsPartitioned != partitioned)
        {
            state.State.IsPartitioned = partitioned;
            await state.WriteStateAsync();
        }
        return state.State.IsPartitioned;
    }
}

/// <summary>Persistent state for <see cref="PartitionChaosGrain"/>.</summary>
[GenerateSerializer]
public sealed record PartitionChaosState
{
    /// <summary>True when the silo-partition simulation is active.</summary>
    [Id(0)] public bool IsPartitioned { get; set; }
}

