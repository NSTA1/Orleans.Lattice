namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Singleton grain tracking whether cross-cluster HTTP replication is
/// currently paused by the <c>ReplicationDisconnect</c> chaos preset.
/// Both the outbound <c>ReplicatorGrain.TickAsync</c> loop and the
/// inbound <c>POST /replicate/{tree}</c> endpoint consult this flag —
/// when set, outbound ship is a no-op and inbound returns 503. The
/// local replog keeps growing while disconnected; on clear, replication
/// resumes from the current cursor.
/// </summary>
public interface IReplicationDisconnectGrain : IGrainWithIntegerKey
{
    /// <summary>Fixed integer key used to address the singleton grain.</summary>
    public const long SingletonKey = 0;

    /// <summary>Returns whether cross-cluster replication is currently paused.</summary>
    Task<bool> IsDisconnectedAsync();

    /// <summary>Sets the disconnect flag and returns the resulting value.</summary>
    Task<bool> SetDisconnectedAsync(bool disconnected);
}

/// <summary>
/// Trivial implementation — a single boolean flag, persisted so a silo
/// restart doesn''t accidentally "heal" the replication pause without an
/// operator asking for it.
/// </summary>
internal sealed class ReplicationDisconnectGrain(
    [PersistentState(stateName: "state", storageName: "msmfgGrainState")]
    IPersistentState<ReplicationDisconnectState> state) : Grain, IReplicationDisconnectGrain
{
    public Task<bool> IsDisconnectedAsync() => Task.FromResult(state.State.IsDisconnected);

    public async Task<bool> SetDisconnectedAsync(bool disconnected)
    {
        if (state.State.IsDisconnected != disconnected)
        {
            state.State.IsDisconnected = disconnected;
            await state.WriteStateAsync();
        }
        return state.State.IsDisconnected;
    }
}

/// <summary>Persistent state for <see cref="ReplicationDisconnectGrain"/>.</summary>
[GenerateSerializer]
public sealed record ReplicationDisconnectState
{
    /// <summary>True when cross-cluster replication is paused by the chaos preset.</summary>
    [Id(0)] public bool IsDisconnected { get; set; }
}
