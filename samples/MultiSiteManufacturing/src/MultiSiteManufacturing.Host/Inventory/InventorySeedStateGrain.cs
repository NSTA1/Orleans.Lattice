namespace MultiSiteManufacturing.Host.Inventory;

/// <summary>Persistent state for <see cref="IInventorySeedStateGrain"/>.</summary>
[GenerateSerializer]
public sealed class InventorySeedState
{
    /// <summary><c>true</c> once <see cref="InventorySeeder"/> has populated the inventory.</summary>
    [Id(0)] public bool HasSeeded { get; set; }
}

/// <inheritdoc cref="IInventorySeedStateGrain"/>
public sealed class InventorySeedStateGrain(
    [PersistentState("state", "msmfgGrainState")] IPersistentState<InventorySeedState> state)
    : Grain, IInventorySeedStateGrain
{
    /// <inheritdoc />
    public Task<bool> HasSeededAsync() => Task.FromResult(state.State.HasSeeded);

    /// <inheritdoc />
    public async Task<bool> TryMarkSeededAsync()
    {
        if (state.State.HasSeeded)
        {
            return false;
        }
        state.State.HasSeeded = true;
        await state.WriteStateAsync();
        return true;
    }
}
