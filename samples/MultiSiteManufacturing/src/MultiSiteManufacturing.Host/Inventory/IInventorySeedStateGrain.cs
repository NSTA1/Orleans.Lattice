namespace MultiSiteManufacturing.Host.Inventory;

/// <summary>
/// Singleton grain that persists whether the bulk-load seeder has already
/// populated the inventory against the current Azure Table Storage account.
/// Keyed with <see cref="SingletonKey"/>. When the <c>HasSeeded</c> flag is
/// set, <see cref="InventorySeeder"/> skips seeding on subsequent host
/// starts so operator mutations are preserved across restarts - provided
/// the lattice data the flag guards still exists. If the durable flag
/// outlives the (non-durable) lattice trees across an abrupt restart, the
/// seeder re-seeds rather than trusting the flag alone.
/// </summary>
public interface IInventorySeedStateGrain : IGrainWithIntegerKey
{
    /// <summary>Fixed integer key used to address the singleton grain.</summary>
    public const long SingletonKey = 0;

    /// <summary>Reads the current seed flag.</summary>
    Task<bool> HasSeededAsync();

    /// <summary>Atomically checks-and-sets the flag. Returns <c>true</c> if the caller is the first to set it.</summary>
    Task<bool> TryMarkSeededAsync();
}
