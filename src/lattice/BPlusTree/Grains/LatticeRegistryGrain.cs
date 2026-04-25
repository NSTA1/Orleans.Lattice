using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Singleton grain that manages the tree registry backed by the internal
/// <see cref="LatticeConstants.RegistryTreeId"/> Lattice tree.
/// <para>
/// Each user tree ID is stored as a key; the value is a JSON-serialized
/// <see cref="TreeRegistryEntry"/>. The registry tree itself uses the
/// <see cref="LatticeConstants.SystemTreePrefix"/> and is excluded from
/// self-registration to avoid circular bootstrap.
/// </para>
/// </summary>
internal sealed class LatticeRegistryGrain(
    IGrainFactory grainFactory) : ILatticeRegistry
{
    private static readonly byte[] EmptyEntry = SerializeEntry(new TreeRegistryEntry());

    // Uses the internal ISystemLattice surface so the registry can address its
    // own backing system tree (`_lattice_trees`). The public ILattice surface
    // rejects any call targeting a reserved system-tree id and would otherwise
    // make the registry impossible to implement on top of Lattice itself.
    private ISystemLattice Registry => grainFactory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId);

    public async Task RegisterAsync(string treeId, TreeRegistryEntry? entry = null)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ThrowIfReservedPrefix(treeId, nameof(treeId));

        // Idempotent — if already registered, preserve existing config.
        if (await Registry.ExistsAsync(treeId))
            return;

        // Seed the structural sizing pin from LatticeConstants so every tree
        // has an unambiguous, immutable structural identity from the moment
        // it is first registered. After seeding, the registry is the only
        // source of structural truth; IOptionsMonitor<LatticeOptions> no
        // longer exposes these fields. ResizeAsync / ReshardAsync are the
        // only legitimate mutation paths. System trees are intentionally
        // not special-cased — they use the same defaults so their leaves,
        // internals, and shard maps share the same invariants as user
        // trees.
        var seeded = SeedStructuralDefaults(entry);

        var bytes = SerializeEntry(seeded);
        await Registry.SetAsync(treeId, bytes);
    }

    private static TreeRegistryEntry SeedStructuralDefaults(TreeRegistryEntry? entry)
    {
        entry ??= new TreeRegistryEntry();
        return entry with
        {
            MaxLeafKeys = entry.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = entry.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = entry.ShardCount ?? LatticeConstants.DefaultShardCount,
        };
    }

    /// <summary>
    /// Rejects user-supplied tree IDs whose names collide with the library's
    /// reserved system-tree namespace. The <see cref="LatticeConstants.SystemTreePrefix"/>
    /// check is the umbrella guard — it subsumes
    /// <see cref="LatticeConstants.ReplogTreePrefix"/> and the registry tree
    /// itself (<see cref="LatticeConstants.RegistryTreeId"/>). Internal
    /// callers that legitimately bootstrap system trees bypass
    /// <see cref="RegisterAsync"/> entirely, so this guard only fires on
    /// user-supplied IDs.
    /// </summary>
    private static void ThrowIfReservedPrefix(string treeId, string paramName)
    {
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new ArgumentException(
                $"Tree ID '{treeId}' is reserved: names starting with '{LatticeConstants.SystemTreePrefix}' " +
                "are reserved for internal Lattice system trees (including the " +
                $"'{LatticeConstants.ReplogTreePrefix}' prefix used by Orleans.Lattice.Replication). " +
                "Choose a tree ID that does not start with an underscore-prefixed Lattice namespace.",
                paramName);
    }

    public async Task UpdateAsync(string treeId, TreeRegistryEntry entry)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entry);
        ThrowIfReservedPrefix(treeId, nameof(treeId));

        await Registry.SetAsync(treeId, SerializeEntry(entry));
    }

    public async Task UnregisterAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        await Registry.DeleteAsync(treeId);
    }

    public async Task<bool> ExistsAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return await Registry.ExistsAsync(treeId);
    }

    public async Task<TreeRegistryEntry?> GetEntryAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var bytes = await Registry.GetAsync(treeId);
        return bytes is not null ? DeserializeEntry(bytes) : null;
    }

    public async Task<IReadOnlyList<string>> GetAllTreeIdsAsync()
    {
        var keys = new List<string>();
        await foreach (var key in Registry.KeysAsync())
        {
            if (!key.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
                keys.Add(key);
        }
        return keys;
    }

    public async Task SetAliasAsync(string treeId, string physicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(physicalTreeId);

        if (string.Equals(treeId, physicalTreeId, StringComparison.Ordinal))
            throw new ArgumentException("Physical tree ID must differ from the logical tree ID.", nameof(physicalTreeId));

        // Enforce single-level indirection: the target must not itself be aliased.
        var targetEntry = await GetEntryAsync(physicalTreeId);
        if (targetEntry?.PhysicalTreeId is not null)
            throw new InvalidOperationException(
                $"Cannot set alias: target tree '{physicalTreeId}' is itself aliased to '{targetEntry.PhysicalTreeId}'. " +
                "Only a single level of indirection is supported.");

        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var updated = existing with { PhysicalTreeId = physicalTreeId };
        await UpdateAsync(treeId, updated);
    }

    public async Task RemoveAliasAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var existing = await GetEntryAsync(treeId);
        if (existing?.PhysicalTreeId is null) return;

        var updated = existing with { PhysicalTreeId = null };
        await UpdateAsync(treeId, updated);
    }

    public async Task<string> ResolveAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var entry = await GetEntryAsync(treeId);
        return entry?.PhysicalTreeId ?? treeId;
    }

    public async Task<ShardMap?> GetShardMapAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var entry = await GetEntryAsync(treeId);
        return entry?.ShardMap;
    }

    public async Task SetShardMapAsync(string treeId, ShardMap map)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(map);

        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        // Bump the map version on every persist so strongly-consistent scans
        // can detect topology changes via a single long comparison. The
        // registry grain is non-reentrant and singleton-keyed, so the
        // get-modify-set sequence is atomic across concurrent split
        // coordinators.
        var previousVersion = existing.ShardMap?.Version ?? 0L;
        map.Version = previousVersion + 1;
        var updated = existing with { ShardMap = map };
        await UpdateAsync(treeId, updated);
    }

    public async Task<int> AllocateNextShardIndexAsync(string treeId, int currentMaxFromMap)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // Atomic read-modify-write: this grain is non-reentrant and is a
        // singleton (keyed by RegistryTreeId), so the entire method body runs
        // without interleaving across concurrent callers, guaranteeing each
        // split coordinator receives a distinct target shard index.
        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var floor = Math.Max(existing.NextShardIndex ?? -1, currentMaxFromMap);
        var allocated = floor + 1;
        var updated = existing with { NextShardIndex = allocated };
        await UpdateAsync(treeId, updated);
        return allocated;
    }

    public async Task SetPublishEventsAsync(string treeId, bool? enabled)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var updated = existing with { PublishEvents = enabled };
        await UpdateAsync(treeId, updated);
    }

    private static byte[] SerializeEntry(TreeRegistryEntry entry) =>
        JsonSerializer.SerializeToUtf8Bytes(entry, RegistryEntryContext.Default.TreeRegistryEntry);

    private static TreeRegistryEntry DeserializeEntry(byte[] bytes) =>
        JsonSerializer.Deserialize(bytes, RegistryEntryContext.Default.TreeRegistryEntry)!;
}

/// <summary>
/// Source-generated JSON context for <see cref="TreeRegistryEntry"/> serialization.
/// </summary>
[JsonSerializable(typeof(TreeRegistryEntry))]
internal sealed partial class RegistryEntryContext : JsonSerializerContext;
