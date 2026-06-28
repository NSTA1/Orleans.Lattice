using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

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
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : ILatticeRegistry
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

        // The existence check is also used by the DIAG block below; keep
        // the call outside the directive so foreground behaviour is
        // identical whether or not LATTICE_DIAG is defined.
        var existsAtCall = await Registry.ExistsAsync(treeId);
#if LATTICE_DIAG
        // DIAG-PATH1: log every entry into RegisterAsync.
        try
        {
            DiagSink.Write(
                $"RegisterAsync entry treeId={treeId} exists={existsAtCall} incoming={(entry is null ? "null" : $"{{mlk={entry.MaxLeafKeys},mic={entry.MaxInternalChildren},sc={entry.ShardCount}}}")}");
        }
        catch { }
#endif

        // Idempotent - if already registered, preserve existing config.
        if (existsAtCall)
            return;

        // Seed the structural sizing pin from LatticeConstants so every tree
        // has an unambiguous, immutable structural identity from the moment
        // it is first registered. After seeding, the registry is the only
        // source of structural truth; IOptionsMonitor<LatticeOptions> no
        // longer exposes these fields. ResizeAsync / ReshardAsync are the
        // only legitimate mutation paths. System trees are intentionally
        // not special-cased - they use the same defaults so their leaves,
        // internals, and shard maps share the same invariants as user
        // trees.
        var seeded = SeedStructuralDefaults(entry, optionsMonitor.Get(treeId).WalPartitions);
#if LATTICE_DIAG
        try
        {
            DiagSink.Write(
                $"RegisterAsync seeding treeId={treeId} seeded={{mlk={seeded.MaxLeafKeys},mic={seeded.MaxInternalChildren},sc={seeded.ShardCount},wp={seeded.WalPartitions}}}");
        }
        catch { }
#endif

        var bytes = SerializeEntry(seeded);
        await Registry.SetAsync(treeId, bytes);
    }

    private static TreeRegistryEntry SeedStructuralDefaults(TreeRegistryEntry? entry, int siloDefaultWalPartitions)
    {
        entry ??= new TreeRegistryEntry();
        return entry with
        {
            MaxLeafKeys = entry.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = entry.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = entry.ShardCount ?? LatticeConstants.DefaultShardCount,
            // WalPartitions is pinned at first-register from the
            // silo's then-current LatticeOptions.WalPartitions. Once
            // stamped the value is tree-immutable - LatticeOptionsResolver
            // reads from this slot in preference to the live options-
            // monitor value so the foreground commit-log writer and
            // the activation-time materialiser always agree on the
            // partition fan-out shape for the lifetime of the tree.
            WalPartitions = entry.WalPartitions ?? siloDefaultWalPartitions,
        };
    }

    /// <summary>
    /// Rejects user-supplied tree IDs whose names collide with the library's
    /// reserved system-tree namespace. The <see cref="LatticeConstants.SystemTreePrefix"/>
    /// check is the umbrella guard - it subsumes
    /// <see cref="LatticeConstants.WalTreePrefix"/> and the registry tree
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
                $"'{LatticeConstants.WalTreePrefix}' prefix used by Orleans.Lattice.Replication). " +
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

    public async Task SetHistoryRetentionAsync(string treeId, HistoryRetentionMode? mode, TimeSpan? window)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        HistoryRetentionValidator.Validate(mode, window);

        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var updated = existing with
        {
            HistoryRetentionMode = mode,
            HistoryRetentionWindowTicks = window?.Ticks,
        };
        await UpdateAsync(treeId, updated);
    }

    public async Task SetMaintainProjectionDigestAsync(string treeId, bool? enabled)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var updated = existing with { MaintainProjectionDigest = enabled };
        await UpdateAsync(treeId, updated);
    }

    public async Task LatchProjectionDigestPermanentlyDisabledAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        if (existing.ProjectionDigestPermanentlyDisabled == true)
        {
            // Idempotent: latch is one-way and re-stamping is a no-op.
            // Skipping the write avoids unnecessary registry churn on
            // every mutation funnel after the first.
            return;
        }
        var updated = existing with { ProjectionDigestPermanentlyDisabled = true };
        await UpdateAsync(treeId, updated);
    }

    public async Task<WalPlacementPin> GetWalPlacementAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var entry = await GetEntryAsync(treeId);
        return entry?.WalPlacement ?? WalPlacementPin.Create();
    }

    public async Task<WalPlacementPin> UpdateWalPlacementAsync(string treeId, long expectedVersion, int partition, string providerKey)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentException.ThrowIfNullOrEmpty(providerKey);

        // Atomic read-validate-write: the registry grain is non-reentrant and
        // singleton-keyed, so the compare-and-swap below cannot interleave with
        // a concurrent placement change.
        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var current = existing.WalPlacement ?? WalPlacementPin.Create();
        if (current.Version != expectedVersion)
        {
            throw new InvalidOperationException(
                $"WAL placement for tree '{treeId}' changed concurrently: expected version {expectedVersion} but found {current.Version}. Re-read the placement and retry.");
        }

        var updatedPin = current.WithPartition(partition, providerKey, expectedVersion + 1);
        var updatedEntry = existing with { WalPlacement = updatedPin };
        await UpdateAsync(treeId, updatedEntry);
        return updatedPin;
    }

    public async Task<WalPlacementPin> UpdateWalPlacementAsync(string treeId, long expectedVersion, IReadOnlyCollection<(int Partition, string ProviderKey)> moves)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(moves);
        if (moves.Count == 0)
        {
            throw new ArgumentException("A batch WAL placement update must contain at least one move.", nameof(moves));
        }
        foreach (var (_, providerKey) in moves)
        {
            ArgumentException.ThrowIfNullOrEmpty(providerKey, nameof(moves));
        }

        // Atomic read-validate-write: the registry grain is non-reentrant and
        // singleton-keyed, so the compare-and-swap below applies every move under
        // one version bump with no intermediate placement observable.
        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var current = existing.WalPlacement ?? WalPlacementPin.Create();
        if (current.Version != expectedVersion)
        {
            throw new InvalidOperationException(
                $"WAL placement for tree '{treeId}' changed concurrently: expected version {expectedVersion} but found {current.Version}. Re-read the placement and retry.");
        }

        var updatedPin = current.WithPartitions(moves, expectedVersion + 1);
        var updatedEntry = existing with { WalPlacement = updatedPin };
        await UpdateAsync(treeId, updatedEntry);
        return updatedPin;
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
