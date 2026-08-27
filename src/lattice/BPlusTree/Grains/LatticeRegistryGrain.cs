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
/// <see cref="Orleans.Lattice.BPlusTree.State.TreeRegistryEntry"/>. The registry tree itself uses the
/// <see cref="LatticeConstants.SystemTreePrefix"/> and is excluded from
/// self-registration to avoid circular bootstrap.
/// </para>
/// </summary>
internal sealed class LatticeRegistryGrain(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ITreePlacementResolver? placementResolver = null,
    TreeAliasObserverDispatcher? aliasObservers = null) : ILatticeRegistry
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
        seeded = await ApplyRegistrationWalPlacementAsync(treeId, seeded);
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
    /// Seeds the tree's durable WAL placement pin from the physical placement the
    /// <see cref="ITreePlacementResolver"/> seam resolves for a newly registered
    /// tree. Runs only on first registration (callers reach here past the
    /// already-registered idempotency guard), so a tree's physical placement is
    /// immutable for its lifetime: re-registration never re-resolves, and a later
    /// change to a tenant's placement binding does not re-place trees that already
    /// exist (a migration would require data movement, out of scope for v1).
    /// <para>
    /// When the resolver reports the baseline key (which is every tree when tenancy
    /// is off, and every shared / non-tenant tree when it is on), the entry is
    /// returned unchanged with a <c>null</c> <see cref="TreeRegistryEntry.WalPlacement"/>,
    /// so routing is byte-for-byte identical to pre-placement behaviour and the
    /// default-key path in <see cref="LatticeOptionsResolver"/> still honours any
    /// legacy per-tree <see cref="LatticeOptions.WalStorageProvider"/> resolver. A
    /// non-baseline key pins every partition to the dedicated provider by seeding the
    /// pin's default key; the existing catalog machinery then routes the tree's WAL
    /// shards there and fails closed (via <see cref="LatticeWalProviderMissingException"/>)
    /// if the key is absent on a silo.
    /// </para>
    /// </summary>
    private async ValueTask<TreeRegistryEntry> ApplyRegistrationWalPlacementAsync(
        string treeId, TreeRegistryEntry seeded)
    {
        // No resolver (tenancy off in a direct-construction context) or a
        // caller-supplied explicit placement pin: leave the entry untouched. The
        // resolver only seeds the INITIAL pin for a tree that has none.
        if (placementResolver is null || seeded.WalPlacement is not null)
        {
            return seeded;
        }

        if (!placementResolver.TryResolveForRegistration(treeId, out var placement))
        {
            placement = await placementResolver
                .ResolveForRegistrationAsync(treeId);
        }

        var key = placement.WalProviderKey;
        if (string.IsNullOrEmpty(key) ||
            string.Equals(key, IWalStorageProviderCatalog.DefaultProviderKey, StringComparison.Ordinal))
        {
            // Baseline placement: behaviour byte-for-byte identical to a cluster
            // with no per-tenant placement.
            return seeded;
        }

        // Pin every partition to the dedicated provider by seeding the pin's default
        // key. Version 0 marks an initial seed rather than a managed move; the pin is
        // thereafter mutated only through the ILatticeAdmin move surface.
        return seeded with
        {
            WalPlacement = WalPlacementPin.Create() with { DefaultProviderKey = key },
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

    public Task<IReadOnlyList<string>> GetAllTreeIdsAsync() => GetAllTreeIdsAsync(prefix: null);

    public async Task<IReadOnlyList<string>> GetAllTreeIdsAsync(string? prefix)
    {
        // The registry tree is ordinally sorted, so a prefix is one contiguous key
        // range: scanning [prefix, PrefixUpperBound(prefix)) stops the walk
        // touching pages outside the range entirely, rather than reading every key
        // and discarding most of them. PrefixUpperBound returns null when the range
        // is unbounded above (an empty prefix, or one of only U+FFFF), which
        // KeysAsync reads as "no end bound" - the correct degenerate behaviour.
        var scoped = !string.IsNullOrEmpty(prefix);
        var start = scoped ? prefix : null;
        var end = scoped ? LatticeKeyRange.PrefixUpperBound(prefix!) : null;

        var keys = new List<string>();
        await foreach (var key in Registry.KeysAsync(start, end))
        {
            // The reserved system-tree namespace is never part of the catalog,
            // whether or not the scan was scoped. Kept inside the loop so a
            // caller-supplied prefix can never widen the enumeration.
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

        // Fire the alias-change observer only on an effective physical-identity
        // change so a live shipper can rebind reactively (event-driven) instead
        // of polling the registry every pump tick. An unaliased tree resolves to
        // its own id, so the old effective physical is the prior alias or the
        // logical id itself; a no-op re-set of the same alias is suppressed.
        var oldPhysical = existing.PhysicalTreeId ?? treeId;
        if (aliasObservers is { HasObservers: true }
            && !string.Equals(oldPhysical, physicalTreeId, StringComparison.Ordinal))
        {
            await aliasObservers.PublishAsync(new TreeAliasChange
            {
                TreeId = treeId,
                OldPhysicalTreeId = oldPhysical,
                NewPhysicalTreeId = physicalTreeId,
            });
        }
    }

    public async Task RemoveAliasAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var existing = await GetEntryAsync(treeId);
        if (existing?.PhysicalTreeId is null) return;

        var oldPhysical = existing.PhysicalTreeId;
        var updated = existing with { PhysicalTreeId = null };
        await UpdateAsync(treeId, updated);

        // Removing an alias repoints the logical tree back to itself; the new
        // effective physical id is the logical id. The early-return above
        // guarantees an actual change (a stored alias always differs from the
        // logical id), so this always fires when observers are present.
        if (aliasObservers is { HasObservers: true })
        {
            await aliasObservers.PublishAsync(new TreeAliasChange
            {
                TreeId = treeId,
                OldPhysicalTreeId = oldPhysical,
                NewPhysicalTreeId = treeId,
            });
        }
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

    public async Task SetMaxCacheValueBytesAsync(string treeId, long? maxCacheValueBytes)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (maxCacheValueBytes is { } cap && cap < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxCacheValueBytes), cap,
                $"{nameof(LatticeOptions.MaxCacheValueBytes)} must be greater than or equal to 1 when set "
                + "(null leaves the read-through cache mirror unbounded; a positive value caps the resident "
                + "value-payload bytes per cache activation with LRU payload eviction).");
        }

        var existing = await GetEntryAsync(treeId) ?? new TreeRegistryEntry();
        var updated = existing with { MaxCacheValueBytes = maxCacheValueBytes };
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
/// Source-generated JSON context for <see cref="Orleans.Lattice.BPlusTree.State.TreeRegistryEntry"/> serialization.
/// </summary>
[JsonSerializable(typeof(TreeRegistryEntry))]
internal sealed partial class RegistryEntryContext : JsonSerializerContext;
