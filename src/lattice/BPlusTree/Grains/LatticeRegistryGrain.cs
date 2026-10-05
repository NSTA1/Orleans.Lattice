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
/// <para>
/// Read-only members of <see cref="ILatticeRegistry"/> are
/// <c>[AlwaysInterleave]</c> and mutating members deliberately are not; the
/// grain type itself is not <c>[Reentrant]</c>. The full rationale - why the
/// write paths still need exclusion and why the reads are safe to admit
/// mid-body - lives on the interface, which is where the attributes are
/// declared.
/// </para>
/// </summary>
internal sealed class LatticeRegistryGrain(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ITreePlacementResolver? placementResolver = null,
    TreeAliasObserverDispatcher? aliasObservers = null,
    ILatticeAccessGate? accessGate = null,
    ILatticeMembershipContext? membership = null,
    ITreeOwnershipGuard? ownershipGuard = null,
    TreeLineageObserverDispatcher? lineageObservers = null) : ILatticeRegistry
{
    // Uses the internal ISystemLattice surface so the registry can address its
    // own backing system tree (`_lattice_trees`). The public ILattice surface
    // rejects any call targeting a reserved system-tree id and would otherwise
    // make the registry impossible to implement on top of Lattice itself.
    private ISystemLattice Registry => grainFactory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId);

    public Task RegisterAsync(string treeId, TreeRegistryEntry? entry = null)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ThrowIfReservedPrefix(treeId, nameof(treeId));

        return RegistryCallCensus.MeasureAsync(
            RegistryCallCensus.Register,
            () => RegisterCoreAsync(treeId, entry));
    }

    /// <summary>
    /// The registration itself, uninstrumented.
    /// </summary>
    /// <remarks>
    /// Split out for the same reason as <see cref="GetEntryCoreAsync"/>: the
    /// census arm must count inbound grain calls only. This member carries no
    /// <c>[AlwaysInterleave]</c>, so it holds the singleton's turn token for its
    /// whole duration - including the existence check below, which is itself a
    /// hop onto the backing system tree.
    /// </remarks>
    private async Task RegisterCoreAsync(string treeId, TreeRegistryEntry? entry)
    {        // The existence check is also used by the DIAG block below; keep
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
        if (lineageObservers is { HasObservers: true })
        {
            await lineageObservers.NotifyChangingAsync(treeId, currentLineage: null, seeded.Lineage);
        }

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
            Lineage = entry.Lineage ?? Guid.NewGuid(),
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

    /// <summary>
    /// Rejects an alias whose <paramref name="physicalTreeId"/> names a more
    /// privileged namespace than the logical <paramref name="treeId"/> it is
    /// being bound to.
    /// <para>
    /// An alias transplants a logical identity onto another tree's physical
    /// shards, and every data-plane access gate on the
    /// <see cref="Orleans.Lattice.ILattice"/> facade is evaluated
    /// against the <em>logical</em> id before the alias is resolved (the
    /// physical shard and leaf grains enforce no policy of their own). Without
    /// this guard a caller holding admin rights on any ordinary tree could
    /// alias it onto <c>sys-</c>-prefixed authorization, membership or tenant
    /// registry state, or onto another tenant's namespace, and then read and
    /// rewrite that state through the facade - the gates would only ever see
    /// the ordinary logical id.
    /// </para>
    /// <para>
    /// The rule is namespace-preserving rather than a flat deny-list, so
    /// internal maintenance flows that derive the physical id from the logical
    /// one (tree resize's <c>{treeId}/resized/{operationId}</c>, schema
    /// remediation's <c>{treeId}/remediated/{operationId}</c>, and backup's
    /// shadow-restore target) stay legal for system and tenant trees alike.
    /// System-origin callers are exempt: they are library-internal maintenance
    /// paths that have already been gated at their own entry point.
    /// </para>
    /// </summary>
    private static void ThrowIfAliasEscalatesNamespace(string treeId, string physicalTreeId)
    {
        if (LatticeAccessGateContext.IsSystemOrigin)
            return;

        // The logical id can never live in the reserved internal namespace
        // (UpdateAsync rejects it), so an alias into it is always an escalation.
        if (physicalTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Physical tree ID '{physicalTreeId}' is reserved: an alias may not target the " +
                $"'{LatticeConstants.SystemTreePrefix}' internal namespace.",
                nameof(physicalTreeId));
        }

        if (physicalTreeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
            && !treeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Physical tree ID '{physicalTreeId}' is reserved: an alias from the ordinary tree " +
                $"'{treeId}' may not target the '{LatticeConstants.SystemDataTreePrefix}' system-data " +
                "namespace, because access is authorized against the logical tree ID.",
                nameof(physicalTreeId));
        }

        if (LatticeTenantTrees.IsTenantScoped(physicalTreeId))
        {
            var targetKnown = LatticeTenantTrees.TryGetTenant(physicalTreeId, out var target);
            var ownerKnown = LatticeTenantTrees.TryGetTenant(treeId, out var owner);
            if (!targetKnown
                || !ownerKnown
                || !string.Equals(owner.Value, target.Value, StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    $"Physical tree ID '{physicalTreeId}' belongs to a tenant namespace that does not own " +
                    $"the logical tree '{treeId}'. An alias may not cross a tenant boundary, because " +
                    "access is authorized against the logical tree ID.",
                    nameof(physicalTreeId));
            }
        }
    }

    public async Task UpdateAsync(string treeId, TreeRegistryEntry entry)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entry);
        ThrowIfReservedPrefix(treeId, nameof(treeId));

        // Observers of a lineage change are told before it is persisted (#4537),
        // and a failing observer aborts the write.
        if (lineageObservers is { HasObservers: true })
        {
            var current = (await GetEntryCoreAsync(treeId))?.Lineage;
            await lineageObservers.NotifyChangingAsync(treeId, current, entry.Lineage);
        }

        await Registry.SetAsync(treeId, SerializeEntry(entry));
    }

    public Task UnregisterAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return RegistryCallCensus.MeasureAsync(
            RegistryCallCensus.Unregister,
            () => UnregisterCoreAsync(treeId));
    }

    private async Task UnregisterCoreAsync(string treeId)
    {
        if (lineageObservers is { HasObservers: true }
            && (await GetEntryCoreAsync(treeId))?.Lineage is { } current)
        {
            await lineageObservers.NotifyChangingAsync(treeId, current, nextLineage: null);
        }

        await Registry.DeleteAsync(treeId);
    }

    public Task<bool> ExistsAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return RegistryCallCensus.MeasureAsync(
            RegistryCallCensus.Exists,
            () => Registry.ExistsAsync(treeId));
    }

    public Task<TreeRegistryEntry?> GetEntryAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return RegistryCallCensus.MeasureAsync(
            RegistryCallCensus.GetEntry,
            () => GetEntryCoreAsync(treeId));
    }

    /// <summary>
    /// The entry read itself, uninstrumented.
    /// <para>
    /// Every in-grain caller goes through this rather than through
    /// <see cref="GetEntryAsync"/>, so the <c>get_entry</c> arm of the registry
    /// census counts inbound grain calls only. Routing an internal caller through
    /// the public member instead would attribute that caller's read to
    /// <c>get_entry</c> as well as to its own arm, double-counting one admitted
    /// call and nesting one duration inside another.
    /// </para>
    /// </summary>
    private async Task<TreeRegistryEntry?> GetEntryCoreAsync(string treeId)
    {
        var bytes = await Registry.GetAsync(treeId);
        return bytes is not null ? DeserializeEntry(bytes) : null;
    }

    /// <summary>
    /// Reads the row a non-create verb is about to rewrite, failing closed when
    /// there is none (issue #4230). Such a verb used to upsert from an empty
    /// <see cref="TreeRegistryEntry"/>, so a late split, a leaf latch, or a
    /// configuration change against a never-registered or purged id created a
    /// row with no structural pins - for a purged id, silently undoing the
    /// purge. Only <see cref="RegisterAsync"/>, <see cref="UpdateAsync"/> and
    /// <see cref="SetAliasAsync"/> may create a row. No await beyond the backing
    /// read is added to the registry turn.
    /// </summary>
    private async Task<TreeRegistryEntry> GetRegisteredEntryCoreAsync(string treeId, string operation) =>
        await GetEntryCoreAsync(treeId) ?? throw new LatticeTreeNotRegisteredException(treeId, operation);

    public Task<Dictionary<string, TreeRegistryEntry>> GetEntriesAsync(IReadOnlyList<string> treeIds)
    {
        ArgumentNullException.ThrowIfNull(treeIds);
        if (treeIds.Count == 0)
        {
            // No registry hop at all for an empty page: there is nothing to read,
            // so the fan-out below would be pure overhead. Not censused either -
            // a call that reaches no backing read is not fan-in, and recording it
            // would put a zero-cost sample in a histogram read for saturation.
            return Task.FromResult(new Dictionary<string, TreeRegistryEntry>(0, StringComparer.Ordinal));
        }

        return RegistryCallCensus.MeasureAsync(
            RegistryCallCensus.GetEntries,
            () => GetEntriesCoreAsync(treeIds));
    }

    private async Task<Dictionary<string, TreeRegistryEntry>> GetEntriesCoreAsync(IReadOnlyList<string> treeIds)
    {        // One concurrent wave of the same single-key read GetEntryAsync issues,
        // rather than ISystemLattice.GetManyAsync. That looks like the obvious
        // primitive but it was unsafe from here: LatticeGrain.GetManyAsyncCore
        // ends every attempt with an unconditional topology-stability re-probe
        // (`registry.GetShardMapAsync(TreeId)`) against ILatticeRegistry. Called
        // from inside this grain that closes a two-hop cycle back onto this
        // activation while it is still executing this turn. (Since #3180 that
        // re-probe is [AlwaysInterleave] and so would no longer queue behind
        // us, but the single-key read has never had the re-probe at all, which
        // is why the per-entry GetEntryAsync path has always worked from here -
        // and it costs nothing to keep, so the cycle stays closed by
        // construction rather than by one attribute on another method.)
        // Awaiting the whole wave keeps the caller-visible win (one round-trip
        // for a page instead of one per entry) and collapses the registry-side
        // cost from N sequential awaits to a single parallel wave; only the
        // shard-level grouping is given up, and that is silo-internal.
        var reads = new Task<byte[]?>[treeIds.Count];
        for (var i = 0; i < treeIds.Count; i++)
        {
            var treeId = treeIds[i];
            ArgumentNullException.ThrowIfNull(treeId);
            reads[i] = Registry.GetAsync(treeId);
        }

        await Task.WhenAll(reads);

        // Absent and tombstoned ids read back as null and are simply left out,
        // which is the "unregistered ids are absent" contract this method
        // publishes. Later duplicates overwrite earlier ones with an identical
        // value, so a caller passing a duplicated id still gets one entry.
        var entries = new Dictionary<string, TreeRegistryEntry>(treeIds.Count, StringComparer.Ordinal);
        for (var i = 0; i < reads.Length; i++)
        {
            var bytes = await reads[i];
            if (bytes is not null)
            {
                entries[treeIds[i]] = DeserializeEntry(bytes);
            }
        }

        return entries;
    }

    public async Task<IReadOnlyList<string>> GetAliasesTargetingAsync(string physicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(physicalTreeId);

        // Control-plane scan: avoid a second durable index whose write could
        // diverge from the alias entry. The non-interleaved turn excludes mutators.
        List<string>? aliases = null;
        await foreach (var row in Registry.ScanEntriesAsync())
        {
            if (string.Equals(DeserializeEntry(row.Value).PhysicalTreeId, physicalTreeId, StringComparison.Ordinal))
            {
                (aliases ??= []).Add(row.Key);
            }
        }

        return aliases is null ? Array.Empty<string>() : aliases;
    }

    public Task<IReadOnlyList<string>> GetAllTreeIdsAsync() => GetAllTreeIdsAsync(prefix: null);

    public Task<IReadOnlyList<string>> GetAllTreeIdsAsync(string? prefix) =>
        RegistryCallCensus.MeasureAsync(
            RegistryCallCensus.GetAllTreeIds,
            () => GetAllTreeIdsCoreAsync(prefix));

    private async Task<IReadOnlyList<string>> GetAllTreeIdsCoreAsync(string? prefix)
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
        // ScanKeysAsync, not the raw KeysAsync primitive. The registry tree is
        // backed by LatticeGrain, which is [StatelessWorker], so a MoveNext can
        // be routed to a sibling worker activation that holds no state for this
        // enumerator and the scan aborts - a steady-state background rate that
        // rises with concurrency, not a rare failover event, and one a
        // single-page scan is fully exposed to. The wrapper reopens and resumes
        // from the successor of the last yielded key, so the catalog it returns
        // has no duplicates and no gaps.
        await foreach (var key in Registry.ScanKeysAsync(start, end))
        {
            // The reserved system-tree namespace is never part of the catalog,
            // whether or not the scan was scoped. Kept inside the loop so a
            // caller-supplied prefix can never widen the enumeration.
            if (!key.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
                keys.Add(key);
        }
        return keys;
    }

    /// <summary>
    /// Requires the caller to hold whole-tree control of an alias's
    /// <paramref name="physicalTreeId"/> target before the alias is written.
    /// <para>
    /// <see cref="ThrowIfAliasEscalatesNamespace"/> closes the
    /// <em>namespace</em> half of the alias escalation: it refuses a target in
    /// the reserved internal namespace, an ordinary-to-system-data crossing,
    /// and a foreign-tenant target. It cannot close the remaining half, because
    /// two ordinary same-namespace tree ids are indistinguishable to it and so
    /// pass unconditionally. That is the whole of the hole: a caller authorized
    /// on the ordinary tree <c>a</c> could bind it to the equally ordinary tree
    /// <c>b</c> owned by somebody else, and thereafter read and rewrite every
    /// key of <c>b</c> through <c>a</c> - because routing resolves the alias
    /// and addresses <c>b</c>'s shards directly, while every data-plane gate on
    /// the facade has already been evaluated against the logical id <c>a</c>.
    /// </para>
    /// <para>
    /// Ownership is not a namespace property, so it is answered by the same
    /// component that answers it everywhere else: the access gate, consulted
    /// against the target tree. The bar is whole-tree control rather than a
    /// per-key allow, because an alias confers unrestricted read and write over
    /// every key the target holds, now and in future - a key-filtered allow is
    /// therefore refused (<see cref="LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync"/>).
    /// </para>
    /// <para>
    /// A no-op on a host that registered no gate (the default
    /// <c>NullLatticeAccessGate</c>), and on a system-origin turn - the
    /// library-internal maintenance flows that derive a physical id from the
    /// logical one (resize, resharding, schema remediation, shadow restore) are
    /// already gated at their own entry points, which is the same exemption
    /// <see cref="ThrowIfAliasEscalatesNamespace"/> takes.
    /// </para>
    /// </summary>
    private ValueTask EnsureAliasTargetIsControlledAsync(string physicalTreeId) =>
        accessGate is null
            ? ValueTask.CompletedTask
            : LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
                accessGate,
                membership,
                physicalTreeId,
                LatticeOperation.Admin,
                CancellationToken.None);

    public async Task SetAliasAsync(string treeId, string physicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(physicalTreeId);

        if (string.Equals(treeId, physicalTreeId, StringComparison.Ordinal))
            throw new ArgumentException("Physical tree ID must differ from the logical tree ID.", nameof(physicalTreeId));

        await EnsureAliasTargetAdmissibleAsync(treeId, physicalTreeId);

        var existing = await GetEntryCoreAsync(treeId) ?? new TreeRegistryEntry();
        // Writing the alias completes any cutover that carried the target's map
        // onto this entry first, so its in-progress marker is cleared with it.
        var updated = existing with
        {
            PhysicalTreeId = physicalTreeId,
            AliasCutoverTarget = null,
            Lineage = string.Equals(existing.PhysicalTreeId, physicalTreeId, StringComparison.Ordinal)
                ? existing.Lineage
                : Guid.NewGuid(),
        };
        await UpdateAsync(treeId, updated);
        await PublishAliasChangeAsync(treeId, existing.PhysicalTreeId ?? treeId, physicalTreeId);
    }

    public async Task<State.TreeRegistryEntry?> SwapAliasAsync(string treeId, string physicalTreeId, ShardMap shardMap, int? nextShardIndex, string? expectedPhysicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(physicalTreeId);
        ArgumentNullException.ThrowIfNull(shardMap);

        // Every check runs before anything is written, so a refused swap leaves
        // both the alias and the map exactly as they were.
        var removing = string.Equals(treeId, physicalTreeId, StringComparison.Ordinal);
        TreeRegistryEntry? existingRow;
        if (removing)
        {
            await grainFactory.GetGrain<ITreeDeletionGrain>(treeId).EnsureAliasWritableAsync();

            // Moving a tree back onto its own shards rewrites a row that must
            // already exist; an upsert here would recreate a purged tree (#4270).
            existingRow = await GetEntryCoreAsync(treeId)
                ?? throw new LatticeTreeNotRegisteredException(treeId, nameof(SwapAliasAsync));
        }
        else
        {
            await EnsureAliasTargetAdmissibleAsync(treeId, physicalTreeId);
            existingRow = await GetEntryCoreAsync(treeId);
        }

        var existing = existingRow ?? new TreeRegistryEntry();

        // Expected-state fence: the caller read the copy it is replacing outside
        // this turn, so a swap racing another alias change must not overwrite it.
        var replacedPhysical = existing.PhysicalTreeId ?? treeId;
        if (expectedPhysicalTreeId is not null
            && !string.Equals(replacedPhysical, expectedPhysicalTreeId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Cannot swap the alias of tree '{treeId}': it now resolves to '{replacedPhysical}', "
                + $"not the expected '{expectedPhysicalTreeId}'. Another alias change moved it; re-read and retry.");
        }

        // The alias and the map routing pairs with it are one row, written once:
        // a reader resolving routing either before or after this write sees a
        // physical tree together with the map that describes its shards, never
        // one copy addressed by another copy's map. The map is re-versioned above
        // both the row's current map and the supplied one, so every cached router
        // and every map-version scan guard observes the change.
        var updated = existing with
        {
            PhysicalTreeId = removing ? null : physicalTreeId,
            ShardMap = new ShardMap
            {
                Slots = (int[])shardMap.Slots.Clone(),
                Version = Math.Max(existing.ShardMap?.Version ?? 0L, shardMap.Version) + 1,
            },
            NextShardIndex = nextShardIndex,
            AliasCutoverTarget = null,
        };
        await UpdateAsync(treeId, updated);
        await PublishAliasChangeAsync(treeId, existing.PhysicalTreeId ?? treeId, physicalTreeId);

        // The row as it stood before the swap: its map is the final layout of the
        // physical tree the alias just left, because a split or fold bound to that
        // tree is refused once the alias moves off it (#4264).
        return existingRow;
    }

    /// <summary>
    /// Refuses an alias of <paramref name="treeId"/> onto
    /// <paramref name="physicalTreeId"/> that would escalate the caller's
    /// privilege, write onto a tree being deleted, nest a second level of
    /// indirection, or that the ownership provider denies. Writes nothing.
    /// </summary>
    private async Task EnsureAliasTargetAdmissibleAsync(string treeId, string physicalTreeId)
    {
        // The alias target is caller-supplied and is never re-authorized
        // downstream: routing resolves it and addresses its shards directly,
        // while every access gate on the facade has already been evaluated
        // against the logical id. Refuse a target that would raise the caller's
        // effective privilege before anything is written.
        ThrowIfAliasEscalatesNamespace(treeId, physicalTreeId);
        await EnsureAliasTargetIsControlledAsync(physicalTreeId);

        // Enforce single-level indirection: the target must not itself be aliased.
        var targetEntry = await GetEntryCoreAsync(physicalTreeId);
        await grainFactory.GetGrain<ITreeDeletionGrain>(treeId).EnsureAliasWritableAsync();
        await grainFactory.GetGrain<ITreeDeletionGrain>(physicalTreeId).EnsureAliasWritableAsync();
        if (targetEntry?.DerivedFrom is { } owner && owner != treeId)
            await grainFactory.GetGrain<ITreeDeletionGrain>(owner).EnsureAliasWritableAsync();
        if (targetEntry?.PhysicalTreeId is not null)
            throw new InvalidOperationException(
                $"Cannot set alias: target tree '{physicalTreeId}' is itself aliased to '{targetEntry.PhysicalTreeId}'. " +
                "Only a single level of indirection is supported.");

        // Ownership is independent of caller authorization: maintenance must
        // not bypass it merely because it carries system origin.
        var ownership = await (ownershipGuard ?? NullTreeOwnershipGuard.Instance)
            .AuthorizeAliasAsync(treeId, physicalTreeId, targetEntry?.DerivedFrom);
        if (!ownership.Allowed)
            throw new LatticeTreeOwnershipDeniedException(
                ownership.Reason ?? "The ownership provider did not allow this alias.");
    }

    /// <summary>
    /// Fires the alias-change observer only on an effective physical-identity
    /// change so a live shipper can rebind reactively (event-driven) instead of
    /// polling the registry every pump tick. An unaliased tree resolves to its
    /// own id, so the old effective physical is the prior alias or the logical id
    /// itself; a no-op re-set of the same alias is suppressed.
    /// </summary>
    private async Task PublishAliasChangeAsync(string treeId, string oldPhysicalTreeId, string newPhysicalTreeId)
    {
        if (aliasObservers is { HasObservers: true }
            && !string.Equals(oldPhysicalTreeId, newPhysicalTreeId, StringComparison.Ordinal))
        {
            await aliasObservers.PublishAsync(new TreeAliasChange
            {
                TreeId = treeId,
                OldPhysicalTreeId = oldPhysicalTreeId,
                NewPhysicalTreeId = newPhysicalTreeId,
            });
        }
    }

    public async Task RemoveAliasAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        await grainFactory.GetGrain<ITreeDeletionGrain>(treeId).EnsureAliasWritableAsync();

        var existing = await GetEntryCoreAsync(treeId);
        if (existing?.PhysicalTreeId is null) return;

        var oldPhysical = existing.PhysicalTreeId;

        // The logical tree now serves its own shards, not the alias target's: new
        // content lineage, as for an alias set to a different tree (#4537).
        var updated = existing with { PhysicalTreeId = null, AliasCutoverTarget = null, Lineage = Guid.NewGuid() };
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

    public Task<string> ResolveAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        return RegistryCallCensus.MeasureAsync(RegistryCallCensus.Resolve, async () =>
        {
            var entry = await GetEntryCoreAsync(treeId);
            return entry?.PhysicalTreeId ?? treeId;
        });
    }

    public Task<ShardMap?> GetShardMapAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        return RegistryCallCensus.MeasureAsync(RegistryCallCensus.GetShardMap, async () =>
        {
            var entry = await GetEntryCoreAsync(treeId);
            return entry?.ShardMap;
        });
    }

    public async Task SetShardMapAsync(string treeId, ShardMap map)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(map);

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(SetShardMapAsync));
        // Bump the map version on every persist so strongly-consistent scans
        // can detect topology changes via a single long comparison.
        //
        // Note this method replaces the map wholesale. A caller that needs to
        // apply a *diff* onto the live map must not build that diff from a
        // separate GetShardMapAsync call and persist it here: non-reentrancy
        // serialises each individual mutating call, not a sequence of two, so a
        // concurrent coordinator can persist between the caller's read and its
        // write and have its reassignment erased. Use ReassignSlotsAsync,
        // which performs the whole read-modify-write inside one call.
        var previousVersion = existing.ShardMap?.Version ?? 0L;
        map.Version = previousVersion + 1;
        var updated = existing with { ShardMap = map };
        await UpdateAsync(treeId, updated);
    }

    public async Task<ShardMap> ReassignSlotsAsync(
        string treeId,
        int[] slots,
        int targetShardIndex,
        ShardMap fallbackMap)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(slots);
        ArgumentNullException.ThrowIfNull(fallbackMap);

        return (await ReassignSlotsCoreAsync(treeId, slots, targetShardIndex, fallbackMap, boundPhysicalTreeId: null))!;
    }

    public Task<ShardMap?> ReassignSlotsAsync(
        string treeId,
        int[] slots,
        int targetShardIndex,
        ShardMap fallbackMap,
        string boundPhysicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(slots);
        ArgumentNullException.ThrowIfNull(fallbackMap);
        ArgumentNullException.ThrowIfNull(boundPhysicalTreeId);

        return ReassignSlotsCoreAsync(treeId, slots, targetShardIndex, fallbackMap, boundPhysicalTreeId);
    }

    private async Task<ShardMap?> ReassignSlotsCoreAsync(
        string treeId,
        int[] slots,
        int targetShardIndex,
        ShardMap fallbackMap,
        string? boundPhysicalTreeId)
    {
        // Atomic read-modify-write: this grain is a singleton (keyed by
        // RegistryTreeId) and this method carries no [AlwaysInterleave], so the
        // entire method body runs without another mutator interleaving. Both
        // the read of the live map and the persist of the reassigned copy are
        // inside that body, which is what lets a concurrent split and fold
        // compose: each applies its own slot diff onto whatever the other has
        // already committed, rather than onto a view that has since gone stale.
        // Read-only members are [AlwaysInterleave] and may be admitted mid-body;
        // that is harmless, because the entry is rewritten by the single
        // terminal SetAsync below, so a reader sees it wholly before or wholly
        // after.
        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(ReassignSlotsAsync));

        // The same exclusivity makes the fence a real check-and-write: no alias
        // cutover can carry another tree's map onto this entry between the check
        // and the persist below (issue #4264).
        if (boundPhysicalTreeId is not null && !ShardMapCommitFence.Admits(existing, treeId, boundPhysicalTreeId))
        {
            return null;
        }

        var currentMap = existing.ShardMap ?? fallbackMap;
        var newSlots = (int[])currentMap.Slots.Clone();
        foreach (var slot in slots)
        {
            if (slot < 0 || slot >= newSlots.Length)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(slots),
                    slot,
                    $"Virtual slot is outside the shard map's {newSlots.Length} slots.");
            }

            newSlots[slot] = targetShardIndex;
        }

        var reassigned = new ShardMap
        {
            Slots = newSlots,
            Version = (existing.ShardMap?.Version ?? 0L) + 1,
        };

        // A reassignment can drop a physical index from the map - a fold
        // retires its donor this way - and the split allocator derives the next
        // index from the highest one the map still references. Raise the
        // allocation high-water to cover every index this map referenced, so a
        // retired shard, which keeps a routing tombstone, is never handed out
        // again as a split target.
        var highestReferenced = currentMap.GetPhysicalShardIndices() is { Count: > 0 } referenced
            ? referenced[referenced.Count - 1]
            : -1;
        var highWater = Math.Max(existing.NextShardIndex ?? -1, highestReferenced);
        int? nextShardIndex = highWater >= 0 ? highWater : existing.NextShardIndex;

        await UpdateAsync(treeId, existing with { ShardMap = reassigned, NextShardIndex = nextShardIndex });
        return reassigned;
    }

    public async Task<int> AllocateNextShardIndexAsync(string treeId, int currentMaxFromMap)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // Atomic read-modify-write: this grain is a singleton (keyed by
        // RegistryTreeId) and this method carries no [AlwaysInterleave], so the
        // entire method body runs without another mutator interleaving,
        // guaranteeing each split coordinator receives a distinct target shard
        // index.
        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(AllocateNextShardIndexAsync));
        var floor = Math.Max(existing.NextShardIndex ?? -1, currentMaxFromMap);
        var allocated = floor + 1;
        var updated = existing with { NextShardIndex = allocated };
        await UpdateAsync(treeId, updated);
        return allocated;
    }

    public async Task SetPublishEventsAsync(string treeId, bool? enabled)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(SetPublishEventsAsync));
        var updated = existing with { PublishEvents = enabled };
        await UpdateAsync(treeId, updated);
    }

    public async Task SetHistoryRetentionAsync(string treeId, HistoryRetentionMode? mode, TimeSpan? window)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        HistoryRetentionValidator.Validate(mode, window);

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(SetHistoryRetentionAsync));
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

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(SetMaintainProjectionDigestAsync));
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

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(SetMaxCacheValueBytesAsync));
        var updated = existing with { MaxCacheValueBytes = maxCacheValueBytes };
        await UpdateAsync(treeId, updated);
    }

    public async Task SetWalMaxRetainedBytesAsync(string treeId, long? walMaxRetainedBytes)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (walMaxRetainedBytes is { } ceiling && ceiling < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(walMaxRetainedBytes), ceiling,
                $"{nameof(LatticeOptions.WalMaxRetainedBytes)} must be greater than or equal to 1 when set "
                + "(null disables the advisory byte-pressure policy for this tree; a positive value sets the "
                + "retained-byte ceiling the WAL garbage collector evaluates byte pressure against).");
        }

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(SetWalMaxRetainedBytesAsync));
        var updated = existing with { WalMaxRetainedBytes = walMaxRetainedBytes };
        await UpdateAsync(treeId, updated);
    }

    public async Task LatchProjectionDigestPermanentlyDisabledAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(LatchProjectionDigestPermanentlyDisabledAsync));
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
        var entry = await GetEntryCoreAsync(treeId);
        return entry?.WalPlacement ?? WalPlacementPin.Create();
    }

    public async Task<WalPlacementPin> UpdateWalPlacementAsync(string treeId, long expectedVersion, int partition, string providerKey)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentException.ThrowIfNullOrEmpty(providerKey);

        // Atomic read-validate-write: the registry grain is singleton-keyed and
        // this method carries no [AlwaysInterleave], so the compare-and-swap
        // below cannot interleave with a concurrent placement change.
        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(UpdateWalPlacementAsync));
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

        // Atomic read-validate-write: the registry grain is singleton-keyed and
        // this method carries no [AlwaysInterleave], so the compare-and-swap
        // below applies every move under one version bump with no intermediate
        // placement observable.
        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(UpdateWalPlacementAsync));
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

    public async Task<WalPlacementPin> RaiseWalMoveFencesAsync(
        string treeId, long expectedVersion, IReadOnlyCollection<int> partitions, string moveId, TimeSpan lease, bool renew)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(partitions);
        ArgumentException.ThrowIfNullOrEmpty(moveId);
        if (partitions.Count == 0)
        {
            throw new ArgumentException("A WAL move fence must name at least one partition.", nameof(partitions));
        }
        if (lease <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(lease), lease, "A WAL move fence lease must be positive.");
        }

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(RaiseWalMoveFencesAsync));
        var current = existing.WalPlacement ?? WalPlacementPin.Create();
        if (current.Version != expectedVersion)
        {
            throw new InvalidOperationException(
                $"WAL placement for tree '{treeId}' changed concurrently: expected version {expectedVersion} but found {current.Version}. Re-read the placement and retry.");
        }

        var nowTicks = TimeProvider.System.GetUtcNow().UtcTicks;
        var expiresTicks = WalMoveFenceLeaseTicks(nowTicks, lease);
        var updated = current;
        foreach (var partition in partitions)
        {
            var sourceKey = current.ResolveKey(partition);
            var decision = WalMoveFenceCore.EvaluateRaise(current.ResolveFence(partition), sourceKey, moveId, renew, nowTicks);
            switch (decision)
            {
                case WalMoveFenceRaise.RefusedHeldByOtherMove:
                    throw new InvalidOperationException(
                        $"WAL partition {treeId}/{partition} is fenced by another placement move ('{current.ResolveFence(partition)!.MoveId}') "
                        + "whose lease has not lapsed. Wait for it to finish or lapse, then retry.");
                case WalMoveFenceRaise.RefusedReleased:
                    throw new InvalidOperationException(
                        $"WAL move '{moveId}' of {treeId}/{partition} no longer holds its fence: the lease lapsed and the fence was "
                        + "released, so the source may have accepted appends the copy has not seen. The move must abort; retry it.");
            }
            updated = updated.WithFence(partition, new WalMoveFence
            {
                MoveId = moveId,
                SourceProviderKey = sourceKey,
                LeaseExpiresUtcTicks = expiresTicks,
            });
        }

        await UpdateAsync(treeId, existing with { WalPlacement = updated });
        return updated;
    }

    public async Task<WalPlacementPin> ReleaseWalMoveFenceAsync(string treeId, int partition, string moveId, bool onlyIfExpired)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentException.ThrowIfNullOrEmpty(moveId);

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(ReleaseWalMoveFenceAsync));
        var current = existing.WalPlacement ?? WalPlacementPin.Create();
        var nowTicks = TimeProvider.System.GetUtcNow().UtcTicks;
        if (!WalMoveFenceCore.IsReleaseAdmitted(current.ResolveFence(partition), moveId, onlyIfExpired, nowTicks))
        {
            return current;
        }

        var updated = current.WithoutFence(partition);
        await UpdateAsync(treeId, existing with { WalPlacement = updated });
        return updated;
    }

    public async Task<WalPlacementPin> FlipFencedWalPlacementAsync(
        string treeId, long expectedVersion, IReadOnlyCollection<(int Partition, string ProviderKey)> moves, string moveId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(moves);
        ArgumentException.ThrowIfNullOrEmpty(moveId);
        if (moves.Count == 0)
        {
            throw new ArgumentException("A batch WAL placement update must contain at least one move.", nameof(moves));
        }
        foreach (var (_, providerKey) in moves)
        {
            ArgumentException.ThrowIfNullOrEmpty(providerKey, nameof(moves));
        }

        var existing = await GetRegisteredEntryCoreAsync(treeId, nameof(FlipFencedWalPlacementAsync));
        var current = existing.WalPlacement ?? WalPlacementPin.Create();
        if (current.Version != expectedVersion)
        {
            throw new InvalidOperationException(
                $"WAL placement for tree '{treeId}' changed concurrently: expected version {expectedVersion} but found {current.Version}. Re-read the placement and retry.");
        }
        foreach (var (partition, _) in moves)
        {
            if (!WalMoveFenceCore.IsFlipAdmitted(current.ResolveFence(partition), moveId))
            {
                throw new InvalidOperationException(
                    $"WAL move '{moveId}' of {treeId}/{partition} refused to flip: the partition no longer carries the move's "
                    + "fence, which lapsed and was released. The source may hold acknowledged appends the copy has not seen, "
                    + "so the placement was left unchanged; retry the move.");
            }
        }

        var updatedPin = current.WithPartitions(moves, expectedVersion + 1);
        await UpdateAsync(treeId, existing with { WalPlacement = updatedPin });
        return updatedPin;
    }

    /// <summary>
    /// The UTC tick at which a WAL move fence raised at <paramref name="nowTicks"/>
    /// for <paramref name="lease"/> lapses, saturating rather than overflowing for
    /// an extreme lease.
    /// </summary>
    internal static long WalMoveFenceLeaseTicks(long nowTicks, TimeSpan lease)
        => lease.Ticks >= DateTime.MaxValue.Ticks - nowTicks ? DateTime.MaxValue.Ticks : nowTicks + lease.Ticks;

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
