namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable, conflict-free-mergeable definition of a single tenant: its
/// immutable <see cref="Id"/> plus last-writer-wins registers for status,
/// quotas, and placement, an LWW-element-set of tenant-admin subjects, and an
/// LWW-element-map of cross-tenant grants. Every mutating operation stamps its
/// change with a <see cref="HybridLogicalClock"/> and a writer id, and
/// <see cref="MergeFrom"/> / <see cref="Merge"/> join two records field by field
/// using the shared <see cref="TenantClock"/> total order, so concurrent updates
/// from any number of cluster replicas converge to the same record independent
/// of the order they are applied.
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantRecord)]
public sealed class TenantRecord
{
    /// <summary>The immutable identity of the tenant this record defines.</summary>
    [Id(0)]
    public TenantId Id { get; private init; }

    /// <summary>The last-writer-wins register holding the tenant's status.</summary>
    [Id(1)]
    internal TenantLwwRegister<TenantStatus> StatusRegister { get; set; }

    /// <summary>The last-writer-wins register holding the tenant's quotas.</summary>
    [Id(2)]
    internal TenantLwwRegister<TenantQuotas> QuotasRegister { get; set; }

    /// <summary>The last-writer-wins register holding the tenant's placement binding.</summary>
    [Id(3)]
    internal TenantLwwRegister<TenantPlacement> PlacementRegister { get; set; }

    /// <summary>The LWW-element-set of tenant-admin subjects, keyed by subject id.</summary>
    [Id(4)]
    internal Dictionary<string, TenantSubjectSlot> Subjects { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The LWW-element-map of cross-tenant grants, keyed by grant id.</summary>
    [Id(5)]
    internal Dictionary<string, TenantGrantSlot> GrantSlots { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// The LWW-element-set of operator-authorized allowed regions, keyed by region
    /// id. Written only by the operator tier; the tenant admin sets residency
    /// within it. Stamped independently of <see cref="RegionStatuses"/> so operator
    /// and tenant-admin writes converge without clobbering each other.
    /// </summary>
    [Id(6)]
    internal Dictionary<string, TenantRegionAllowSlot> AllowedRegions { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// The LWW-element-map of per-region lifecycle statuses, keyed by region id.
    /// Written by the tenant-admin residency operations and the backfill/drain
    /// promotion driver. A region absent from this map is
    /// <see cref="TenantRegionStatus.None"/>.
    /// </summary>
    [Id(7)]
    internal Dictionary<string, TenantRegionStatusSlot> RegionStatuses { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Parameterless constructor for the Orleans serializer.</summary>
    public TenantRecord()
    {
    }

    private TenantRecord(TenantId id) => Id = id;

    /// <summary>The tenant's resolved status.</summary>
    public TenantStatus Status => StatusRegister.Value;

    /// <summary>The tenant's resolved quotas.</summary>
    public TenantQuotas Quotas => QuotasRegister.Value;

    /// <summary>The tenant's resolved placement binding.</summary>
    public TenantPlacement Placement => PlacementRegister.Value;

    /// <summary><c>true</c> when the tenant's resolved status is <see cref="TenantStatus.Active"/>.</summary>
    public bool IsActive => Status == TenantStatus.Active;

    /// <summary><c>true</c> when the tenant's resolved status is <see cref="TenantStatus.Suspended"/>.</summary>
    public bool IsSuspended => Status == TenantStatus.Suspended;

    /// <summary>
    /// Creates a tenant record with the given definition, stamping every field
    /// with <paramref name="clock"/> and <paramref name="writerId"/>.
    /// </summary>
    /// <param name="id">The tenant identity. Must be an initialised (parsed) tenant id.</param>
    /// <param name="status">The initial status.</param>
    /// <param name="quotas">The initial quotas.</param>
    /// <param name="placement">The initial placement binding.</param>
    /// <param name="clock">The clock to stamp the initial fields with.</param>
    /// <param name="writerId">The writer id to stamp the initial fields with (may be <c>null</c>).</param>
    /// <returns>The constructed record.</returns>
    /// <exception cref="ArgumentException"><paramref name="id"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    public static TenantRecord Create(
        TenantId id,
        TenantStatus status,
        TenantQuotas quotas,
        TenantPlacement placement,
        HybridLogicalClock clock,
        string? writerId)
    {
        if (id.Value is null)
        {
            throw new ArgumentException(
                "Cannot create a tenant record for the uninitialised 'no tenant' value.",
                nameof(id));
        }

        ValidateQuotas(quotas, nameof(quotas));

        return new TenantRecord(id)
        {
            StatusRegister = TenantLwwRegister<TenantStatus>.Create(status, clock, writerId),
            QuotasRegister = TenantLwwRegister<TenantQuotas>.Create(quotas, clock, writerId),
            PlacementRegister = TenantLwwRegister<TenantPlacement>.Create(placement, clock, writerId),
        };
    }

    /// <summary>
    /// Creates the reserved <see cref="TenantId.Default"/> tenant: active, with
    /// the <see cref="TenantQuotas.Unbounded"/> quota and the
    /// <see cref="TenantPlacement.Shared"/> placement.
    /// </summary>
    /// <param name="clock">The clock to stamp the seed fields with.</param>
    /// <param name="writerId">The writer id to stamp the seed fields with (may be <c>null</c>).</param>
    /// <returns>The default tenant's record.</returns>
    public static TenantRecord CreateDefault(HybridLogicalClock clock, string? writerId) =>
        Create(
            TenantId.Default,
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            clock,
            writerId);

    /// <summary>Sets the tenant's status if the stamp supersedes the current one.</summary>
    /// <param name="status">The new status.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    public void SetStatus(TenantStatus status, HybridLogicalClock clock, string? writerId) =>
        StatusRegister = StatusRegister.Set(status, clock, writerId);

    /// <summary>Sets the tenant's quotas if the stamp supersedes the current one.</summary>
    /// <param name="quotas">The new quotas.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    public void SetQuotas(TenantQuotas quotas, HybridLogicalClock clock, string? writerId)
    {
        ValidateQuotas(quotas, nameof(quotas));
        QuotasRegister = QuotasRegister.Set(quotas, clock, writerId);
    }

    /// <summary>Sets the tenant's placement binding if the stamp supersedes the current one.</summary>
    /// <param name="placement">The new placement binding.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    public void SetPlacement(TenantPlacement placement, HybridLogicalClock clock, string? writerId) =>
        PlacementRegister = PlacementRegister.Set(placement, clock, writerId);

    /// <summary>Adds a tenant-admin subject (add-wins by stamp).</summary>
    /// <param name="subjectId">The subject id to add. Must not be <c>null</c>.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    public void AddAdminSubject(string subjectId, HybridLogicalClock clock, string? writerId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        ApplySubject(subjectId, present: true, clock, writerId);
    }

    /// <summary>Removes a tenant-admin subject (remove is a tombstone by stamp).</summary>
    /// <param name="subjectId">The subject id to remove. Must not be <c>null</c>.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    public void RemoveAdminSubject(string subjectId, HybridLogicalClock clock, string? writerId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        ApplySubject(subjectId, present: false, clock, writerId);
    }

    /// <summary>Issues or updates a cross-tenant grant (keyed by <see cref="CrossTenantGrant.GrantId"/>).</summary>
    /// <param name="grant">The grant to issue. Its <see cref="CrossTenantGrant.Grantee"/> and <see cref="CrossTenantGrant.Scope"/> must not be <c>null</c>.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    /// <exception cref="ArgumentException"><paramref name="grant"/> has a <c>null</c> grantee or scope.</exception>
    public void AddGrant(CrossTenantGrant grant, HybridLogicalClock clock, string? writerId)
    {
        if (grant.Grantee is null || grant.Scope is null)
        {
            throw new ArgumentException("A grant must have a non-null grantee and scope.", nameof(grant));
        }

        ApplyGrant(grant.GrantId, grant, present: true, clock, writerId);
    }

    /// <summary>Revokes the grant with the given id (a tombstone by stamp).</summary>
    /// <param name="grantId">The grant id to revoke. Must not be <c>null</c>.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="grantId"/> is <c>null</c>.</exception>
    public void RemoveGrant(string grantId, HybridLogicalClock clock, string? writerId)
    {
        ArgumentNullException.ThrowIfNull(grantId);
        var payload = GrantSlots.TryGetValue(grantId, out var existing) ? existing.Grant : default;
        ApplyGrant(grantId, payload, present: false, clock, writerId);
    }

    /// <summary>Revokes the given grant (by its <see cref="CrossTenantGrant.GrantId"/>).</summary>
    /// <param name="grant">The grant to revoke.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    public void RemoveGrant(CrossTenantGrant grant, HybridLogicalClock clock, string? writerId) =>
        RemoveGrant(grant.GrantId, clock, writerId);

    /// <summary>
    /// Returns <c>true</c> when <paramref name="subjectId"/> is a live tenant-admin
    /// subject. A zero-allocation membership check.
    /// </summary>
    /// <param name="subjectId">The subject id to test. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the subject's winning slot is present.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    public bool HasAdminSubject(string subjectId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        return Subjects.TryGetValue(subjectId, out var slot) && slot.Present;
    }

    /// <summary>
    /// Looks up the live grant with the given id. A zero-allocation lookup.
    /// </summary>
    /// <param name="grantId">The grant id to look up. Must not be <c>null</c>.</param>
    /// <param name="grant">The grant when this returns <c>true</c>; otherwise <c>default</c>.</param>
    /// <returns><c>true</c> when a live grant with that id exists.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grantId"/> is <c>null</c>.</exception>
    public bool TryGetGrant(string grantId, out CrossTenantGrant grant)
    {
        ArgumentNullException.ThrowIfNull(grantId);
        if (GrantSlots.TryGetValue(grantId, out var slot) && slot.Present)
        {
            grant = slot.Grant;
            return true;
        }

        grant = default;
        return false;
    }

    /// <summary>
    /// The live tenant-admin subject ids, in ordinal order. Materialised on each
    /// access; prefer <see cref="HasAdminSubject"/> for a single membership test.
    /// </summary>
    public IReadOnlyList<string> AdminSubjects
    {
        get
        {
            var result = new List<string>(Subjects.Count);
            foreach (var (subjectId, slot) in Subjects)
            {
                if (slot.Present)
                {
                    result.Add(subjectId);
                }
            }

            result.Sort(StringComparer.Ordinal);
            return result;
        }
    }

    /// <summary>
    /// The live cross-tenant grants, ordered by grant id. Materialised on each
    /// access; prefer <see cref="TryGetGrant"/> for a single lookup.
    /// </summary>
    public IReadOnlyList<CrossTenantGrant> Grants
    {
        get
        {
            // Project each live grant to a (grantId, grant) pair once, so the
            // computed GrantId interpolated string is allocated exactly once per
            // grant rather than O(n log n) times inside the sort comparator.
            var present = new List<(string GrantId, CrossTenantGrant Grant)>(GrantSlots.Count);
            foreach (var slot in GrantSlots.Values)
            {
                if (slot.Present)
                {
                    present.Add((slot.Grant.GrantId, slot.Grant));
                }
            }

            present.Sort(static (a, b) => string.CompareOrdinal(a.GrantId, b.GrantId));
            var result = new List<CrossTenantGrant>(present.Count);
            foreach (var (_, grant) in present)
            {
                result.Add(grant);
            }

            return result;
        }
    }

    /// <summary>
    /// Authorizes <paramref name="regionId"/> in the operator-written allowed set
    /// (add-wins by stamp). Only an authorized region may be made resident.
    /// </summary>
    /// <param name="regionId">The region id to authorize. Must not be <c>null</c> or empty.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    public void AuthorizeRegion(string regionId, HybridLogicalClock clock, string? writerId)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        ApplyAllowedRegion(regionId, present: true, clock, writerId);
    }

    /// <summary>
    /// Revokes <paramref name="regionId"/> from the operator-written allowed set (a
    /// tombstone by stamp).
    /// </summary>
    /// <param name="regionId">The region id to revoke. Must not be <c>null</c> or empty.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    public void RevokeRegion(string regionId, HybridLogicalClock clock, string? writerId)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        ApplyAllowedRegion(regionId, present: false, clock, writerId);
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="regionId"/> is a live
    /// operator-authorized region. A zero-allocation membership check.
    /// </summary>
    /// <param name="regionId">The region id to test. Must not be <c>null</c> or empty.</param>
    /// <returns><c>true</c> when the region's winning allow slot is present.</returns>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    public bool IsRegionAllowed(string regionId)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        return AllowedRegions.TryGetValue(regionId, out var slot) && slot.Present;
    }

    /// <summary>
    /// Sets the lifecycle status of <paramref name="regionId"/> if the stamp
    /// supersedes the region's current status stamp. A no-op when an older or equal
    /// stamp is written.
    /// </summary>
    /// <param name="regionId">The region id. Must not be <c>null</c> or empty.</param>
    /// <param name="status">The new lifecycle status.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>).</param>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    public void SetRegionStatus(string regionId, TenantRegionStatus status, HybridLogicalClock clock, string? writerId)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        var slot = new TenantRegionStatusSlot { Status = status, Clock = clock, WriterId = writerId };
        RegionStatuses[regionId] = RegionStatuses.TryGetValue(regionId, out var existing)
            ? TenantRegionStatusSlot.Merge(existing, slot)
            : slot;
    }

    /// <summary>
    /// The lifecycle status of <paramref name="regionId"/>, or
    /// <see cref="TenantRegionStatus.None"/> when the region has no status. A
    /// zero-allocation lookup.
    /// </summary>
    /// <param name="regionId">The region id to look up. Must not be <c>null</c> or empty.</param>
    /// <returns>The region's resolved status.</returns>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    public TenantRegionStatus GetRegionStatus(string regionId)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        return RegionStatuses.TryGetValue(regionId, out var slot) ? slot.Status : TenantRegionStatus.None;
    }

    /// <summary>
    /// <c>true</c> when the tenant has any region with a status other than
    /// <see cref="TenantRegionStatus.None"/>, i.e. residency has been configured at
    /// least once. When <c>false</c>, the tenant is unconfigured and treated as
    /// online in every region (backward-compatible admit-all).
    /// </summary>
    public bool HasResidencyConfiguration
    {
        get
        {
            foreach (var slot in RegionStatuses.Values)
            {
                if (slot.Status != TenantRegionStatus.None)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>
    /// The number of regions currently resident (status
    /// <see cref="TenantRegionStatus.Provisioning"/>,
    /// <see cref="TenantRegionStatus.Backfilling"/>, or
    /// <see cref="TenantRegionStatus.Online"/>). The last-resident-region guard uses
    /// this to refuse draining the final resident region.
    /// </summary>
    public int ResidentRegionCount
    {
        get
        {
            var count = 0;
            foreach (var slot in RegionStatuses.Values)
            {
                if (TenantRegionLifecycle.IsResident(slot.Status))
                {
                    count++;
                }
            }

            return count;
        }
    }

    /// <summary>
    /// The live operator-authorized region ids, in ordinal order. Materialised on
    /// each access; prefer <see cref="IsRegionAllowed"/> for a single membership test.
    /// </summary>
    public IReadOnlyList<string> AllowedRegionIds
    {
        get
        {
            var result = new List<string>(AllowedRegions.Count);
            foreach (var (regionId, slot) in AllowedRegions)
            {
                if (slot.Present)
                {
                    result.Add(regionId);
                }
            }

            result.Sort(StringComparer.Ordinal);
            return result;
        }
    }

    /// <summary>
    /// The per-region lifecycle statuses (excluding
    /// <see cref="TenantRegionStatus.None"/>), ordered by region id. Materialised on
    /// each access; prefer <see cref="GetRegionStatus"/> for a single lookup. This is
    /// the queryable per-region status the tenant-administration read op returns.
    /// </summary>
    public IReadOnlyList<KeyValuePair<string, TenantRegionStatus>> RegionStatusEntries
    {
        get
        {
            var result = new List<KeyValuePair<string, TenantRegionStatus>>(RegionStatuses.Count);
            foreach (var (regionId, slot) in RegionStatuses)
            {
                if (slot.Status != TenantRegionStatus.None)
                {
                    result.Add(new KeyValuePair<string, TenantRegionStatus>(regionId, slot.Status));
                }
            }

            result.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
            return result;
        }
    }

    /// <summary>
    /// Produces an independent deep copy of this record, so it can be merged or
    /// mutated without affecting the original.
    /// </summary>
    /// <returns>The cloned record.</returns>
    public TenantRecord Clone() =>
        new(Id)
        {
            StatusRegister = StatusRegister,
            QuotasRegister = QuotasRegister,
            PlacementRegister = PlacementRegister,
            Subjects = new Dictionary<string, TenantSubjectSlot>(Subjects, StringComparer.Ordinal),
            GrantSlots = new Dictionary<string, TenantGrantSlot>(GrantSlots, StringComparer.Ordinal),
            AllowedRegions = new Dictionary<string, TenantRegionAllowSlot>(AllowedRegions, StringComparer.Ordinal),
            RegionStatuses = new Dictionary<string, TenantRegionStatusSlot>(RegionStatuses, StringComparer.Ordinal),
        };

    /// <summary>
    /// Merges <paramref name="other"/> into this record in place, joining every
    /// field with the shared last-writer-wins order. The join is commutative,
    /// associative, and idempotent.
    /// </summary>
    /// <param name="other">The record to merge in. Must share this record's <see cref="Id"/>.</param>
    /// <returns>This record, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="other"/> defines a different tenant.</exception>
    public TenantRecord MergeFrom(TenantRecord other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (!Id.Equals(other.Id))
        {
            throw new ArgumentException(
                $"Cannot merge a record for tenant '{other.Id}' into a record for tenant '{Id}'.",
                nameof(other));
        }

        StatusRegister = TenantLwwRegister<TenantStatus>.Merge(StatusRegister, other.StatusRegister);
        QuotasRegister = TenantLwwRegister<TenantQuotas>.Merge(QuotasRegister, other.QuotasRegister);
        PlacementRegister = TenantLwwRegister<TenantPlacement>.Merge(PlacementRegister, other.PlacementRegister);

        foreach (var (subjectId, slot) in other.Subjects)
        {
            Subjects[subjectId] = Subjects.TryGetValue(subjectId, out var mine)
                ? TenantSubjectSlot.Merge(mine, slot)
                : slot;
        }

        foreach (var (grantId, slot) in other.GrantSlots)
        {
            GrantSlots[grantId] = GrantSlots.TryGetValue(grantId, out var mine)
                ? TenantGrantSlot.Merge(mine, slot)
                : slot;
        }

        foreach (var (regionId, slot) in other.AllowedRegions)
        {
            AllowedRegions[regionId] = AllowedRegions.TryGetValue(regionId, out var mine)
                ? TenantRegionAllowSlot.Merge(mine, slot)
                : slot;
        }

        foreach (var (regionId, slot) in other.RegionStatuses)
        {
            RegionStatuses[regionId] = RegionStatuses.TryGetValue(regionId, out var mine)
                ? TenantRegionStatusSlot.Merge(mine, slot)
                : slot;
        }

        return this;
    }

    /// <summary>
    /// Merges two records into a new record, leaving both inputs unchanged. The
    /// join is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">One record. Must not be <c>null</c>.</param>
    /// <param name="right">The other record. Must share <paramref name="left"/>'s <see cref="Id"/>. Must not be <c>null</c>.</param>
    /// <returns>The merged record.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/> or <paramref name="right"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The two records define different tenants.</exception>
    public static TenantRecord Merge(TenantRecord left, TenantRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.Clone().MergeFrom(right);
    }

    private static void ValidateQuotas(TenantQuotas quotas, string paramName)
    {
        if (quotas.BurstPercent < 0)
        {
            throw new ArgumentException(
                $"TenantQuotas.BurstPercent must be non-negative, but was {quotas.BurstPercent}.",
                paramName);
        }
    }

    private void ApplySubject(string subjectId, bool present, HybridLogicalClock clock, string? writerId)
    {
        var slot = new TenantSubjectSlot { Present = present, Clock = clock, WriterId = writerId };
        Subjects[subjectId] = Subjects.TryGetValue(subjectId, out var existing)
            ? TenantSubjectSlot.Merge(existing, slot)
            : slot;
    }

    private void ApplyGrant(string grantId, CrossTenantGrant grant, bool present, HybridLogicalClock clock, string? writerId)
    {
        var slot = new TenantGrantSlot { Grant = grant, Present = present, Clock = clock, WriterId = writerId };
        GrantSlots[grantId] = GrantSlots.TryGetValue(grantId, out var existing)
            ? TenantGrantSlot.Merge(existing, slot)
            : slot;
    }

    private void ApplyAllowedRegion(string regionId, bool present, HybridLogicalClock clock, string? writerId)
    {
        var slot = new TenantRegionAllowSlot { Present = present, Clock = clock, WriterId = writerId };
        AllowedRegions[regionId] = AllowedRegions.TryGetValue(regionId, out var existing)
            ? TenantRegionAllowSlot.Merge(existing, slot)
            : slot;
    }
}
