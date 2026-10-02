using System.Collections.Frozen;
using System.Runtime.InteropServices;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, in-memory compilation of the tenant registry: the per-subject
/// tenant membership, and per-tenant status, admin set, and cross-tenant grants,
/// arranged for allocation-light lookup on the decision path. Built by
/// <see cref="Compile"/> from every <see cref="TenantRecord"/> in the registry
/// and swapped atomically by the snapshot maintainer whenever the registry tree
/// changes, so a warm tenant-policy decision is a pure in-memory lookup.
/// </summary>
/// <remarks>
/// This type is in-process singleton state. It is never serialized and never
/// crosses a grain boundary, so it carries no Orleans serialization attributes.
/// </remarks>
internal sealed class CompiledTenantPolicy
{
    /// <summary>
    /// The ceiling applied to the capacity hint the subject index is built at, so
    /// a pathological estate - one tenant carrying thousands of admin subjects -
    /// cannot make the hint over-allocate a map the compile will never fill.
    /// </summary>
    private const int SubjectCapacityHintLimit = 1024;

    /// <summary>
    /// The ceiling applied to the capacity hint the per-record grantee index is
    /// built at. A record's grant count is a sound upper bound on its distinct
    /// grantees, but a tenant that granted many scopes to a single peer would
    /// otherwise size the map for grantees it does not have.
    /// </summary>
    private const int GranteeCapacityHintLimit = 64;

    private static readonly TenantId[] NoTenants = [];

    private readonly FrozenDictionary<string, TenantId[]> _subjectToTenants;
    private readonly FrozenDictionary<string, CompiledTenant> _tenants;

    private CompiledTenantPolicy(
        FrozenDictionary<string, TenantId[]> subjectToTenants,
        FrozenDictionary<string, CompiledTenant> tenants,
        bool delegatedAccessEnabled)
    {
        _subjectToTenants = subjectToTenants;
        _tenants = tenants;
        IsDelegatedAccessEnabled = delegatedAccessEnabled;
    }

    /// <summary>The empty snapshot: no tenants and no subjects. Used before the first compile.</summary>
    public static CompiledTenantPolicy Empty { get; } =
        new(FrozenDictionary<string, TenantId[]>.Empty, FrozenDictionary<string, CompiledTenant>.Empty, false);

    /// <summary>
    /// <c>true</c> when the snapshot was compiled with delegated tenant access
    /// administration enabled, so member entries and group entries count. When
    /// <c>false</c> the snapshot is exactly the exact-id admin index it has always
    /// been: no member or group index is built.
    /// </summary>
    public bool IsDelegatedAccessEnabled { get; }

    /// <summary>The number of registered tenants in the snapshot. Exposed for tests.</summary>
    internal int TenantCount => _tenants.Count;

    /// <summary>
    /// The number of distinct subject entries (admin entries, plus member entries
    /// when delegated access administration is enabled) across all tenants in the
    /// snapshot. Exposed for tests.
    /// </summary>
    internal int SubjectCount => _subjectToTenants.Count;

    /// <summary>
    /// The tenants <paramref name="subjectId"/> may act as - the tenants for which
    /// it is a registered tenant-admin subject, or, when the snapshot was compiled
    /// with delegated access administration enabled, an admin or member entry - in
    /// ascending tenant-id order. A zero-allocation lookup that returns a shared
    /// empty array when the subject may act as no tenant; the returned array is the
    /// snapshot's own cached projection and must not be mutated.
    /// </summary>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <returns>The tenants the subject may act as.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        return _subjectToTenants.TryGetValue(subjectId, out var tenants) ? tenants : NoTenants;
    }

    /// <summary>
    /// The tenants <paramref name="subjectId"/> administers by exact id, in
    /// ascending tenant-id order, ignoring member entries. On a snapshot compiled
    /// with delegated access administration disabled - where the subject index
    /// holds admin entries only - this is exactly
    /// <see cref="ResolveAllowedTenants(string)"/> and returns the cached array.
    /// On a group-aware snapshot it filters the subject's tenants to those it
    /// administers, returning the cached array when nothing is filtered out.
    /// </summary>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <returns>The tenants the subject administers by exact id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    public IReadOnlyList<TenantId> ResolveAdminTenants(string subjectId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        if (!_subjectToTenants.TryGetValue(subjectId, out var tenants))
        {
            return NoTenants;
        }

        if (!IsDelegatedAccessEnabled)
        {
            return tenants;
        }

        var admins = 0;
        foreach (var tenant in tenants)
        {
            if (_tenants[tenant.Value].IsAdmin(subjectId))
            {
                admins++;
            }
        }

        if (admins == tenants.Length)
        {
            return tenants;
        }

        if (admins == 0)
        {
            return NoTenants;
        }

        // Already in ascending tenant-id order; filtering preserves it.
        var result = new TenantId[admins];
        var next = 0;
        foreach (var tenant in tenants)
        {
            if (_tenants[tenant.Value].IsAdmin(subjectId))
            {
                result[next++] = tenant;
            }
        }

        return result;
    }

    /// <summary>
    /// The tenants a subject may act as, considering its resolved transitive
    /// groups: the tenants for which its id or any of its groups is an admin or
    /// member entry, in ascending tenant-id order, without duplicates. When the
    /// snapshot was compiled with delegated access administration disabled, or the
    /// subject carries no groups, this is exactly
    /// <see cref="ResolveAllowedTenants(string)"/>.
    /// </summary>
    /// <remarks>
    /// Returns the snapshot's own cached array, with no merged copy, when at most
    /// one of the subject's id and groups resolves to any tenant; a subject whose id
    /// and groups resolve through several entries gets a freshly merged array. This
    /// is a listing read, not the per-request decision path, which is
    /// <see cref="CompiledTenant.IsMember"/>.
    /// </remarks>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <param name="groupIds">The subject's resolved transitive group ids. Must not be <c>null</c>.</param>
    /// <returns>The tenants the subject may act as.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> or <paramref name="groupIds"/> is <c>null</c>.</exception>
    public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId, IReadOnlyCollection<string> groupIds)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentNullException.ThrowIfNull(groupIds);

        if (!IsDelegatedAccessEnabled || groupIds.Count == 0)
        {
            return ResolveAllowedTenants(subjectId);
        }

        TenantId[]? single = _subjectToTenants.TryGetValue(subjectId, out var own) ? own : null;
        List<TenantId[]>? several = null;
        foreach (var group in groupIds)
        {
            if (group is null || !_subjectToTenants.TryGetValue(group, out var tenants))
            {
                continue;
            }

            if (single is null)
            {
                single = tenants;
            }
            else if (!ReferenceEquals(single, tenants))
            {
                several ??= [single];
                several.Add(tenants);
            }
        }

        if (several is null)
        {
            return single ?? NoTenants;
        }

        var merged = new HashSet<TenantId>();
        foreach (var tenants in several)
        {
            merged.UnionWith(tenants);
        }

        var result = new TenantId[merged.Count];
        merged.CopyTo(result);
        Array.Sort(result, static (a, b) => string.CompareOrdinal(a.Value, b.Value));
        return result;
    }

    /// <summary>Attempts to get the compiled entry for <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant id text.</param>
    /// <param name="tenant">The compiled tenant when present; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when the tenant is registered in the snapshot.</returns>
    public bool TryGetTenant(string tenantId, out CompiledTenant? tenant) =>
        _tenants.TryGetValue(tenantId, out tenant);

    /// <summary>
    /// Compiles a set of tenant records into an immutable snapshot with delegated
    /// tenant access administration disabled: exactly
    /// <see cref="Compile(IEnumerable{TenantRecord}, bool)"/> with <c>false</c>.
    /// </summary>
    /// <param name="records">The tenant records. Must not be <c>null</c>.</param>
    /// <returns>The compiled snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="records"/> is <c>null</c>.</exception>
    public static CompiledTenantPolicy Compile(IEnumerable<TenantRecord> records) => Compile(records, false);

    /// <summary>
    /// Compiles a set of tenant records into an immutable snapshot. Each record's
    /// admin subjects populate the subject-to-tenants index and the per-tenant
    /// admin set; each record's tenant-grantee cross-tenant grants are indexed by
    /// grantee tenant id for fast resolution. Records with the uninitialised
    /// tenant id are skipped.
    /// </summary>
    /// <remarks>
    /// When <paramref name="delegatedAccessEnabled"/> is <c>true</c>, each record's
    /// member subjects are compiled into a per-tenant member set and join the
    /// subject index, and the tenants become group-aware. The reserved
    /// <see cref="TenantId.Default"/> tenant never does: it accepts no member
    /// entries and stays exact-id. A group-aware tenant's admin and member entries
    /// are also filtered through <see cref="TenantAccessEntries.IsAdmissible"/>, so
    /// another tenant's group or a malformed <c>t/</c> entry - which a record can
    /// carry if it arrived by replication or restore without facade validation -
    /// never admits anyone (D4). When it is <c>false</c>, no member set or group
    /// index is built, nothing is filtered, and the snapshot is exactly what it was
    /// before member sets existed.
    /// </remarks>
    /// <param name="records">The tenant records. Must not be <c>null</c>.</param>
    /// <param name="delegatedAccessEnabled">Whether delegated tenant access administration is enabled.</param>
    /// <returns>The compiled snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="records"/> is <c>null</c>.</exception>
    public static CompiledTenantPolicy Compile(IEnumerable<TenantRecord> records, bool delegatedAccessEnabled)
    {
        ArgumentNullException.ThrowIfNull(records);

        // The source count is an exact upper bound on the tenant map's final size,
        // so when the source can hand it over for free the map is built at its
        // final capacity rather than rehashing every entry it already holds
        // through the 3/7/17/37/71/163/... prime bucket chain. The registry scan
        // that drives the rebuild passes a materialised list, so this is the
        // normal case, not the exceptional one.
        var tenants = records.TryGetNonEnumeratedCount(out var recordCount) && recordCount > 0
            ? new Dictionary<string, CompiledTenant>(recordCount, StringComparer.Ordinal)
            : new Dictionary<string, CompiledTenant>(StringComparer.Ordinal);

        foreach (var record in records)
        {
            if (record is null || record.Id.Value is null)
            {
                continue;
            }

            var id = record.Id;
            var groupAware = delegatedAccessEnabled && !id.Equals(TenantId.Default);
            var admins = record.AdminSubjects;
            var adminSet = admins.Count == 0
                ? FrozenSet<string>.Empty
                : groupAware
                    ? Admissible(admins, id)
                    : admins.ToFrozenSet(StringComparer.Ordinal);

            tenants[id.Value] = new CompiledTenant(
                id,
                record.Status,
                adminSet,
                CompileTenantGrants(record.Grants),
                groupAware ? CompileMembers(record) : null);
        }

        if (tenants.Count == 0)
        {
            return delegatedAccessEnabled ? EmptyDelegated : Empty;
        }

        return new CompiledTenantPolicy(
            BuildSubjectIndex(tenants),
            tenants.ToFrozenDictionary(StringComparer.Ordinal),
            delegatedAccessEnabled);
    }

    /// <summary>The empty snapshot compiled with delegated access administration enabled.</summary>
    private static CompiledTenantPolicy EmptyDelegated { get; } =
        new(FrozenDictionary<string, TenantId[]>.Empty, FrozenDictionary<string, CompiledTenant>.Empty, true);

    /// <summary>Compiles a record's live, admissible member subjects into a frozen set.</summary>
    private static FrozenSet<string> CompileMembers(TenantRecord record)
    {
        if (record.MemberSubjectCount == 0)
        {
            return FrozenSet<string>.Empty;
        }

        return Admissible(record.MemberSubjects, record.Id);
    }

    /// <summary>
    /// Freezes the entries of <paramref name="entries"/> that may count for
    /// <paramref name="tenant"/> (<see cref="TenantAccessEntries.IsAdmissible"/>):
    /// another tenant's group and any malformed <c>t/</c> entry are dropped, so a
    /// record that arrived by replication or restore without facade validation can
    /// never admit through one. Runs only on the rebuild path.
    /// </summary>
    private static FrozenSet<string> Admissible(IReadOnlyList<string> entries, TenantId tenant)
    {
        var kept = new List<string>(entries.Count);
        foreach (var entry in entries)
        {
            if (TenantAccessEntries.IsAdmissible(entry, tenant))
            {
                kept.Add(entry);
            }
        }

        return kept.Count == 0 ? FrozenSet<string>.Empty : kept.ToFrozenSet(StringComparer.Ordinal);
    }

    /// <summary>
    /// Inverts the compiled tenants into the subject-to-tenants index the
    /// decision path reads, with each subject's tenants in ascending tenant-id
    /// order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Built in two passes over the already-compiled tenants - never over the
    /// source records, which are an <see cref="IEnumerable{T}"/> and so are not
    /// guaranteed to be re-enumerable. The first pass counts how many tenants
    /// each subject administers; the second allocates every bucket once at that
    /// exact width and fills it. A subject therefore costs exactly one array,
    /// rather than the <see cref="List{T}"/> object, the four-slot backing array
    /// its first <c>Add</c> grows into from empty, and the exact-sized copy a
    /// finalising <c>ToArray</c> takes - three allocations where the
    /// overwhelmingly common subject administers a single tenant. Counting up
    /// front also rules out the quadratic append that a grow-by-one array bucket
    /// would cost the shared platform-operator subject that administers every
    /// tenant in the estate: there is no per-subject bound on that fan-in.
    /// </para>
    /// <para>
    /// The fill pass walks each bucket from the back, so the width entry counted
    /// in the first pass doubles as the fill cursor and no second cursor map is
    /// needed. Bucket order before the sort is therefore the reverse of tenant
    /// enumeration order, which is immaterial: the buckets are sorted by tenant
    /// id, and a subject's tenants are distinct because the tenant map is keyed
    /// by tenant id, so that ordering is total and deterministic.
    /// </para>
    /// </remarks>
    private static FrozenDictionary<string, TenantId[]> BuildSubjectIndex(
        Dictionary<string, CompiledTenant> tenants)
    {
        var adminSlots = 0;
        foreach (var tenant in tenants.Values)
        {
            adminSlots += tenant.Admins.Count + tenant.Members.Count;
        }

        if (adminSlots == 0)
        {
            return FrozenDictionary<string, TenantId[]>.Empty;
        }

        // Total admin and member slots is an upper bound on the distinct subject
        // count, clamped so a single subject-heavy tenant cannot inflate the hint.
        var widths = new Dictionary<string, int>(
            Math.Min(adminSlots, SubjectCapacityHintLimit),
            StringComparer.Ordinal);

        foreach (var tenant in tenants.Values)
        {
            foreach (var subject in tenant.Admins)
            {
                ref var width = ref CollectionsMarshal.GetValueRefOrAddDefault(widths, subject, out _);
                width++;
            }

            // A member entry that is also an admin entry of the same tenant is one
            // slot, not two: the subject's tenants stay distinct. A tenant that is
            // not group-aware has no members, so this loop is empty for it.
            foreach (var subject in tenant.Members)
            {
                if (!tenant.Admins.Contains(subject))
                {
                    ref var width = ref CollectionsMarshal.GetValueRefOrAddDefault(widths, subject, out _);
                    width++;
                }
            }
        }

        var index = new Dictionary<string, TenantId[]>(widths.Count, StringComparer.Ordinal);
        foreach (var tenant in tenants.Values)
        {
            var id = tenant.Id;
            foreach (var subject in tenant.Admins)
            {
                Place(widths, index, subject, id);
            }

            foreach (var subject in tenant.Members)
            {
                if (!tenant.Admins.Contains(subject))
                {
                    Place(widths, index, subject, id);
                }
            }
        }

        foreach (var bucket in index.Values)
        {
            // Sort each subject's tenants by id so ResolveAllowedTenants is
            // deterministic (ascending tenant-id order). A subject administering
            // exactly one tenant - the common case - is already ordered and skips
            // the sort. This runs only on the infrequent rebuild path, never on
            // the evaluate path.
            if (bucket.Length > 1)
            {
                Array.Sort(
                    bucket,
                    static (a, b) => string.CompareOrdinal(a.Value, b.Value));
            }
        }

        return index.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <summary>
    /// Places <paramref name="tenant"/> in <paramref name="subject"/>'s bucket,
    /// filling it from the back with the width counted in the first pass as the
    /// cursor, and allocating the bucket at its exact width on first use.
    /// </summary>
    private static void Place(
        Dictionary<string, int> widths,
        Dictionary<string, TenantId[]> index,
        string subject,
        TenantId tenant)
    {
        // Guaranteed present: the width pass counted this exact slot.
        ref var remaining = ref CollectionsMarshal.GetValueRefOrAddDefault(widths, subject, out _);
        var slot = --remaining;

        ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(index, subject, out var existed);
        if (!existed)
        {
            bucket = new TenantId[slot + 1];
        }

        bucket![slot] = tenant;
    }

    /// <summary>
    /// Indexes the tenant-grantee grants of one record by grantee tenant id.
    /// Subject-grantee grants are not indexed here: the engine's cross-tenant
    /// resolution is tenant-to-tenant.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every <em>live</em> grant is indexed regardless of its
    /// <see cref="CrossTenantGrant.State"/>. The lifecycle gate that admits only
    /// an active grant belongs on the decision path
    /// (<see cref="LatticeTenantPolicyEngine.ResolveCrossTenantGrant"/>), so
    /// pre-filtering here would split that authorization rule across two places;
    /// this projection stays a faithful view of the record.
    /// </para>
    /// <para>
    /// Built by counting each grantee's grants before allocating anything, so
    /// every bucket is allocated once at its exact final width. A grantee
    /// therefore costs one array instead of a <see cref="List{T}"/>, the
    /// four-slot backing array its first <c>Add</c> grows into from empty, and
    /// the exact-sized copy the finalising <c>ToArray</c> takes; the result also
    /// freezes through the <see cref="Dictionary{TKey, TValue}"/>-source
    /// <c>ToFrozenDictionary</c> overload rather than the selector overload,
    /// which builds an intermediate of its own. A record with no tenant-grantee
    /// grant still allocates nothing at all, as before.
    /// </para>
    /// <para>
    /// The fill pass walks the grants in reverse and fills each bucket from the
    /// back, so the counted width doubles as the fill cursor and each grant still
    /// lands at its encounter position - preserving the grant-id order
    /// <see cref="TenantRecord.Grants"/> hands over.
    /// </para>
    /// </remarks>
    private static FrozenDictionary<string, CrossTenantGrant[]> CompileTenantGrants(
        IReadOnlyList<CrossTenantGrant> grants)
    {
        if (grants.Count == 0)
        {
            return FrozenDictionary<string, CrossTenantGrant[]>.Empty;
        }

        Dictionary<string, int>? widths = null;
        for (var i = 0; i < grants.Count; i++)
        {
            var grant = grants[i];
            if (grant.GranteeKind != TenantGranteeKind.Tenant || grant.Grantee is null)
            {
                continue;
            }

            widths ??= new Dictionary<string, int>(
                Math.Min(grants.Count, GranteeCapacityHintLimit),
                StringComparer.Ordinal);
            ref var width = ref CollectionsMarshal.GetValueRefOrAddDefault(widths, grant.Grantee, out _);
            width++;
        }

        if (widths is null)
        {
            return FrozenDictionary<string, CrossTenantGrant[]>.Empty;
        }

        var byGrantee = new Dictionary<string, CrossTenantGrant[]>(widths.Count, StringComparer.Ordinal);
        for (var i = grants.Count - 1; i >= 0; i--)
        {
            var grant = grants[i];
            if (grant.GranteeKind != TenantGranteeKind.Tenant || grant.Grantee is null)
            {
                continue;
            }

            // Guaranteed present: the width pass counted this exact grant.
            ref var remaining = ref CollectionsMarshal.GetValueRefOrAddDefault(widths, grant.Grantee, out _);
            var slot = --remaining;

            ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(byGrantee, grant.Grantee, out var existed);
            if (!existed)
            {
                bucket = new CrossTenantGrant[slot + 1];
            }

            bucket![slot] = grant;
        }

        return byGrantee.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
