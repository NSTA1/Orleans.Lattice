using System.Collections.Frozen;

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
    private static readonly TenantId[] NoTenants = [];

    private readonly FrozenDictionary<string, TenantId[]> _subjectToTenants;
    private readonly FrozenDictionary<string, CompiledTenant> _tenants;

    private CompiledTenantPolicy(
        FrozenDictionary<string, TenantId[]> subjectToTenants,
        FrozenDictionary<string, CompiledTenant> tenants)
    {
        _subjectToTenants = subjectToTenants;
        _tenants = tenants;
    }

    /// <summary>The empty snapshot: no tenants and no subjects. Used before the first compile.</summary>
    public static CompiledTenantPolicy Empty { get; } =
        new(FrozenDictionary<string, TenantId[]>.Empty, FrozenDictionary<string, CompiledTenant>.Empty);

    /// <summary>The number of registered tenants in the snapshot. Exposed for tests.</summary>
    internal int TenantCount => _tenants.Count;

    /// <summary>The number of distinct admin subjects across all tenants in the snapshot. Exposed for tests.</summary>
    internal int SubjectCount => _subjectToTenants.Count;

    /// <summary>
    /// The tenants <paramref name="subjectId"/> may act as - the tenants for which
    /// it is a registered tenant-admin subject - in ascending tenant-id order. A
    /// zero-allocation lookup that returns a shared empty array when the subject
    /// administers no tenant; the returned array is the snapshot's own cached
    /// projection and must not be mutated.
    /// </summary>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <returns>The tenants the subject may act as.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        return _subjectToTenants.TryGetValue(subjectId, out var tenants) ? tenants : NoTenants;
    }

    /// <summary>Attempts to get the compiled entry for <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant id text.</param>
    /// <param name="tenant">The compiled tenant when present; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when the tenant is registered in the snapshot.</returns>
    public bool TryGetTenant(string tenantId, out CompiledTenant? tenant) =>
        _tenants.TryGetValue(tenantId, out tenant);

    /// <summary>
    /// Compiles a set of tenant records into an immutable snapshot. Each record's
    /// admin subjects populate the subject-to-tenants index and the per-tenant
    /// admin set; each record's tenant-grantee cross-tenant grants are indexed by
    /// grantee tenant id for fast resolution. Records with the uninitialised
    /// tenant id are skipped.
    /// </summary>
    /// <param name="records">The tenant records. Must not be <c>null</c>.</param>
    /// <returns>The compiled snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="records"/> is <c>null</c>.</exception>
    public static CompiledTenantPolicy Compile(IEnumerable<TenantRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        var tenants = new Dictionary<string, CompiledTenant>(StringComparer.Ordinal);
        var subjectAccum = new Dictionary<string, List<TenantId>>(StringComparer.Ordinal);

        foreach (var record in records)
        {
            if (record is null || record.Id.Value is null)
            {
                continue;
            }

            var id = record.Id;
            var admins = record.AdminSubjects;
            var adminSet = admins.Count == 0
                ? FrozenSet<string>.Empty
                : admins.ToFrozenSet(StringComparer.Ordinal);

            foreach (var subject in admins)
            {
                if (!subjectAccum.TryGetValue(subject, out var list))
                {
                    list = [];
                    subjectAccum[subject] = list;
                }

                list.Add(id);
            }

            tenants[id.Value] = new CompiledTenant(
                id,
                record.Status,
                adminSet,
                CompileTenantGrants(record.Grants));
        }

        if (tenants.Count == 0)
        {
            return Empty;
        }

        var subjectIndex = subjectAccum.Count == 0
            ? FrozenDictionary<string, TenantId[]>.Empty
            : subjectAccum.ToFrozenDictionary(
                static pair => pair.Key,
                static pair =>
                {
                    // Sort each subject's tenants by id so ResolveAllowedTenants is
                    // deterministic (ascending tenant-id order). This runs only on
                    // the infrequent rebuild path, never on the evaluate path.
                    var tenantsForSubject = pair.Value.ToArray();
                    Array.Sort(
                        tenantsForSubject,
                        static (a, b) => string.CompareOrdinal(a.Value, b.Value));
                    return tenantsForSubject;
                },
                StringComparer.Ordinal);

        return new CompiledTenantPolicy(
            subjectIndex,
            tenants.ToFrozenDictionary(StringComparer.Ordinal));
    }

    /// <summary>
    /// Indexes the tenant-grantee grants of one record by grantee tenant id.
    /// Subject-grantee grants are not indexed here: the engine's cross-tenant
    /// resolution is tenant-to-tenant.
    /// </summary>
    private static FrozenDictionary<string, CrossTenantGrant[]> CompileTenantGrants(
        IReadOnlyList<CrossTenantGrant> grants)
    {
        if (grants.Count == 0)
        {
            return FrozenDictionary<string, CrossTenantGrant[]>.Empty;
        }

        Dictionary<string, List<CrossTenantGrant>>? byGrantee = null;
        for (var i = 0; i < grants.Count; i++)
        {
            var grant = grants[i];
            if (grant.GranteeKind != TenantGranteeKind.Tenant || grant.Grantee is null)
            {
                continue;
            }

            byGrantee ??= new Dictionary<string, List<CrossTenantGrant>>(StringComparer.Ordinal);
            if (!byGrantee.TryGetValue(grant.Grantee, out var list))
            {
                list = [];
                byGrantee[grant.Grantee] = list;
            }

            list.Add(grant);
        }

        return byGrantee is null
            ? FrozenDictionary<string, CrossTenantGrant[]>.Empty
            : byGrantee.ToFrozenDictionary(
                static pair => pair.Key,
                static pair => pair.Value.ToArray(),
                StringComparer.Ordinal);
    }
}
