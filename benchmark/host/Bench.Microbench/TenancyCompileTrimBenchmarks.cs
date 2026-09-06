using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three allocation reductions made to the tenancy <b>snapshot
/// rebuild</b> path - the whole-registry recompile the tenancy snapshot
/// maintainers perform every time the tenant registry changes and on their
/// periodic refresh cadence, over every tenant record in the store.
/// <para>
/// Nothing here touches a silo, so the suite is cheap to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>. Judge it on <b>Allocated</b>: these are
/// allocation trims, and the allocated column is deterministic and
/// bit-reproducible across rounds where Mean on a shared host is not.
/// </para>
/// <para>
/// The three edits under test sit on private members of
/// <c>CompiledTenantPolicy</c> reached only through
/// <c>CompiledTenantPolicy.Compile</c>, and the type's constructor is private, so
/// neither A/B lane can call the production method and still expose the shape it
/// built. Both lanes therefore reproduce the <b>same</b> surrounding shell -
/// identical records, identical per-record projection into
/// <see cref="CompiledTenant"/>, identical frozen output - and differ only in the
/// bodies under test. That symmetry is the point: a baseline arm that skips part
/// of the optimized arm's shell fabricates a regression.
/// <see cref="Compile_Production"/> pins the copied lanes to reality by running
/// the real shipped <c>CompiledTenantPolicy.Compile</c> over the same estate.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) the subject-to-tenants inversion accumulated into
/// <c>Dictionary&lt;string, List&lt;TenantId&gt;&gt;</c> buckets and froze through
/// the <b>selector</b> <c>ToFrozenDictionary</c> overload. That cost three
/// allocations for every admin subject - the list, the four-slot backing array
/// its first <c>Add</c> grows from empty, and the <c>ToArray</c> copy taken at
/// freeze time - where the overwhelmingly common subject administers exactly one
/// tenant, plus an intermediate the selector overload builds because it is not
/// the <see cref="Dictionary{TKey, TValue}"/>-source fast path. The replacement
/// counts each subject's tenants first and then allocates every bucket once at
/// its exact final width;
/// (2) the per-record grantee index had the identical list-bucket and
/// selector-freeze shape, replaced the same way;
/// (3) the placement and residency snapshot <c>Build</c> methods copied their
/// source into a defensive dedup <see cref="Dictionary{TKey, TValue}"/> before
/// freezing, even though their only production caller has just scanned the
/// registry into a dictionary and so already guarantees unique keys.
/// </para>
/// <para>
/// <b>Why count-then-fill and not grow-by-one.</b> The array-bucket rewrite the
/// authorization compile took (<see cref="AuthPolicyCompileTrimBenchmarks"/>)
/// appends with <see cref="Array.Resize{T}"/>, which is only sound when bucket
/// cardinality is bounded and small. Neither tenancy index has that bound: a
/// shared platform-operator subject can administer every tenant in the estate,
/// and a tenant can grant many scopes to a single peer, so grow-by-one would be
/// quadratic in both time and abandoned bytes. The
/// <c>*_ArrayResize</c> arms below measure that failure mode directly at a wide
/// fan-in, next to the count-then-fill arm that avoids it.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=tenancycompiletrims</c> (or
/// <c>--suite tenancycompiletrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class TenancyCompileTrimBenchmarks
{
    // An estate shaped like a real multi-tenant registry: a few hundred tenants,
    // each with a handful of admin subjects drawn from an overlapping pool (so
    // the inversion sees real fan-in, not a one-to-one map), plus a small fan of
    // cross-tenant grants.
    private const int TenantCount = 256;
    private const int AdminsPerTenant = 4;
    private const int AdminPoolSize = 320;
    private const int GrantsPerTenant = 6;
    private const int GranteePoolSize = 8;

    // The wide-fan-in shape the grow-by-one arms are measured against: one
    // grantee collecting every grant a tenant issued.
    private const int WideGrantCount = 256;

    private List<TenantRecord> _records = null!;
    private Dictionary<string, CompiledTenant> _tenants = null!;

    private CrossTenantGrant[] _narrowGrants = null!;
    private CrossTenantGrant[] _wideGrants = null!;

    private Dictionary<TenantId, TenantPlacement> _placements = null!;
    private Dictionary<TenantId, TenantRegionStatus> _residency = null!;

    [GlobalSetup]
    public void Setup()
    {
        _records = new List<TenantRecord>(TenantCount);
        for (var t = 0; t < TenantCount; t++)
        {
            _records.Add(BuildRecord(t));
        }

        // The compiled tenants both subject-index lanes invert, prepared once so
        // those lanes measure only the inversion body and not the record
        // projection that precedes it.
        _tenants = new Dictionary<string, CompiledTenant>(_records.Count, StringComparer.Ordinal);
        foreach (var record in _records)
        {
            _tenants[record.Id.Value!] = new CompiledTenant(
                record.Id,
                record.Status,
                record.AdminSubjects.ToFrozenSet(StringComparer.Ordinal),
                FrozenDictionary<string, CrossTenantGrant[]>.Empty);
        }

        // Narrow: many grantees, one grant each - the common registry shape.
        _narrowGrants = new CrossTenantGrant[GranteePoolSize];
        for (var i = 0; i < _narrowGrants.Length; i++)
        {
            _narrowGrants[i] = Grant($"grantee-{i:D3}", $"tree-{i:D3}");
        }

        // Wide: one grantee collecting every grant - the unbounded fan-in that
        // rules the grow-by-one append out.
        _wideGrants = new CrossTenantGrant[WideGrantCount];
        for (var i = 0; i < _wideGrants.Length; i++)
        {
            _wideGrants[i] = Grant("beta", $"tree-{i:D3}");
        }

        _placements = new Dictionary<TenantId, TenantPlacement>(TenantCount);
        _residency = new Dictionary<TenantId, TenantRegionStatus>(TenantCount);
        for (var t = 0; t < TenantCount; t++)
        {
            var id = TenantId.Parse($"tenant-{t:D4}");
            _placements[id] = t % 8 == 0
                ? new TenantPlacement { WalProviderName = $"wal-{t % 4}", DedicatedWal = true }
                : TenantPlacement.Shared;
            _residency[id] = t % 5 == 0 ? TenantRegionStatus.Draining : TenantRegionStatus.Online;
        }
    }

    // ---- whole-compile A/B: the aggregate of edits (1) and (2) --------------

    /// <summary>The full registry recompile as it stood before the change.</summary>
    [Benchmark(Baseline = true, Description = "Tenancy compile (before: List buckets, selector freeze, unsized map)")]
    public int Compile_Baseline() => CompileShell(_records, optimized: false).Tenants.Count;

    /// <summary>The full registry recompile with both bodies replaced.</summary>
    [Benchmark(Description = "Tenancy compile (after: count-then-fill exact-width buckets, direct freeze)")]
    public int Compile_Optimized() => CompileShell(_records, optimized: true).Tenants.Count;

    /// <summary>
    /// The real shipped <c>CompiledTenantPolicy.Compile</c> over the same estate.
    /// Not an A/B arm - it pins the copied shell above to the production path so a
    /// drift between them is visible rather than silent.
    /// </summary>
    [Benchmark(Description = "Tenancy compile (production CompiledTenantPolicy.Compile)")]
    public int Compile_Production() => CompiledTenantPolicy.Compile(_records).TenantCount;

    // ---- (1) subject inversion: list buckets vs count-then-fill -------------

    /// <summary>Inverting into List buckets and freezing through the selector overload.</summary>
    [Benchmark(Description = "Subject index (before: List bucket per subject, selector freeze)")]
    public int SubjectIndex_Baseline() => BuildSubjectIndexBaseline(_tenants).Count;

    /// <summary>Inverting by counting each subject's width, then filling exact-width arrays.</summary>
    [Benchmark(Description = "Subject index (after: count-then-fill exact-width arrays)")]
    public int SubjectIndex_Optimized() => BuildSubjectIndexOptimized(_tenants).Count;

    // ---- (2) grantee index, narrow shape: many grantees x one grant ---------

    /// <summary>Narrow fan-in, List buckets and the selector freeze.</summary>
    [Benchmark(Description = "Grantee index narrow (before: List bucket, selector freeze)")]
    public int GranteeNarrow_Baseline() => CompileGrantsBaseline(_narrowGrants).Count;

    /// <summary>Narrow fan-in, count-then-fill.</summary>
    [Benchmark(Description = "Grantee index narrow (after: count-then-fill)")]
    public int GranteeNarrow_Optimized() => CompileGrantsOptimized(_narrowGrants).Count;

    // ---- (2) grantee index, wide shape: one grantee x many grants -----------

    /// <summary>Wide fan-in, List buckets and the selector freeze.</summary>
    [Benchmark(Description = "Grantee index wide (before: List bucket, selector freeze)")]
    public int GranteeWide_Baseline() => CompileGrantsBaseline(_wideGrants).Count;

    /// <summary>Wide fan-in, count-then-fill: one array at the exact final width.</summary>
    [Benchmark(Description = "Grantee index wide (after: count-then-fill)")]
    public int GranteeWide_Optimized() => CompileGrantsOptimized(_wideGrants).Count;

    /// <summary>
    /// Wide fan-in through the grow-by-one array append the authorization compile
    /// uses. Not a candidate - it is here to show why it was rejected for this
    /// index, where bucket cardinality has no bound.
    /// </summary>
    [Benchmark(Description = "Grantee index wide (rejected: Array.Resize grow-by-one)")]
    public int GranteeWide_ArrayResize() => CompileGrantsArrayResize(_wideGrants).Count;

    // ---- (3) snapshot Build: defensive dedup copy vs direct freeze ----------

    /// <summary>Placement snapshot built through the defensive dedup copy.</summary>
    [Benchmark(Description = "Placement snapshot (before: defensive dedup copy then freeze)")]
    public int PlacementSnapshot_Baseline() => BuildPlacementBaseline(_placements).Count;

    /// <summary>Placement snapshot built by the shipped method, which elides the copy for a map source.</summary>
    [Benchmark(Description = "Placement snapshot (after: direct freeze of the dictionary source)")]
    public int PlacementSnapshot_Optimized() => TenantPlacementSnapshot.Build(_placements).Count;

    /// <summary>Residency snapshot built through the defensive dedup copy.</summary>
    [Benchmark(Description = "Residency snapshot (before: defensive dedup copy then freeze)")]
    public int ResidencySnapshot_Baseline() => BuildResidencyBaseline(_residency).Count;

    /// <summary>Residency snapshot built by the shipped method, which elides the copy for a map source.</summary>
    [Benchmark(Description = "Residency snapshot (after: direct freeze of the dictionary source)")]
    public int ResidencySnapshot_Optimized() => TenantResidencySnapshot.Build(_residency).Count;

    // ---- the shared shell both compile lanes run ---------------------------

    /// <summary>
    /// A faithful copy of <c>CompiledTenantPolicy.Compile</c>, parameterised by
    /// which set of bodies to run. Everything outside the two bodies under test is
    /// identical between the lanes, so the delta is exactly the work the
    /// production change removes.
    /// </summary>
    private static PolicyShape CompileShell(IEnumerable<TenantRecord> records, bool optimized)
    {
        var tenants = optimized && records.TryGetNonEnumeratedCount(out var recordCount) && recordCount > 0
            ? new Dictionary<string, CompiledTenant>(recordCount, StringComparer.Ordinal)
            : new Dictionary<string, CompiledTenant>(StringComparer.Ordinal);

        // The pre-change shell accumulated the subject inversion inline, as the
        // records were walked; the post-change shell inverts the compiled tenants
        // afterwards. Reproduced exactly, so neither lane is credited with work
        // the other actually did.
        var subjectAccum = optimized ? null : new Dictionary<string, List<TenantId>>(StringComparer.Ordinal);

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

            if (subjectAccum is not null)
            {
                foreach (var subject in admins)
                {
                    if (!subjectAccum.TryGetValue(subject, out var list))
                    {
                        list = [];
                        subjectAccum[subject] = list;
                    }

                    list.Add(id);
                }
            }

            tenants[id.Value] = new CompiledTenant(
                id,
                record.Status,
                adminSet,
                optimized
                    ? CompileGrantsOptimized(record.Grants)
                    : CompileGrantsBaseline(record.Grants));
        }

        var subjectIndex = optimized
            ? BuildSubjectIndexOptimized(tenants)
            : FreezeSubjectAccumBaseline(subjectAccum!);

        return new PolicyShape(subjectIndex, tenants.ToFrozenDictionary(StringComparer.Ordinal));
    }

    // ---- the bodies under test, before and after ---------------------------

    private static FrozenDictionary<string, TenantId[]> BuildSubjectIndexBaseline(
        Dictionary<string, CompiledTenant> tenants)
    {
        var subjectAccum = new Dictionary<string, List<TenantId>>(StringComparer.Ordinal);
        foreach (var tenant in tenants.Values)
        {
            foreach (var subject in tenant.Admins)
            {
                if (!subjectAccum.TryGetValue(subject, out var list))
                {
                    list = [];
                    subjectAccum[subject] = list;
                }

                list.Add(tenant.Id);
            }
        }

        return FreezeSubjectAccumBaseline(subjectAccum);
    }

    private static FrozenDictionary<string, TenantId[]> FreezeSubjectAccumBaseline(
        Dictionary<string, List<TenantId>> subjectAccum) =>
        subjectAccum.Count == 0
            ? FrozenDictionary<string, TenantId[]>.Empty
            : subjectAccum.ToFrozenDictionary(
                static pair => pair.Key,
                static pair =>
                {
                    var tenantsForSubject = pair.Value.ToArray();
                    Array.Sort(
                        tenantsForSubject,
                        static (a, b) => string.CompareOrdinal(a.Value, b.Value));
                    return tenantsForSubject;
                },
                StringComparer.Ordinal);

    private static FrozenDictionary<string, TenantId[]> BuildSubjectIndexOptimized(
        Dictionary<string, CompiledTenant> tenants)
    {
        var adminSlots = 0;
        foreach (var tenant in tenants.Values)
        {
            adminSlots += tenant.Admins.Count;
        }

        if (adminSlots == 0)
        {
            return FrozenDictionary<string, TenantId[]>.Empty;
        }

        var widths = new Dictionary<string, int>(Math.Min(adminSlots, 1024), StringComparer.Ordinal);
        foreach (var tenant in tenants.Values)
        {
            foreach (var subject in tenant.Admins)
            {
                ref var width = ref CollectionsMarshal.GetValueRefOrAddDefault(widths, subject, out _);
                width++;
            }
        }

        var index = new Dictionary<string, TenantId[]>(widths.Count, StringComparer.Ordinal);
        foreach (var tenant in tenants.Values)
        {
            var id = tenant.Id;
            foreach (var subject in tenant.Admins)
            {
                ref var remaining = ref CollectionsMarshal.GetValueRefOrAddDefault(widths, subject, out _);
                var slot = --remaining;

                ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(index, subject, out var existed);
                if (!existed)
                {
                    bucket = new TenantId[slot + 1];
                }

                bucket![slot] = id;
            }
        }

        foreach (var bucket in index.Values)
        {
            if (bucket.Length > 1)
            {
                Array.Sort(
                    bucket,
                    static (a, b) => string.CompareOrdinal(a.Value, b.Value));
            }
        }

        return index.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static FrozenDictionary<string, CrossTenantGrant[]> CompileGrantsBaseline(
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

    private static FrozenDictionary<string, CrossTenantGrant[]> CompileGrantsOptimized(
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

            widths ??= new Dictionary<string, int>(Math.Min(grants.Count, 64), StringComparer.Ordinal);
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

    /// <summary>
    /// The grow-by-one array append, for contrast only. Each append copies the
    /// whole bucket, so a grantee collecting k grants costs O(k^2) copied elements
    /// and abandons k-1 arrays.
    /// </summary>
    private static FrozenDictionary<string, CrossTenantGrant[]> CompileGrantsArrayResize(
        IReadOnlyList<CrossTenantGrant> grants)
    {
        if (grants.Count == 0)
        {
            return FrozenDictionary<string, CrossTenantGrant[]>.Empty;
        }

        Dictionary<string, CrossTenantGrant[]>? byGrantee = null;
        for (var i = 0; i < grants.Count; i++)
        {
            var grant = grants[i];
            if (grant.GranteeKind != TenantGranteeKind.Tenant || grant.Grantee is null)
            {
                continue;
            }

            byGrantee ??= new Dictionary<string, CrossTenantGrant[]>(StringComparer.Ordinal);
            ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(byGrantee, grant.Grantee, out var existed);
            if (!existed)
            {
                bucket = [grant];
                continue;
            }

            Array.Resize(ref bucket, bucket!.Length + 1);
            bucket[^1] = grant;
        }

        return byGrantee is null
            ? FrozenDictionary<string, CrossTenantGrant[]>.Empty
            : byGrantee.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static FrozenDictionary<TenantId, TenantPlacement> BuildPlacementBaseline(
        IEnumerable<KeyValuePair<TenantId, TenantPlacement>> placements)
    {
        var deduped = new Dictionary<TenantId, TenantPlacement>();
        foreach (var pair in placements)
        {
            deduped[pair.Key] = pair.Value;
        }

        return deduped.ToFrozenDictionary();
    }

    private static FrozenDictionary<TenantId, TenantRegionStatus> BuildResidencyBaseline(
        IEnumerable<KeyValuePair<TenantId, TenantRegionStatus>> statuses)
    {
        var deduped = new Dictionary<TenantId, TenantRegionStatus>();
        foreach (var pair in statuses)
        {
            deduped[pair.Key] = pair.Value;
        }

        return deduped.ToFrozenDictionary();
    }

    // ---- estate construction ------------------------------------------------

    private static TenantRecord BuildRecord(int t)
    {
        var record = TenantRecord.Create(
            TenantId.Parse($"tenant-{t:D4}"),
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1_000_000 },
            TenantPlacement.Shared,
            Clock(t + 1),
            "bench");

        for (var a = 0; a < AdminsPerTenant; a++)
        {
            // Subjects are drawn from a pool smaller than the total admin slot
            // count, so a subject genuinely administers several tenants and the
            // inversion sees real fan-in rather than a one-to-one map.
            var subject = $"admin-{((t * AdminsPerTenant) + a) % AdminPoolSize:D4}";
            record.AddAdminSubject(subject, Clock(t + a + 1), "bench");
        }

        for (var g = 0; g < GrantsPerTenant; g++)
        {
            record.AddGrant(
                Grant($"grantee-{g % GranteePoolSize:D3}", $"tree-{g:D3}"),
                Clock(t + g + 1),
                "bench");
        }

        return record;
    }

    private static CrossTenantGrant Grant(string grantee, string scope) =>
        CrossTenantGrant.Create(grantee, TenantGranteeKind.Tenant, scope, TenantGrantOperations.Read);

    private static HybridLogicalClock Clock(long ticks) =>
        new() { WallClockTicks = ticks, Counter = 0 };

    /// <summary>
    /// The shape <c>CompiledTenantPolicy</c> builds, carried as a plain record so
    /// both lanes can materialise it. The production type's constructor is
    /// private, so neither lane could otherwise expose what it produced.
    /// </summary>
    private sealed record PolicyShape(
        FrozenDictionary<string, TenantId[]> SubjectToTenants,
        FrozenDictionary<string, CompiledTenant> Tenants);
}
