using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Catalog-enumeration workload for issue #1686: measures what it costs an
/// operator to page a tree catalog end to end, and how much of that cost is
/// grain round-trips rather than work.
/// </summary>
/// <remarks>
/// <para>
/// <b>What it compares.</b> Each emitted catalog entry used to cost two
/// sequential grain round-trips - one <c>ILatticeRegistry.GetEntryAsync</c> and
/// one <c>ITreeDeletionGrain.IsDeletedAsync</c> - so a default 100-entry page
/// cost 200 sequential hops on top of the enumeration. The shipped shape reads
/// exactly the surviving page in two waves: one batched
/// <c>ILatticeRegistry.GetEntriesAsync</c> multi-get plus one bounded concurrent
/// fan-out of the deletion probes, i.e. 1 + P hops in two waves. The
/// <c>PerEntry</c> arms reproduce the prior shape verbatim over identical
/// inputs, and the <c>Batched</c> arms reproduce the shipped shape, so the
/// column delta is exactly the call-shape change. <see cref="Paginate_Shipped"/>
/// anchors both to production code by driving the real
/// <c>LatticeStateQuery.ListTreesAsync</c>.
/// </para>
/// <para>
/// <b>Catalog shapes.</b> <see cref="CatalogShape.Flat2k"/> is a realistic
/// single-tenant catalog. The <c>Tenants64_*</c> pair is a multi-tenant catalog
/// read with and without an ambient active tenant, which is where the #1684
/// prefix range scan shows up: the scoped arm transfers one tenant's ids, the
/// unscoped arm transfers every tenant's. The tenant-count axis itself is swept
/// deterministically by <see cref="CatalogRoundTripReport"/> rather than by
/// BenchmarkDotNet, because round-trip count is exact and needs no statistics.
/// </para>
/// <para>
/// <b>Visibility.</b> Run with the access gate off and on. With a gate
/// registered the per-entry <c>IsCatalogEntryVisibleAsync</c> probe stays per
/// entry by design (it is an authorization decision, and batching ahead of it
/// would read entries it drops), so the gate-on arms show the residual per-entry
/// cost that remains once the other two hops are batched.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=catalog</c> (or <c>--suite catalog</c>);
/// see <c>Program.cs</c>. No Orleans silo is involved.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class CatalogEnumerationBenchmarks
{
    /// <summary>The catalog shapes the workload pages.</summary>
    public enum CatalogShape
    {
        /// <summary>A realistic single-tenant catalog: 2,000 flat tree ids.</summary>
        Flat2k,

        /// <summary>64 tenants of 128 trees, paged with no ambient active tenant.</summary>
        Tenants64_Unscoped,

        /// <summary>64 tenants of 128 trees, paged with one tenant asserted.</summary>
        Tenants64_Scoped,
    }

    private const int FlatCatalogSize = 2_000;
    private const int TenantCount = 64;
    private const int TreesPerTenant = 128;

    /// <summary>The catalog shape this run pages.</summary>
    [Params(CatalogShape.Flat2k, CatalogShape.Tenants64_Unscoped, CatalogShape.Tenants64_Scoped)]
    public CatalogShape Shape { get; set; }

    /// <summary>Whether an access gate is registered, making the catalog visibility-filtered.</summary>
    [Params(false, true)]
    public bool Visibility { get; set; }

    private CatalogGrainSurface _surface = null!;
    private LatticeStateQuery _query = null!;
    private List<string> _firstPageIds = null!;
    private List<List<string>> _allPageIds = null!;
    private TenantId? _activeTenant;

    /// <summary>
    /// Builds the catalog, the counting grain surface, and the real query, then
    /// captures the exact page partitions the shipped query produces so the two
    /// projection arms replay identical inputs.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _surface = new CatalogGrainSurface(CatalogHarness.BuildCatalog(Shape, FlatCatalogSize, TenantCount, TreesPerTenant));
        _activeTenant = CatalogHarness.ActiveTenantFor(Shape);
        _query = CatalogHarness.BuildQuery(_surface, Visibility);

        _allPageIds = CatalogHarness.CapturePagesAsync(_query, _activeTenant).GetAwaiter().GetResult();
        _firstPageIds = _allPageIds.Count > 0 ? _allPageIds[0] : [];
    }

    // ------------------------------------------------------------------
    // One page - the per-page round-trip cost
    // ------------------------------------------------------------------

    /// <summary>Baseline: two sequential grain round-trips per emitted entry.</summary>
    [Benchmark(Description = "Catalog page projection: per-entry (baseline)")]
    public Task<List<TreeCatalogEntry>> Page_PerEntry() =>
        CatalogPageProjections.PerEntryAsync(_surface, _firstPageIds);

    /// <summary>Shipped: one batched multi-get plus one bounded deletion fan-out.</summary>
    [Benchmark(Description = "Catalog page projection: batched (shipped)")]
    public Task<List<TreeCatalogEntry>> Page_Batched() =>
        CatalogPageProjections.BatchedAsync(_surface, _firstPageIds);

    // ------------------------------------------------------------------
    // Full pagination - the end-to-end transfer cost
    // ------------------------------------------------------------------

    /// <summary>Baseline: the per-entry projection replayed over every page.</summary>
    [Benchmark(Description = "Catalog full pagination: per-entry (baseline)")]
    public async Task<int> Paginate_PerEntry()
    {
        var emitted = 0;
        foreach (var page in _allPageIds)
        {
            emitted += (await CatalogPageProjections.PerEntryAsync(_surface, page).ConfigureAwait(false)).Count;
        }

        return emitted;
    }

    /// <summary>Shipped: the batched projection replayed over every page.</summary>
    [Benchmark(Description = "Catalog full pagination: batched (shipped)")]
    public async Task<int> Paginate_Batched()
    {
        var emitted = 0;
        foreach (var page in _allPageIds)
        {
            emitted += (await CatalogPageProjections.BatchedAsync(_surface, page).ConfigureAwait(false)).Count;
        }

        return emitted;
    }

    /// <summary>
    /// End-to-end anchor: the real <c>LatticeStateQuery.ListTreesAsync</c> paged
    /// to exhaustion, including registry enumeration, tenant scoping, the
    /// per-entry visibility probe, and the shipped batched projection.
    /// </summary>
    [Benchmark(Description = "Catalog full pagination: real LatticeStateQuery (end to end)")]
    public async Task<int> Paginate_Shipped()
    {
        var emitted = 0;
        using var scope = CatalogHarness.EnterTenant(_activeTenant);

        string? token = null;
        do
        {
            var page = await _query.ListTreesAsync(new CatalogRequest { PageToken = token }).ConfigureAwait(false);
            emitted += page.Entries.Count;
            token = page.NextPageToken;
        }
        while (token is not null);

        return emitted;
    }
}
