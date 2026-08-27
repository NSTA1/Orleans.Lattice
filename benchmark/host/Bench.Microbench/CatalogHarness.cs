using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Shared construction for the catalog-enumeration workload: the catalog shapes,
/// the wired-up <see cref="LatticeStateQuery"/>, the ambient tenant scope, and
/// the page partition capture the projection arms replay.
/// </summary>
internal static class CatalogHarness
{
    /// <summary>
    /// Builds the tree ids for a catalog shape. The flat shape models a
    /// single-tenant cluster; the tenant shapes model a multi-tenant one, whose
    /// ids share the <c>t/{tenant}/</c> prefix the registry range scan keys off.
    /// </summary>
    public static List<string> BuildCatalog(
        CatalogEnumerationBenchmarks.CatalogShape shape,
        int flatSize,
        int tenantCount,
        int treesPerTenant)
    {
        if (shape == CatalogEnumerationBenchmarks.CatalogShape.Flat2k)
        {
            var flat = new List<string>(flatSize);
            for (var i = 0; i < flatSize; i++)
            {
                flat.Add(string.Create(CultureInfo.InvariantCulture, $"catalog-tree-{i:D5}"));
            }

            return flat;
        }

        var ids = new List<string>(tenantCount * treesPerTenant);
        for (var t = 0; t < tenantCount; t++)
        {
            var tenant = TenantName(t);
            for (var i = 0; i < treesPerTenant; i++)
            {
                ids.Add(string.Create(CultureInfo.InvariantCulture, $"t/{tenant}/tree-{i:D4}"));
            }
        }

        return ids;
    }

    /// <summary>The tenant name for slot <paramref name="index"/>.</summary>
    public static string TenantName(int index) =>
        string.Create(CultureInfo.InvariantCulture, $"tenant-{index:D3}");

    /// <summary>
    /// The active tenant a shape pages under, or <c>null</c> to page unscoped.
    /// Only the scoped shape asserts one, which is what lets the registry narrow
    /// the enumeration to a single contiguous prefix range.
    /// </summary>
    public static TenantId? ActiveTenantFor(CatalogEnumerationBenchmarks.CatalogShape shape) =>
        shape == CatalogEnumerationBenchmarks.CatalogShape.Tenants64_Scoped
            ? TenantId.Parse(TenantName(0))
            : null;

    /// <summary>
    /// Stamps the ambient active tenant for the duration of a paging pass, or
    /// returns a no-op scope when the shape pages unscoped.
    /// </summary>
    public static IDisposable EnterTenant(TenantId? tenant) =>
        tenant is { } value ? LatticeActiveTenantContext.With(value) : NoScope.Instance;

    /// <summary>
    /// Builds the real <see cref="LatticeStateQuery"/> over the counting grain
    /// surface. With <paramref name="visibility"/> set, an access gate and a
    /// membership context are registered, which is what switches the per-entry
    /// visibility probe on.
    /// </summary>
    public static LatticeStateQuery BuildQuery(CatalogGrainSurface surface, bool visibility)
    {
        var grainFactory = new FakeGrainFactory();
        grainFactory.RouteByString<ILatticeRegistry>(_ => surface.Registry);
        grainFactory.RouteByString<ITreeDeletionGrain>(surface.Deletion);

        var options = new FakeOptionsMonitor<LatticeOptions>(new LatticeOptions());

        var services = new ServiceCollection();
        if (visibility)
        {
            services.AddSingleton<ILatticeAccessGate>(new AllowAllGate());
            services.AddSingleton<ILatticeMembershipContext>(new FixedMembership(new LatticeSubject("bench")));
        }

        return new LatticeStateQuery(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions()),
            services.BuildServiceProvider(),
            new NullTenantContextResolver());
    }

    /// <summary>
    /// Pages the catalog once and records the id partition of every page, so the
    /// projection arms replay exactly the pages the shipped query produces (same
    /// count, same sizes, same ids) instead of an invented partition.
    /// </summary>
    public static async Task<List<List<string>>> CapturePagesAsync(LatticeStateQuery query, TenantId? tenant)
    {
        var pages = new List<List<string>>();
        using var scope = EnterTenant(tenant);

        string? token = null;
        do
        {
            var page = await query.ListTreesAsync(new CatalogRequest { PageToken = token }).ConfigureAwait(false);
            var ids = new List<string>(page.Entries.Count);
            foreach (var entry in page.Entries)
            {
                ids.Add(entry.TreeId);
            }

            if (ids.Count > 0)
            {
                pages.Add(ids);
            }

            token = page.NextPageToken;
        }
        while (token is not null);

        return pages;
    }

    /// <summary>An always-allow gate: the visibility probe runs, and admits every tree.</summary>
    private sealed class AllowAllGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }

    /// <summary>Resolves the same named subject for every page.</summary>
    private sealed class FixedMembership(LatticeSubject subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(subject);
    }

    /// <summary>The no-tenant scope, so callers need no null branch around <c>using</c>.</summary>
    private sealed class NoScope : IDisposable
    {
        public static readonly NoScope Instance = new();

        public void Dispose()
        {
            // Nothing is stamped, so nothing needs unwinding.
        }
    }
}
