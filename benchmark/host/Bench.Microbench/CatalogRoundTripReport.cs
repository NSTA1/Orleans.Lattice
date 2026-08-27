using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The deterministic half of the catalog-enumeration workload: an exact
/// round-trip census of the catalog paging path, measured rather than argued.
/// </summary>
/// <remarks>
/// <para>
/// Round-trip count is the figure issue #1686 targets, and unlike latency it is
/// exact: it does not vary with host, core count, or scheduler luck, so it needs
/// no statistical treatment and no BenchmarkDotNet job. This pass runs each
/// catalog shape once per arm against the counting grain surface and reports the
/// hops each actually made, broken down by kind.
/// </para>
/// <para>
/// It also sweeps the tenant-count axis, which is where the #1684 prefix range
/// scan is visible: as tenant count rises, an unscoped enumeration transfers
/// every tenant's ids while a tenant-scoped one keeps transferring just its own.
/// </para>
/// </remarks>
internal static class CatalogRoundTripReport
{
    private const int FlatCatalogSize = 2_000;
    private const int TreesPerTenant = 128;
    private static readonly int[] TenantSweep = [1, 8, 64, 256];

    /// <summary>One measured row of the census.</summary>
    internal sealed record Row(
        string Catalog,
        int CatalogSize,
        bool Visibility,
        int Pages,
        int Entries,
        int Enumerations,
        int PerEntryRoundTrips,
        int BatchedRoundTrips,
        int RegistryFanOutReads)
    {
        /// <summary>Round-trips removed per page, averaged over the pages the pass emitted.</summary>
        public double PerPageSaved => Pages == 0 ? 0 : (PerEntryRoundTrips - BatchedRoundTrips) / (double)Pages;

        /// <summary>The batched cost as a fraction of the per-entry cost.</summary>
        public double Ratio => PerEntryRoundTrips == 0 ? 1 : BatchedRoundTrips / (double)PerEntryRoundTrips;

        /// <summary>
        /// Total grain hops the batched arm causes anywhere in the system - the
        /// caller-visible round-trips plus the registry-internal reads the
        /// registry grain issues on the caller's behalf. Reported so the census
        /// cannot be read as claiming those inner reads vanished: they are the
        /// same reads, moved behind one facade crossing and issued concurrently
        /// instead of in sequence.
        /// </summary>
        public int BatchedTotalHops => BatchedRoundTrips + RegistryFanOutReads;
    }

    /// <summary>
    /// Runs the census and returns one row per (catalog shape, visibility) pair.
    /// </summary>
    public static async Task<List<Row>> MeasureAsync()
    {
        var rows = new List<Row>();

        foreach (var visibility in new[] { false, true })
        {
            rows.Add(await MeasureOneAsync(
                "single-tenant flat",
                CatalogHarness.BuildCatalog(CatalogEnumerationBenchmarks.CatalogShape.Flat2k, FlatCatalogSize, 0, 0),
                tenant: null,
                visibility).ConfigureAwait(false));

            foreach (var tenants in TenantSweep)
            {
                var ids = CatalogHarness.BuildCatalog(
                    CatalogEnumerationBenchmarks.CatalogShape.Tenants64_Unscoped,
                    FlatCatalogSize,
                    tenants,
                    TreesPerTenant);

                rows.Add(await MeasureOneAsync(
                    Label(tenants, scoped: false),
                    ids,
                    tenant: null,
                    visibility).ConfigureAwait(false));

                rows.Add(await MeasureOneAsync(
                    Label(tenants, scoped: true),
                    ids,
                    tenant: TenantId.Parse(CatalogHarness.TenantName(0)),
                    visibility).ConfigureAwait(false));
            }
        }

        return rows;
    }

    private static string Label(int tenants, bool scoped) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{tenants} tenants x {TreesPerTenant} ({(scoped ? "tenant-scoped" : "unscoped")})");

    private static async Task<Row> MeasureOneAsync(
        string label,
        List<string> treeIds,
        TenantId? tenant,
        bool visibility)
    {
        // Batched arm: the shipped query, end to end. Its counters are the real
        // hop count of the code that ships, enumeration included.
        var batchedSurface = new CatalogGrainSurface(treeIds);
        var query = CatalogHarness.BuildQuery(batchedSurface, visibility);
        batchedSurface.ResetCounters();
        var pages = await CatalogHarness.CapturePagesAsync(query, tenant).ConfigureAwait(false);
        var batchedRoundTrips = batchedSurface.RoundTrips;
        var enumerations = batchedSurface.Enumerations;

        var entries = 0;
        foreach (var page in pages)
        {
            entries += page.Count;
        }

        // Per-entry arm: replay the identical pages through the prior projection
        // and add the same enumeration cost, so the two totals differ only in the
        // projection's call shape.
        var perEntrySurface = new CatalogGrainSurface(treeIds);
        perEntrySurface.ResetCounters();
        foreach (var page in pages)
        {
            await CatalogPageProjections.PerEntryAsync(perEntrySurface, page).ConfigureAwait(false);
        }

        var perEntryRoundTrips = perEntrySurface.RoundTrips + enumerations;

        return new Row(
            label,
            treeIds.Count,
            visibility,
            pages.Count,
            entries,
            enumerations,
            perEntryRoundTrips,
            batchedRoundTrips,
            batchedSurface.RegistryFanOutReads);
    }

    /// <summary>Renders the census as a fixed-width console table.</summary>
    public static string Render(IReadOnlyList<Row> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine();
        sb.AppendLine("[catalog] grain round-trips to page a tree catalog end to end (exact, deterministic)");
        sb.AppendLine();
        sb.AppendLine("  catalog                              vis  pages  entries   per-entry    batched   saved/page  ratio  all-hops");
        sb.AppendLine("  ------------------------------------ ---  -----  -------  ----------  ---------  -----------  -----  --------");
        foreach (var r in rows)
        {
            sb.AppendLine(string.Create(
                CultureInfo.InvariantCulture,
                $"  {r.Catalog,-36} {(r.Visibility ? "on " : "off"),-3}  {r.Pages,5}  {r.Entries,7}  {r.PerEntryRoundTrips,10}  {r.BatchedRoundTrips,9}  {r.PerPageSaved,11:F1}  {r.Ratio,5:F2}  {r.BatchedTotalHops,8}"));
        }

        sb.AppendLine();
        sb.AppendLine("  per-entry = 2 round-trips per emitted entry (registry read + deletion probe), awaited");
        sb.AppendLine("  in sequence. batched = 1 registry read + 1 bounded concurrent deletion fan-out per");
        sb.AppendLine("  page. Both include the identical enumeration cost.");
        sb.AppendLine();
        sb.AppendLine("  per-entry / batched count hops crossing the catalog caller's facade - the figure the");
        sb.AppendLine("  change targets. all-hops adds the registry-internal single-key reads the registry");
        sb.AppendLine("  grain now issues on the caller's behalf: those reads did not vanish, they moved");
        sb.AppendLine("  behind one facade crossing and are issued as one concurrent wave, so they cost one");
        sb.AppendLine("  read latency per page rather than N. Sequential depth per page is what collapses:");
        sb.AppendLine("  2N awaits before, 3 waves after (enumerate, batched read, deletion fan-out).");
        sb.AppendLine();
        return sb.ToString();
    }

    /// <summary>Writes the census next to the harness results as <c>catalog-roundtrips.json</c>.</summary>
    public static void Write(IReadOnlyList<Row> rows, string resultsJsonPath)
    {
        var directory = Path.GetDirectoryName(resultsJsonPath);
        if (string.IsNullOrEmpty(directory))
        {
            return;
        }

        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "catalog-roundtrips.json");
        File.WriteAllText(path, JsonSerializer.Serialize(rows, JsonOptions));
        Console.WriteLine($"[catalog] round-trip census -> {path}");
    }

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
}
