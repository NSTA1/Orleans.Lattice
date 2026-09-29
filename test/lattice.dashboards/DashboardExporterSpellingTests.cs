using System.Text.RegularExpressions;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Discriminators for the exact exporter-spelling rule the dashboard drift guard
/// enforces (issue #3260): a bundled panel must name the family
/// <c>.AddPrometheusExporter()</c> emits, never the repository-context container's
/// unsuffixed spelling, and never a unit word its instrument's unit does not imply.
/// </summary>
/// <remarks>
/// Each case perturbs a <b>real</b> bundled dashboard and runs it through
/// <see cref="DashboardJsonTests.UnresolvedTokens"/>, the same resolution check the
/// production gate applies, so a regression in that gate - a map that starts
/// vouching for a second spelling again - turns these cases red rather than only
/// a synthetic helper.
/// </remarks>
[TestFixture]
public sealed class DashboardExporterSpellingTests
{
    /// <summary>
    /// Floor on the unit-bearing time-histogram bucket tokens the dashboards read.
    /// There are sixty-odd; the floor rules out a scan that silently finds none.
    /// </summary>
    private const int MinimumUnitBucketTokens = 50;

    /// <summary>
    /// Floor on the declared instruments whose container spelling differs from the
    /// exporter spelling, so the population-wide case cannot pass vacuously.
    /// There are over ninety; the floor sits a little below that.
    /// </summary>
    private const int MinimumUnitBearingInstruments = 80;

    private static readonly Regex UnitBucketTokenRegex =
        new(@"\borleans_lattice_[a-z0-9_]+_(?:milliseconds|seconds)_bucket\b", RegexOptions.Compiled);

    /// <summary>
    /// The 22 tokens issue #3260 corrected, each paired with the container spelling
    /// it replaced. Every one must be present on a bundled dashboard, and reverting
    /// it must fail the gate.
    /// </summary>
    private static readonly (string Exporter, string Container)[] CorrectedTokens =
    [
        ("orleans_lattice_auth_snapshot_age_seconds", "orleans_lattice_auth_snapshot_age"),
        ("orleans_lattice_backup_bytes_processed_bytes_total", "orleans_lattice_backup_bytes_processed_total"),
        ("orleans_lattice_backup_inventory_newest_age_seconds", "orleans_lattice_backup_inventory_newest_age"),
        ("orleans_lattice_backup_inventory_oldest_age_seconds", "orleans_lattice_backup_inventory_oldest_age"),
        ("orleans_lattice_backup_retention_bytes_reclaimed_bytes_total", "orleans_lattice_backup_retention_bytes_reclaimed_total"),
        ("orleans_lattice_backup_scope_last_success_age_seconds", "orleans_lattice_backup_scope_last_success_age"),
        ("orleans_lattice_compress_dictionary_trained_bytes_in_bytes_total", "orleans_lattice_compress_dictionary_trained_bytes_in_total"),
        ("orleans_lattice_compress_dictionary_trained_bytes_out_bytes_total", "orleans_lattice_compress_dictionary_trained_bytes_out_total"),
        ("orleans_lattice_grainindex_backfill_percent_complete_percent", "orleans_lattice_grainindex_backfill_percent_complete"),
        ("orleans_lattice_leaf_split_completion_oldest_age_seconds", "orleans_lattice_leaf_split_completion_oldest_age"),
        ("orleans_lattice_replication_bootstrap_bytes_received_bytes_total", "orleans_lattice_replication_bootstrap_bytes_received_total"),
        ("orleans_lattice_replication_coalesce_bytes_elided_bytes_total", "orleans_lattice_replication_coalesce_bytes_elided_total"),
        ("orleans_lattice_replication_compress_dictionary_bytes_in_bytes_total", "orleans_lattice_replication_compress_dictionary_bytes_in_total"),
        ("orleans_lattice_replication_compress_dictionary_bytes_out_bytes_total", "orleans_lattice_replication_compress_dictionary_bytes_out_total"),
        ("orleans_lattice_replication_peer_bytes_behind_bytes", "orleans_lattice_replication_peer_bytes_behind"),
        ("orleans_lattice_storage_policy_bytes_reclaimed_bytes_total", "orleans_lattice_storage_policy_bytes_reclaimed_total"),
        ("orleans_lattice_wal_gc_scheduler_pass_duration_seconds_count", "orleans_lattice_wal_gc_scheduler_pass_duration_count"),
        ("orleans_lattice_wal_gc_scheduler_pass_duration_seconds_sum", "orleans_lattice_wal_gc_scheduler_pass_duration_sum"),
        ("orleans_lattice_wal_gc_scheduler_phase_age_seconds", "orleans_lattice_wal_gc_scheduler_phase_age"),
        ("orleans_lattice_wal_gc_scheduler_wait_seconds_count", "orleans_lattice_wal_gc_scheduler_wait_count"),
        ("orleans_lattice_wal_gc_scheduler_wait_seconds_sum", "orleans_lattice_wal_gc_scheduler_wait_sum"),
        ("orleans_lattice_wal_replay_permit_wait_oldest_age_seconds", "orleans_lattice_wal_replay_permit_wait_oldest_age"),
    ];

    private static IEnumerable<TestCaseData> CorrectedTokenCases() =>
        CorrectedTokens.Select(static pair => new TestCaseData(pair.Exporter, pair.Container).SetArgDisplayNames(pair.Container));

    /// <summary>
    /// Reverting any corrected token on the real dashboard that reads it to the
    /// container's spelling fails the resolution gate.
    /// </summary>
    [TestCaseSource(nameof(CorrectedTokenCases))]
    public void Reverting_a_corrected_token_to_the_container_spelling_fails_resolution(string exporter, string container)
    {
        var dashboards = LatticeDashboards.All
            .Select(static kind => (Kind: kind, Json: LatticeDashboards.GetGrafanaDashboardJson(kind)))
            .Where(d => Regex.IsMatch(d.Json, $@"\b{Regex.Escape(exporter)}\b"))
            .ToList();

        Assert.That(dashboards, Is.Not.Empty,
            $"No bundled dashboard reads '{exporter}', so this case perturbs nothing.");

        foreach (var (kind, json) in dashboards)
        {
            Assert.That(DashboardJsonTests.UnresolvedTokens(json), Does.Not.Contain(exporter),
                $"'{exporter}' does not resolve on the unperturbed {kind} dashboard.");

            var perturbed = Regex.Replace(json, $@"\b{Regex.Escape(exporter)}\b", container);

            Assert.That(DashboardJsonTests.UnresolvedTokens(perturbed), Does.Contain(container),
                $"The {kind} dashboard written in the container's spelling '{container}' still resolves, so the "
                + "gate is vouching for a series .AddPrometheusExporter() never emits.");
        }
    }

    /// <summary>
    /// Stripping the unit word from any time-histogram bucket token a real
    /// dashboard reads fails the resolution gate. These are the tokens issue #3260
    /// explicitly keeps: they are correct, and their unsuffixed form is not.
    /// </summary>
    [Test]
    public void Stripping_the_unit_word_from_a_real_bucket_token_fails_resolution()
    {
        var checkedTokens = 0;
        var stillResolving = new List<string>();

        foreach (var kind in LatticeDashboards.All)
        {
            var json = LatticeDashboards.GetGrafanaDashboardJson(kind);
            var tokens = UnitBucketTokenRegex.Matches(json)
                .Select(static m => m.Value)
                .Distinct(StringComparer.Ordinal)
                .ToList();

            foreach (var token in tokens)
            {
                var stripped = token
                    .Replace("_milliseconds_bucket", "_bucket", StringComparison.Ordinal)
                    .Replace("_seconds_bucket", "_bucket", StringComparison.Ordinal);

                var perturbed = Regex.Replace(json, $@"\b{Regex.Escape(token)}\b", stripped);
                checkedTokens++;

                if (!DashboardJsonTests.UnresolvedTokens(perturbed).Contains(stripped, StringComparer.Ordinal))
                {
                    stillResolving.Add($"{kind}: {stripped}");
                }
            }
        }

        Assert.That(checkedTokens, Is.GreaterThanOrEqualTo(MinimumUnitBucketTokens),
            "The dashboards were expected to read many unit-suffixed bucket tokens; finding few means the scan is "
            + "not reading them, and an empty failure list below would be vacuous.");

        Assert.That(stillResolving, Is.Empty,
            "These bucket tokens still resolve with their unit word stripped, so the gate accepts a series "
            + $".AddPrometheusExporter() never emits:{Environment.NewLine}  - "
            + string.Join(Environment.NewLine + "  - ", stillResolving));
    }

    /// <summary>
    /// For every declared instrument whose unit contributes a word, the container's
    /// unsuffixed spelling of each of its series is rejected - unless that spelling
    /// happens to be the exact series of a different instrument.
    /// </summary>
    [Test]
    public void The_container_spelling_of_every_unit_bearing_instrument_does_not_resolve()
    {
        var exact = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in DeclaredInstruments.ByDottedName.Keys)
        {
            if (DashboardJsonTests.TryGetExporterSeriesNames(name, out var series))
            {
                exact.UnionWith(series);
            }
        }

        var examined = 0;
        var accepted = new List<string>();

        foreach (var (name, kind) in DeclaredInstruments.ByDottedName)
        {
            var unit = DeclaredInstruments.UnitByDottedName.GetValueOrDefault(name, string.Empty);
            var type = PrometheusExporterNaming.FamilyTypeOf(kind);
            var containerFamily = PrometheusExporterNaming.FamilyName(name, null, type);

            if (string.Equals(containerFamily, PrometheusExporterNaming.FamilyName(name, unit, type), StringComparison.Ordinal))
            {
                continue;
            }

            examined++;
            foreach (var container in PrometheusExporterNaming.SeriesNames(name, null, type))
            {
                if (DashboardJsonTests.Resolves(container) && !exact.Contains(container))
                {
                    accepted.Add($"{container} ({name}, unit '{unit}')");
                }
            }
        }

        Assert.That(examined, Is.GreaterThanOrEqualTo(MinimumUnitBearingInstruments),
            "Too few unit-bearing instruments were examined for the verdict below to mean anything.");

        Assert.That(accepted, Is.Empty,
            "The gate resolves the container's unsuffixed spelling of these instruments:"
            + $"{Environment.NewLine}  - " + string.Join(Environment.NewLine + "  - ", accepted));
    }
}
