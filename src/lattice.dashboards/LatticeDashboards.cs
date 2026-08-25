namespace Orleans.Lattice.Dashboards;

/// <summary>
/// Static accessor for the Grafana dashboard JSON files bundled with
/// <c>Orleans.Lattice.Dashboards</c>. Dashboards are embedded as
/// resources in this assembly so a host can fetch them at runtime
/// (e.g. to write them to a Grafana sidecar's provisioning directory)
/// without taking a filesystem dependency on the package layout.
/// </summary>
/// <remarks>
/// <para>
/// Every dashboard targets the <c>orleans.lattice</c> meter
/// (and, for <see cref="LatticeDashboardKind.Replication"/>, the
/// <c>orleans.lattice.replication</c> meter; for
/// <see cref="LatticeDashboardKind.Authorization"/>, the
/// <c>orleans.lattice.auth</c> and <c>orleans.lattice.membership</c>
/// meters; for <see cref="LatticeDashboardKind.Backup"/>, the
/// <c>orleans.lattice.backup</c> meter) over a Prometheus data source. Import the JSON in Grafana via
/// <em>Dashboards → New → Import</em> or drop it into a
/// provisioning directory referenced by
/// <c>Provisioning/dashboards.yaml</c>.
/// </para>
/// <para>
/// The dashboards reference metric names by their published instrument
/// names. A regression test in the companion test project asserts
/// every referenced name resolves to a live instrument on
/// <c>LatticeMetrics.Meter</c> (or <c>LatticeReplicationMetrics.Meter</c>
/// for the Replication dashboard), so a future rename in the core or
/// replication package fails CI before the dashboard ships stale.
/// </para>
/// </remarks>
public static class LatticeDashboards
{
    /// <summary>
    /// Returns the Grafana dashboard JSON for <paramref name="kind"/>
    /// as a UTF-8 string. The returned content is a complete Grafana
    /// dashboard model (panels, templating, time range) suitable for
    /// import.
    /// </summary>
    /// <param name="kind">The dashboard to retrieve.</param>
    /// <returns>The dashboard JSON as a string.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="kind"/> is not a defined value of
    /// <see cref="LatticeDashboardKind"/>.
    /// </exception>
    public static string GetGrafanaDashboardJson(LatticeDashboardKind kind)
    {
        var resourceName = ResourceNameFor(kind);
        var assembly = typeof(LatticeDashboards).Assembly;
        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException(
                $"Embedded dashboard resource '{resourceName}' was not found in assembly '{assembly.FullName}'.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <summary>
    /// Every dashboard kind shipped with the package.
    /// </summary>
    public static IReadOnlyList<LatticeDashboardKind> All { get; } =
        Enum.GetValues<LatticeDashboardKind>();

    internal static string ResourceNameFor(LatticeDashboardKind kind) => kind switch
    {
        LatticeDashboardKind.Overview => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeOverview.json",
        LatticeDashboardKind.CommitPath => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeCommitPath.json",
        LatticeDashboardKind.Replication => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeReplication.json",
        LatticeDashboardKind.AtomicWrites => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeAtomicWrites.json",
        LatticeDashboardKind.MaterialisedViews => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeMaterialisedViews.json",
        LatticeDashboardKind.Authorization => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeAuthorization.json",
        LatticeDashboardKind.Backup => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeBackup.json",
        LatticeDashboardKind.Scaling => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeScaling.json",
        LatticeDashboardKind.ReplicationGrpc => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeReplicationGrpc.json",
        LatticeDashboardKind.Tenancy => "Orleans.Lattice.Dashboards.Grafana.OrleansLatticeTenancy.json",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown dashboard kind."),
    };
}
