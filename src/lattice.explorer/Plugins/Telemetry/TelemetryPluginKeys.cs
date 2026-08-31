using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The Telemetry plugin's own key vocabulary: the stable id its access decision
/// is filed under, the declared preference key the panel remembers the selected
/// catalogue entry under, and the query-string key that entry is addressed by.
/// <para>
/// The plugin owns these strings; nothing outside the telemetry feature needs to
/// know them, which is what lets a decision be keyed without a shared record.
/// </para>
/// </summary>
public static class TelemetryPluginKeys
{
    /// <summary>The stable plugin id the Telemetry area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.telemetry";

    /// <summary>
    /// The query-string key the selected catalogue entry is addressed by, so a
    /// panel someone is looking at can be linked to rather than described.
    /// </summary>
    /// <remarks>
    /// A query parameter rather than a path segment: the catalogue id is a
    /// cluster-authored opaque value, not a slug this area coins, and the route
    /// grammar's path segments are canonical slugs. The shell's parser and
    /// formatter carry an extra parameter with no change to the grammar, which
    /// is what <c>ExplorerRouteParameters</c> exists for.
    /// </remarks>
    public const string SelectedQueryParameter = "query";

    /// <summary>
    /// The declared key the panel remembers the selected catalogue entry under.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A remembered id is re-validated against the catalogue on every load, so
    /// an entry the cluster stopped offering cannot leave the panel pointing at
    /// nothing - and, because the restore runs through the contract, the stale
    /// value is forgotten rather than rejected again on every later visit.
    /// </para>
    /// <para>
    /// Declared on the shell's preference contract rather than written through
    /// an opaque string namespace, so it is enumerable at <c>/reset-view</c>,
    /// scoped per user and per cluster, and explainable when it no longer
    /// resolves. Declared once as a static field because keys are compared by
    /// reference.
    /// </para>
    /// </remarks>
    public static readonly ExplorerPreferenceKey SelectedQueryPreference = new(
        "telemetry.query",
        "the telemetry panel you were last looking at");
}
