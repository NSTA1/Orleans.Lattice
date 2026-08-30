namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The Telemetry plugin's own key vocabulary: the stable id its access decision
/// is filed under, and the preference keys the panels remember a caller's
/// bounded control choices in.
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
    /// The preference key the panel remembers the selected catalogue entry
    /// under. A remembered id is re-validated against the catalogue on every
    /// load, so an entry the cluster stopped offering cannot leave the panel
    /// pointing at nothing.
    /// </summary>
    public const string SelectedQueryPreference = "selected-query";
}
