using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema plugin's own key vocabulary: the stable id its access decisions
/// are filed under, and the declared preference key its retained sub-surface is
/// remembered under.
/// <para>
/// The plugin owns these strings; nothing outside the Schema feature needs to
/// know them, which is what lets a decision be keyed without a shared record.
/// </para>
/// </summary>
public static class SchemaPluginKeys
{
    /// <summary>The stable plugin id the Schema area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.schema";

    /// <summary>
    /// The query-string key the open sub-surface is addressed by when the
    /// address carries no catalogue selection.
    /// </summary>
    /// <remarks>
    /// The route grammar's <c>surface</c> path segment qualifies a selection and
    /// is ignored without one, which is the ordinary case for this area: Schema
    /// scopes to a tree it picks itself rather than to the shell's catalogue
    /// selection. So the path segment is used whenever the address carries a
    /// selection, and this parameter otherwise, and both are read. The key is
    /// area-scoped because switching area keeps the parameters, so a shared key
    /// would leak one area's surface into another's address.
    /// </remarks>
    public const string SurfaceParameter = "schema-surface";

    /// <summary>
    /// The declared key the area's open sub-surface is remembered under.
    /// </summary>
    /// <remarks>
    /// Declared on the shell's preference contract rather than written through
    /// an opaque string namespace, so it is enumerable at <c>/reset-view</c>,
    /// scoped per user and per cluster, and explainable when a remembered value
    /// no longer resolves. Declared once as a static field because keys are
    /// compared by reference.
    /// </remarks>
    public static readonly ExplorerPreferenceKey SurfacePreference = new(
        "schema.surface",
        "the Schema surface you were last on");
}
