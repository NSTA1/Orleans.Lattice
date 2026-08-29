namespace Orleans.Lattice.Explorer.Plugins.Schema.Components;

/// <summary>
/// The Schema area's status-banner styling: the modifier class one
/// <see cref="Orleans.Lattice.Explorer.Schema.SchemaOperationStatus"/> renders
/// with. Kept in one place so every concern-scoped component in the area styles
/// a success, a denial, and a failure identically.
/// </summary>
internal static class SchemaStatusStyles
{
    /// <summary>The banner modifier class for <paramref name="status"/>.</summary>
    /// <param name="status">The operation outcome to style.</param>
    public static string For(Orleans.Lattice.Explorer.Schema.SchemaOperationStatus status) => status switch
    {
        Orleans.Lattice.Explorer.Schema.SchemaOperationStatus.Succeeded => "is-success",
        Orleans.Lattice.Explorer.Schema.SchemaOperationStatus.Denied => "is-denied",
        _ => "is-failed",
    };
}
