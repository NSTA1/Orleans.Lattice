namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The scan an export was paging through has expired - its cursor idled out or was
/// lost - so the export reopens it just after the last key it wrote.
/// </summary>
internal sealed class DataExportCursorExpiredException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="inner">The transport fault that reported it.</param>
    public DataExportCursorExpiredException(Exception? inner = null)
        : base("The export's scan expired.", inner)
    {
    }
}
