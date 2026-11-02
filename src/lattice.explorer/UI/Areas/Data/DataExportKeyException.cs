namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// A key the export cannot write: it holds an unpaired UTF-16 surrogate, so it is not
/// Unicode text and no UTF-8 file can carry it exactly. The export stops rather than
/// write a different key.
/// </summary>
internal sealed class DataExportKeyException : Exception
{
    /// <summary>Creates the exception.</summary>
    public DataExportKeyException()
        : base("A key is not valid Unicode text, so it cannot be written to the export.")
    {
    }
}
