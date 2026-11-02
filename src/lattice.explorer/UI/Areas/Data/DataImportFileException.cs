namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Thrown when an import cannot read its file any further (issue #5365): a line longer
/// than the import's bound, a CSV file with no usable header, or a file that changed
/// between the pre-flight pass and the load. One malformed record is not this: it is a
/// refused line, and the import reads on.
/// </summary>
internal sealed class DataImportFileException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="line">The 1-based line the import stopped at.</param>
    /// <param name="message">What is wrong, as a sentence a person can act on.</param>
    public DataImportFileException(long line, string message)
        : base(message)
    {
        Line = line;
    }

    /// <summary>The 1-based line the import stopped at.</summary>
    public long Line { get; }
}
