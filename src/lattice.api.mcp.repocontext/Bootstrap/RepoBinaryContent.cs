namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The text/binary classification every repository source (the mounted-tree
/// walker and the git fetcher) applies to file content, so a file is classified
/// identically whichever source indexed it.
/// </summary>
internal static class RepoBinaryContent
{
    // The leading window scanned for a NUL byte to classify a file as binary. This
    // matches the size Git samples (its FIRST_FEW_BYTES) for the same decision.
    private const int SniffByteCount = 8000;

    /// <summary>
    /// Reports whether <paramref name="content"/> looks like a binary (non-text)
    /// blob: a <c>NUL</c> byte anywhere in the leading window is treated as the
    /// signal, matching the classic heuristic Git applies. This deliberately reads
    /// only a bounded prefix so a large file costs a fixed scan.
    /// </summary>
    /// <param name="content">The file content, or its leading bytes.</param>
    internal static bool IsProbablyBinary(ReadOnlySpan<byte> content)
    {
        var window = content.Length <= SniffByteCount
            ? content
            : content[..SniffByteCount];
        return window.IndexOf((byte)0) >= 0;
    }
}
