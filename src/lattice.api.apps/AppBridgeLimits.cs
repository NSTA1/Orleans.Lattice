namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The size bounds the app bridge enforces, equal to the AppKit frame protocol's limits. The frame broker
/// enforces them too, but it is not trusted, so the facade enforces them again.
/// </summary>
internal static class AppBridgeLimits
{
    /// <summary>The largest value, in bytes, a request may write or a response may carry.</summary>
    public const int MaxValueBytes = 65536;

    /// <summary>The longest key, in UTF-16 code units.</summary>
    public const int MaxKeyLength = 1024;

    /// <summary>The longest app-local tree name.</summary>
    public const int MaxTreeNameLength = 128;

    /// <summary>The longest continuation, in UTF-16 code units.</summary>
    public const int MaxContinuationLength = 4096;

    /// <summary>The largest page a scan returns; a larger request is clamped to it.</summary>
    public const int MaxPageSize = 200;

    /// <summary>The largest encoded response the frame accepts, in bytes.</summary>
    public const int MaxResponseBytes = 1048576;

    /// <summary>
    /// The estimated encoded cost of one scanned entry beyond its key and value: the JSON member names,
    /// quoting and separators the frame protocol wraps it in.
    /// </summary>
    public const int EntryOverheadBytes = 32;

    /// <summary>The estimated encoded cost of a scan response beyond its entries.</summary>
    public const int PageOverheadBytes = 64;

    /// <summary>
    /// Estimates the encoded size of one scanned entry: its UTF-8 key, its base64 value, and the per-entry
    /// overhead. The frame protocol carries values as base64, which is what the response bound is measured in.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="valueBytes">The value length in bytes.</param>
    /// <returns>The estimated encoded size in bytes.</returns>
    public static long EstimateEntryBytes(string key, int valueBytes) =>
        System.Text.Encoding.UTF8.GetByteCount(key) + (4L * ((valueBytes + 2) / 3)) + EntryOverheadBytes;
}
