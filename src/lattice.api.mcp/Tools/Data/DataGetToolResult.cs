namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_get</c> point-read tool. <see cref="Found"/>
/// distinguishes a present value (including an empty one) from an absent key. A
/// key the caller may not read is reported as absent (<see cref="Found"/> is
/// <see langword="false"/>), never a value - the underlying facade prunes it
/// silently rather than throwing.
/// </summary>
public sealed record DataGetToolResult
{
    /// <summary>Logical tree the key was read from.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key that was read.</summary>
    public required string Key { get; init; }

    /// <summary>
    /// <see langword="true"/> when a live value was returned; <see langword="false"/>
    /// when the key is absent, tombstoned, or hidden from the caller.
    /// </summary>
    public bool Found { get; init; }

    /// <summary>
    /// The value bytes when <see cref="Found"/> is <see langword="true"/>;
    /// otherwise an empty array. Base64-encoded in JSON structured content.
    /// </summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();
}
