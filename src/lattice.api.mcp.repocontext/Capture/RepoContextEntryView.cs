namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A read projection of a single repository-context entry returned by the
/// <c>repocontext_recall</c> and <c>repocontext_scan</c> tools. It flattens any
/// record family (structural node, symbol, or agent memory) into a stable,
/// agent-readable shape: the key and parsed identity, the current value of each
/// last-writer-wins scalar in <see cref="Fields"/>, the live members of the
/// record's tag set, the memory link relations, and the entry's remaining life.
/// <para>
/// A key that has no live value projects with <see cref="Exists"/> set to
/// <see langword="false"/> and empty collections, so a caller can distinguish an
/// absent or expired entry from an empty one without a second call.
/// </para>
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextEntryView
{
    /// <summary>The full repository-context key the entry is stored under.</summary>
    public required string Key { get; init; }

    /// <summary>Whether a live (non-expired, non-tombstoned) value exists at the key.</summary>
    public required bool Exists { get; init; }

    /// <summary>The record family the key addresses (for example <c>File</c>, <c>Symbol</c>, or <c>Memory</c>).</summary>
    public required string Kind { get; init; }

    /// <summary>The repository identifier the entry belongs to.</summary>
    public required string RepoId { get; init; }

    /// <summary>The file or package path, for structural file/package entries; otherwise <see langword="null"/>.</summary>
    public string? Path { get; init; }

    /// <summary>The fully-qualified name, for symbol entries; otherwise <see langword="null"/>.</summary>
    public string? FullyQualifiedName { get; init; }

    /// <summary>The topic bucket, for memory entries; otherwise <see langword="null"/>.</summary>
    public string? Topic { get; init; }

    /// <summary>The per-topic identifier, for memory entries; otherwise <see langword="null"/>.</summary>
    public string? Id { get; init; }

    /// <summary>
    /// The current value of each last-writer-wins scalar field of the record,
    /// keyed by a stable field name (for example <c>digest</c>, <c>title</c>, or
    /// <c>kind</c>). Absent registers are omitted.
    /// </summary>
    public required IReadOnlyDictionary<string, string> Fields { get; init; }

    /// <summary>The live members of the record's add-wins tag set, in ordinal order.</summary>
    public required IReadOnlyList<string> Tags { get; init; }

    /// <summary>
    /// For a memory entry, each link relation mapped to its live target keys; an
    /// empty map for every other record family.
    /// </summary>
    public required IReadOnlyDictionary<string, IReadOnlyList<string>> Links { get; init; }

    /// <summary>Whether the entry carries a finite expiry (a time-to-live was applied at write time).</summary>
    public required bool Expires { get; init; }

    /// <summary>
    /// The absolute expiry as an ISO-8601 UTC timestamp (round-trip "O" format), or
    /// <see langword="null"/> when the entry never expires. Emitted as a string
    /// rather than raw <see cref="DateTime.Ticks"/> so it stays within the safe
    /// integer range of JSON consumers.
    /// </summary>
    public string? ExpiresAtUtc { get; init; }

    /// <summary>
    /// The entry's remaining life in seconds at the instant it was read, clamped to
    /// be non-negative, or <see langword="null"/> when the entry never expires.
    /// </summary>
    public double? RemainingSeconds { get; init; }

    /// <summary>Whether the entry carries an expiry that the read instant has reached or passed.</summary>
    public required bool HasExpired { get; init; }
}
