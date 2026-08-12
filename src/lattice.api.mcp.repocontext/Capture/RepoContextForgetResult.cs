namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_forget</c> tool: which key was forgotten and
/// how - a hard delete that removes the entry immediately, or a soft lapse that
/// re-writes the entry with a short time-to-live so it expires on its own.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextForgetResult
{
    /// <summary>The full repository-context key that was forgotten.</summary>
    public required string Key { get; init; }

    /// <summary>
    /// The forget mode: <c>delete</c> for an immediate hard delete, or <c>lapse</c>
    /// for a soft time-to-live expiry.
    /// </summary>
    public required string Mode { get; init; }

    /// <summary>
    /// Whether a live entry was found to forget. A hard delete of an absent key,
    /// or a lapse over an absent key, reports <see langword="false"/>.
    /// </summary>
    public required bool Existed { get; init; }

    /// <summary>
    /// For a soft lapse, the absolute UTC expiry the entry was re-written with, in
    /// <see cref="DateTime.Ticks"/>; <c>0</c> for a hard delete or an absent key.
    /// </summary>
    public required long ExpiresAtTicks { get; init; }
}
