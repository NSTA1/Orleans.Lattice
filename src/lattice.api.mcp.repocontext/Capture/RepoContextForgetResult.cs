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
    /// For a soft lapse, the absolute UTC expiry the entry was re-written with, as
    /// an ISO-8601 UTC timestamp (round-trip "O" format); <see langword="null"/> for
    /// a hard delete or an absent key.
    /// </summary>
    public string? ExpiresAtUtc { get; init; }

    /// <summary>
    /// Whether the entry that was forgotten held a value this server could not
    /// decode.
    /// <para>
    /// A lapse over a malformed entry deliberately succeeds - retiring a record
    /// does not require reading it, and refusing would leave an unreadable entry
    /// with no remedy but destruction. Reporting it is what keeps that tolerance
    /// honest: without this flag a store could quietly shed records it could not
    /// read, which is a worse failure than the one the tolerance fixes. A caller
    /// seeing <see langword="true"/> has learned that the entry was corrupt, which
    /// is a fact about the store worth acting on, not merely about this call.
    /// </para>
    /// </summary>
    public bool Undecodable { get; init; }
}
