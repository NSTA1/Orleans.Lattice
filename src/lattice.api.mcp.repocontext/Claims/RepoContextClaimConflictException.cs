using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Thrown when a repository-context write is refused by the fencing check: the
/// caller presented a superseded fencing token, presented none against a claimed
/// record, presented a token whose claim was already released, or wrote from a
/// region other than the one the claim was taken in.
/// <para>
/// It derives from <see cref="McpException"/> so the refusal travels the protocol's
/// own error channel exactly as every other caller error on this surface does,
/// while remaining a distinct type a host or a test can catch and attribute without
/// parsing a message. The structured fields carry the fencing state that produced
/// the refusal, so an agent can decide whether to re-claim or to abandon the item
/// without a second round trip.
/// </para>
/// </summary>
/// <remarks>
/// This exception is raised inside the MCP host process and is projected to a
/// protocol error, so it never crosses an Orleans grain boundary and carries no
/// Orleans serialization attributes. That is also why it needs no companion
/// <c>[RegisterCopier]</c> deep copier.
/// </remarks>
public sealed class RepoContextClaimConflictException : McpException
{
    /// <summary>Initialises a new instance with no diagnostic context.</summary>
    public RepoContextClaimConflictException()
        : base("The repository-context write was refused by the fencing check.")
    {
        Key = string.Empty;
        Reason = nameof(RepoContextFenceVerdict.StaleToken);
    }

    /// <summary>Initialises a new instance with the specified message.</summary>
    /// <param name="message">Diagnostic context describing the refusal.</param>
    public RepoContextClaimConflictException(string message) : base(message)
    {
        Key = string.Empty;
        Reason = nameof(RepoContextFenceVerdict.StaleToken);
    }

    /// <summary>Initialises a new instance with the specified message and inner exception.</summary>
    /// <param name="message">Diagnostic context describing the refusal.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public RepoContextClaimConflictException(string message, Exception innerException)
        : base(message, innerException)
    {
        Key = string.Empty;
        Reason = nameof(RepoContextFenceVerdict.StaleToken);
    }

    /// <summary>
    /// Initialises a new instance carrying the full fencing context of the refusal.
    /// The primary production throw shape.
    /// </summary>
    /// <param name="message">Diagnostic context describing the refusal.</param>
    /// <param name="key">The repository-context key the refused write targeted. Must not be <see langword="null"/>.</param>
    /// <param name="reason">The verdict that refused the write.</param>
    /// <param name="presentedFencingToken">The token the caller presented, or <see langword="null"/> for an unfenced write.</param>
    /// <param name="currentFencingToken">The record's fencing high-water mark, or <see langword="null"/> when it has never been claimed.</param>
    /// <param name="owner">The identity holding the claim, or <see langword="null"/>.</param>
    /// <param name="region">The region the claim was taken in, or <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is null.</exception>
    internal RepoContextClaimConflictException(
        string message,
        string key,
        RepoContextFenceVerdict reason,
        long? presentedFencingToken,
        long? currentFencingToken,
        string? owner,
        string? region) : base(message)
    {
        ArgumentNullException.ThrowIfNull(key);
        Key = key;
        Reason = reason.ToString();
        PresentedFencingToken = presentedFencingToken;
        CurrentFencingToken = currentFencingToken;
        Owner = owner;
        Region = region;
    }

    /// <summary>The repository-context key the refused write targeted.</summary>
    public string Key { get; }

    /// <summary>
    /// The machine-readable refusal reason: <c>StaleToken</c>, <c>ClaimRequired</c>,
    /// <c>ClaimReleased</c>, or <c>ForeignRegion</c>.
    /// </summary>
    public string Reason { get; }

    /// <summary>The fencing token the caller presented, or <see langword="null"/> for an unfenced write.</summary>
    public long? PresentedFencingToken { get; }

    /// <summary>The record's fencing high-water mark, or <see langword="null"/> when it has never been claimed.</summary>
    public long? CurrentFencingToken { get; }

    /// <summary>The identity holding the claim that refused the write, or <see langword="null"/>.</summary>
    public string? Owner { get; }

    /// <summary>The region the refusing claim was taken in, or <see langword="null"/>.</summary>
    public string? Region { get; }
}
