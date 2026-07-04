namespace Orleans.Lattice;

/// <summary>
/// Thrown by the public <see cref="ILattice"/> write, delete, CRDT, atomic,
/// range-delete, bulk-load, and lifecycle surface when the registered
/// <see cref="ILatticeAccessGate"/> denies the caller's request. The offending
/// operation is <b>fail-closed</b>: nothing is persisted before this exception
/// is raised, so a denial never leaves a partial write behind.
/// </summary>
/// <remarks>
/// <para>
/// Point reads are deliberately <em>not</em> gated with this exception: a read
/// of a key the caller may not observe returns "not found" / empty rather than
/// throwing, matching the read-path key-filter semantics. This type is raised
/// only for the mutating and lifecycle surface (and for a hard-denied range
/// delete), where a denial must be surfaced explicitly rather than silently
/// narrowed.
/// </para>
/// <para>
/// Derives from <see cref="UnauthorizedAccessException"/> so callers can catch
/// the standard .NET access-denied type; the typed slot additionally carries the
/// <see cref="TreeId"/>, <see cref="Operation"/>, <see cref="SubjectId"/>, and
/// <see cref="Reason"/> that produced the denial. The type is
/// Orleans-serializable so the denial propagates intact across a grain-call
/// boundary from the enforcing <c>LatticeGrain</c> back to the client.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAuthorizationDenied)]
public sealed class LatticeAuthorizationDeniedException : UnauthorizedAccessException
{
    /// <summary>
    /// The logical tree id the denied operation targeted. Empty on the
    /// parameterless / message-only constructors.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>The operation the access gate denied.</summary>
    [Id(1)]
    public LatticeOperation Operation { get; }

    /// <summary>
    /// The stable subject id of the denied caller (for example
    /// <see cref="LatticeSubject.AnonymousSubjectId"/> when the caller carried
    /// no resolvable credential). Empty on the parameterless / message-only
    /// constructors.
    /// </summary>
    [Id(2)]
    public string SubjectId { get; }

    /// <summary>
    /// The human-readable reason the gate returned for the denial. Empty on the
    /// parameterless / message-only constructors.
    /// </summary>
    [Id(3)]
    public string Reason { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeAuthorizationDeniedException()
    {
        TreeId = string.Empty;
        SubjectId = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the denial.</param>
    public LatticeAuthorizationDeniedException(string message) : base(message)
    {
        TreeId = string.Empty;
        SubjectId = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the denial.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeAuthorizationDeniedException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
        SubjectId = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the denied tree id, operation,
    /// subject id, and gate reason. The primary production throw shape.
    /// </summary>
    /// <param name="treeId">The tree id the denied operation targeted. Must not be <c>null</c>.</param>
    /// <param name="operation">The operation the access gate denied.</param>
    /// <param name="subjectId">The stable subject id of the denied caller. Must not be <c>null</c>.</param>
    /// <param name="reason">The reason the gate returned for the denial. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/>, <paramref name="subjectId"/>, or <paramref name="reason"/> is <c>null</c>.</exception>
    public LatticeAuthorizationDeniedException(
        string treeId,
        LatticeOperation operation,
        string subjectId,
        string reason)
        : base(BuildMessage(treeId, operation, subjectId, reason))
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentNullException.ThrowIfNull(reason);
        TreeId = treeId;
        Operation = operation;
        SubjectId = subjectId;
        Reason = reason;
    }

    private static string BuildMessage(string treeId, LatticeOperation operation, string subjectId, string reason)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentNullException.ThrowIfNull(reason);
        return $"Access denied: subject '{subjectId}' is not authorized to perform "
            + $"{operation} on tree '{treeId}'. {reason}";
    }
}
