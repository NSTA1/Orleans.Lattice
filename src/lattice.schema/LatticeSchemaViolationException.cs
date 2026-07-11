namespace Orleans.Lattice.Schema;

/// <summary>
/// Thrown by the public <see cref="ILattice"/> write / CRDT surface when the
/// schema-enforcement interceptor rejects an incoming <b>local</b> value that
/// does not satisfy the tree's <see cref="LatticeSchemaPolicy"/>. The offending
/// write is <b>fail-closed</b>: nothing is persisted before this exception is
/// raised, so a violation never leaves a partial write behind.
/// </summary>
/// <remarks>
/// <para>
/// Only local (user-origin) writes surface this exception. Trusted ingest
/// (replication apply, backup restore) never throws: under strict mode a
/// non-compliant ingested item is dead-lettered so ingest never blocks, and
/// without strict mode it is trusted as-is.
/// </para>
/// <para>
/// The type is Orleans-serializable so the violation propagates intact across a
/// grain-call boundary from the enforcing <c>LatticeGrain</c> back to the client.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaViolationException)]
public sealed class LatticeSchemaViolationException : InvalidOperationException
{
    /// <summary>The logical tree id the rejected write targeted. Empty on the message-only constructors.</summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>The key the rejected write targeted. Empty on the message-only constructors.</summary>
    [Id(1)]
    public string Key { get; }

    /// <summary>The human-readable reason the value failed validation. Empty on the message-only constructors.</summary>
    [Id(2)]
    public string Reason { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception-construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeSchemaViolationException()
    {
        TreeId = string.Empty;
        Key = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty context.</summary>
    /// <param name="message">Diagnostic context describing the violation.</param>
    public LatticeSchemaViolationException(string message) : base(message)
    {
        TreeId = string.Empty;
        Key = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the violation.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeSchemaViolationException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
        Key = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the rejected tree id, key, and
    /// validation reason. The primary production throw shape.
    /// </summary>
    /// <param name="treeId">The tree id the rejected write targeted. Must not be <c>null</c>.</param>
    /// <param name="key">The key the rejected write targeted. Must not be <c>null</c>.</param>
    /// <param name="reason">The reason the value failed validation. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/>, <paramref name="key"/>, or <paramref name="reason"/> is <c>null</c>.</exception>
    public LatticeSchemaViolationException(string treeId, string key, string reason)
        : base(BuildMessage(treeId, key, reason))
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(reason);
        TreeId = treeId;
        Key = key;
        Reason = reason;
    }

    private static string BuildMessage(string treeId, string key, string reason)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(reason);
        return $"Schema violation: the value for key '{key}' of tree '{treeId}' "
            + $"does not satisfy the tree's schema policy. {reason}";
    }
}
