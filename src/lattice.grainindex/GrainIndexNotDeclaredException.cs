namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Thrown when an administrative call names an index this silo does not
/// declare, so there is no declaration to report on and no crawl to control.
/// </summary>
/// <remarks>
/// <para>
/// The failure is loud rather than a null or empty result because an operator
/// asking about an index by name has almost certainly mistyped it or is talking
/// to a silo that does not host it - and both of those are worth knowing
/// immediately, whereas a quiet "no status" reads as "the index is fine".
/// </para>
/// <para>
/// The type derives directly from <see cref="Exception"/> so Orleans can
/// deep-copy it across a co-located grain-call boundary without a hand-written
/// copier, and is Orleans-serializable so the failure propagates intact across a
/// silo boundary.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.GrainIndexNotDeclaredException)]
public sealed class GrainIndexNotDeclaredException : Exception
{
    /// <summary>
    /// The index name the caller asked about. Empty on the message-only
    /// constructors.
    /// </summary>
    [Id(0)]
    public string IndexName { get; }

    /// <summary>
    /// The indexes this silo does declare, in declaration order. Empty on the
    /// message-only constructors.
    /// </summary>
    [Id(1)]
    public IReadOnlyList<string> DeclaredIndexes { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception-construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public GrainIndexNotDeclaredException()
    {
        IndexName = string.Empty;
        DeclaredIndexes = [];
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty context.</summary>
    /// <param name="message">Diagnostic context describing the unknown index.</param>
    public GrainIndexNotDeclaredException(string message) : base(message)
    {
        IndexName = string.Empty;
        DeclaredIndexes = [];
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the unknown index.</param>
    /// <param name="innerException">The underlying cause.</param>
    public GrainIndexNotDeclaredException(string message, Exception innerException)
        : base(message, innerException)
    {
        IndexName = string.Empty;
        DeclaredIndexes = [];
    }

    /// <summary>
    /// Initialises a new instance naming the unknown index and the ones this
    /// silo does declare. The primary production throw shape.
    /// </summary>
    /// <param name="indexName">The index the caller asked about. Must not be <c>null</c>.</param>
    /// <param name="declaredIndexes">The indexes this silo declares. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexNotDeclaredException(string indexName, IReadOnlyList<string> declaredIndexes)
        : base(BuildMessage(indexName, declaredIndexes))
    {
        IndexName = indexName;
        DeclaredIndexes = declaredIndexes;
    }

    private static string BuildMessage(string indexName, IReadOnlyList<string> declaredIndexes)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(declaredIndexes);

        string declared = declaredIndexes.Count == 0
            ? "(none)"
            : string.Join(", ", declaredIndexes);

        return $"This silo declares no grain index named '{indexName}', so it can neither report on "
            + $"it nor control its backfill. Declared indexes: {declared}.";
    }
}
