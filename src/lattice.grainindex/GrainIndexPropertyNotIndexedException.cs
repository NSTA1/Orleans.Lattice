namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Thrown when a query predicate names a property the index does not project,
/// so the index holds no entry that could answer it.
/// <para>
/// The failure is deliberately loud and immediate. An index entry carries only
/// the one property it was projected from, and the core predicate evaluator
/// treats an absent member as <i>missing</i> rather than as an error, so a
/// predicate over an unprojected property would quietly return an empty - or,
/// for an inequality, a wrongly full - result set. Failing at translation time
/// with the offending property and the projected set named turns that silent
/// wrong answer into an actionable one.
/// </para>
/// </summary>
/// <remarks>
/// The type derives directly from <see cref="Exception"/> so Orleans can
/// deep-copy it across a co-located grain-call boundary without a hand-written
/// copier, and is Orleans-serializable so the failure propagates intact across a
/// silo boundary.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.GrainIndexPropertyNotIndexedException)]
public sealed class GrainIndexPropertyNotIndexedException : Exception
{
    /// <summary>
    /// The logical name of the index the query was issued against. Empty on the
    /// message-only constructors.
    /// </summary>
    [Id(0)]
    public string IndexName { get; }

    /// <summary>
    /// The property the predicate named. Empty on the message-only constructors.
    /// </summary>
    [Id(1)]
    public string PropertyName { get; }

    /// <summary>
    /// The properties the index does project, in declaration order. Empty on the
    /// message-only constructors.
    /// </summary>
    [Id(2)]
    public IReadOnlyList<string> IndexedProperties { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception-construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public GrainIndexPropertyNotIndexedException()
    {
        IndexName = string.Empty;
        PropertyName = string.Empty;
        IndexedProperties = [];
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty context.</summary>
    /// <param name="message">Diagnostic context describing the unprojected property.</param>
    public GrainIndexPropertyNotIndexedException(string message) : base(message)
    {
        IndexName = string.Empty;
        PropertyName = string.Empty;
        IndexedProperties = [];
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the unprojected property.</param>
    /// <param name="innerException">The underlying cause.</param>
    public GrainIndexPropertyNotIndexedException(string message, Exception innerException)
        : base(message, innerException)
    {
        IndexName = string.Empty;
        PropertyName = string.Empty;
        IndexedProperties = [];
    }

    /// <summary>
    /// Initialises a new instance naming the index, the offending property, and
    /// the projected set. The primary production throw shape.
    /// </summary>
    /// <param name="indexName">The index's logical name. Must not be <c>null</c>.</param>
    /// <param name="propertyName">The property the predicate named. Must not be <c>null</c>.</param>
    /// <param name="indexedProperties">The projected property names. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexPropertyNotIndexedException(
        string indexName,
        string propertyName,
        IReadOnlyList<string> indexedProperties)
        : base(BuildMessage(indexName, propertyName, indexedProperties))
    {
        IndexName = indexName;
        PropertyName = propertyName;
        IndexedProperties = indexedProperties;
    }

    private static string BuildMessage(
        string indexName,
        string propertyName,
        IReadOnlyList<string> indexedProperties)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(propertyName);
        ArgumentNullException.ThrowIfNull(indexedProperties);

        string projected = indexedProperties.Count == 0
            ? "(none)"
            : string.Join(", ", indexedProperties);

        return $"Grain index '{indexName}' does not project property '{propertyName}', so no index "
            + $"entry can answer a predicate over it. The index projects: {projected}. Either "
            + "query one of those properties, or add the property to the index declaration with "
            + "Include and rebuild the index.";
    }
}
