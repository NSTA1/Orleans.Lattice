namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Thrown at silo start when a declared grain index has drifted from the
/// declaration its already-written data was built under, on one or more fields
/// that <see cref="GrainIndexDriftClassification"/> classifies as
/// drift-breaking, and the index's
/// <see cref="GrainIndexOptions.DriftPolicy"/> is
/// <see cref="GrainIndexDriftPolicy.Reject"/>.
/// <para>
/// Starting anyway would leave the index's stored entries encoded under the old
/// declaration while every query read them under the new one, which produces a
/// quietly wrong answer rather than an error. Failing the silo makes the
/// operator choose deliberately between reverting the declaration and rebuilding
/// the index.
/// </para>
/// </summary>
/// <remarks>
/// The type derives directly from <see cref="Exception"/> so Orleans can
/// deep-copy it across a co-located grain-call boundary without a hand-written
/// copier, and is Orleans-serializable so the failure propagates intact across a
/// silo boundary.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.GrainIndexConfigurationDriftException)]
public sealed class GrainIndexConfigurationDriftException : Exception
{
    /// <summary>
    /// The logical name of the index whose declaration drifted. Empty on the
    /// message-only constructors.
    /// </summary>
    [Id(0)]
    public string IndexName { get; }

    /// <summary>
    /// The drift-breaking fields that changed, in
    /// <see cref="GrainIndexDefinitionField"/> order. Empty on the message-only
    /// constructors.
    /// </summary>
    [Id(1)]
    public IReadOnlyList<GrainIndexDefinitionField> ChangedFields { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception-construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public GrainIndexConfigurationDriftException()
    {
        IndexName = string.Empty;
        ChangedFields = [];
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty context.</summary>
    /// <param name="message">Diagnostic context describing the drift.</param>
    public GrainIndexConfigurationDriftException(string message) : base(message)
    {
        IndexName = string.Empty;
        ChangedFields = [];
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the drift.</param>
    /// <param name="innerException">The underlying cause.</param>
    public GrainIndexConfigurationDriftException(string message, Exception innerException)
        : base(message, innerException)
    {
        IndexName = string.Empty;
        ChangedFields = [];
    }

    /// <summary>
    /// Initialises a new instance naming the index and the drift-breaking fields
    /// that changed. The primary production throw shape.
    /// </summary>
    /// <param name="indexName">The drifted index's logical name. Must not be <c>null</c>.</param>
    /// <param name="changedFields">
    /// The drift-breaking fields that changed. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexConfigurationDriftException(
        string indexName,
        IReadOnlyList<GrainIndexDefinitionField> changedFields)
        : base(BuildMessage(indexName, changedFields))
    {
        IndexName = indexName;
        ChangedFields = changedFields;
    }

    private static string BuildMessage(
        string indexName,
        IReadOnlyList<GrainIndexDefinitionField> changedFields)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(changedFields);

        // Startup-time diagnostics: a string.Join over a small list is the
        // clearest rendering and runs once per drifted index, never on a
        // request path.
        var fields = changedFields.Count == 0
            ? "(none reported)"
            : string.Join(", ", changedFields);

        return $"Grain index '{indexName}' has drifted from the declaration its stored entries "
            + $"were written under. Changed drift-breaking fields: {fields}. Entries already on "
            + "the index tree are encoded under the previous declaration, so honouring the new "
            + "one would return incorrect query results. Either revert the declaration to match "
            + "the stored one, or set this index's DriftPolicy to "
            + $"{nameof(GrainIndexDriftPolicy)}.{nameof(GrainIndexDriftPolicy.Rebuild)} to accept "
            + "the change and schedule a backfill rebuild.";
    }
}
