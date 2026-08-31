namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The authoritative classification of each <see cref="GrainIndexDefinitionField"/>
/// as drift-breaking or drift-safe, and the rule the index registry's startup
/// reconciliation branches on.
/// <para>
/// A field is <b>drift-breaking</b> when changing it invalidates index entries
/// already written under the previous declaration - because the entry's key
/// encoding, its value encoding, its ordering, or the tree it lives in is a
/// function of that field. Honouring such a change without a rebuild would
/// leave a reader querying data it no longer agrees with, so it is rejected by
/// default. A field is <b>drift-safe</b> when no stored entry depends on it, so
/// the stored record is simply refreshed.
/// </para>
/// </summary>
/// <remarks>
/// The classification is deliberately conservative: a field is drift-safe only
/// when it demonstrably cannot appear in an entry's encoding, its ordering, or
/// its location. Anything else is breaking, because the failure mode of getting
/// this wrong is a silently incorrect query result rather than an error.
/// </remarks>
public static class GrainIndexDriftClassification
{
    /// <summary>
    /// The fields whose change invalidates data already written under the
    /// previous declaration, in <see cref="GrainIndexDefinitionField"/> order.
    /// A change to any of these is rejected under
    /// <see cref="GrainIndexDriftPolicy.Reject"/>.
    /// </summary>
    public static IReadOnlyList<GrainIndexDefinitionField> BreakingFields { get; } =
    [
        GrainIndexDefinitionField.Name,
        GrainIndexDefinitionField.TreeName,
        GrainIndexDefinitionField.GrainInterfaceType,
        GrainIndexDefinitionField.StateType,
        GrainIndexDefinitionField.KeyCodec,
        GrainIndexDefinitionField.Properties,
    ];

    /// <summary>
    /// The fields whose change leaves existing index data valid, in
    /// <see cref="GrainIndexDefinitionField"/> order. A change confined to these
    /// updates the stored registry record and logs at
    /// <see cref="Microsoft.Extensions.Logging.LogLevel.Information"/>.
    /// </summary>
    public static IReadOnlyList<GrainIndexDefinitionField> SafeFields { get; } =
    [
        GrainIndexDefinitionField.AllowReplication,
    ];

    /// <summary>
    /// Reports whether a change to <paramref name="field"/> invalidates index
    /// entries already written under the previous declaration.
    /// </summary>
    /// <param name="field">The declaration field that changed.</param>
    /// <returns>
    /// <c>true</c> when the change is drift-breaking; <c>false</c> when it is
    /// drift-safe.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="field"/> is not a declared
    /// <see cref="GrainIndexDefinitionField"/>. An unclassified field is a
    /// defect rather than a default-safe case: silently treating an unknown
    /// field as safe is exactly the silent-corruption outcome this gate exists
    /// to prevent.
    /// </exception>
    public static bool IsBreaking(GrainIndexDefinitionField field) => field switch
    {
        GrainIndexDefinitionField.Name => true,
        GrainIndexDefinitionField.TreeName => true,
        GrainIndexDefinitionField.GrainInterfaceType => true,
        GrainIndexDefinitionField.StateType => true,
        GrainIndexDefinitionField.KeyCodec => true,
        GrainIndexDefinitionField.Properties => true,
        GrainIndexDefinitionField.AllowReplication => false,
        _ => throw new ArgumentOutOfRangeException(
            nameof(field),
            field,
            "The field is not classified as drift-breaking or drift-safe. Every declaration "
            + "field must be classified explicitly, because treating an unclassified one as "
            + "safe is what would let a breaking change through unnoticed."),
    };
}
