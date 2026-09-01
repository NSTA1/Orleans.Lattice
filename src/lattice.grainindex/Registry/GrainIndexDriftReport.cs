namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// The outcome of comparing a live grain-index declaration against the record
/// persisted for it: which declaration fields changed, and whether any of them
/// is drift-breaking.
/// </summary>
/// <remarks>
/// A <c>readonly record struct</c> because the report is a small, immutable
/// value produced once per index per silo start and never mutated. It is not
/// serialized and never leaves the process.
/// </remarks>
internal readonly record struct GrainIndexDriftReport
{
    /// <summary>Initialises a report.</summary>
    /// <param name="changedFields">
    /// The declaration fields that changed, in
    /// <see cref="GrainIndexDefinitionField"/> order. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="changedFields"/> is <c>null</c>.</exception>
    internal GrainIndexDriftReport(IReadOnlyList<GrainIndexDefinitionField> changedFields)
    {
        ArgumentNullException.ThrowIfNull(changedFields);
        ChangedFields = changedFields;
    }

    /// <summary>
    /// A report describing a declaration that matches its stored record exactly.
    /// </summary>
    internal static GrainIndexDriftReport None { get; } = new([]);

    /// <summary>
    /// The declaration fields that changed, in
    /// <see cref="GrainIndexDefinitionField"/> order. Empty when nothing drifted.
    /// </summary>
    internal IReadOnlyList<GrainIndexDefinitionField> ChangedFields { get; }

    /// <summary>Whether anything drifted at all.</summary>
    internal bool HasDrift => ChangedFields.Count > 0;

    /// <summary>
    /// The drift-breaking subset of <see cref="ChangedFields"/>, allocated only
    /// when there is drift to report. Empty when every change is drift-safe.
    /// </summary>
    internal IReadOnlyList<GrainIndexDefinitionField> BreakingFields()
    {
        if (ChangedFields.Count == 0)
        {
            return [];
        }

        List<GrainIndexDefinitionField>? breaking = null;
        for (var i = 0; i < ChangedFields.Count; i++)
        {
            var field = ChangedFields[i];
            if (!GrainIndexDriftClassification.IsBreaking(field))
            {
                continue;
            }

            breaking ??= new List<GrainIndexDefinitionField>(ChangedFields.Count);
            breaking.Add(field);
        }

        return breaking ?? (IReadOnlyList<GrainIndexDefinitionField>)[];
    }

    /// <summary>Whether any changed field is drift-breaking.</summary>
    internal bool HasBreakingChange()
    {
        for (var i = 0; i < ChangedFields.Count; i++)
        {
            if (GrainIndexDriftClassification.IsBreaking(ChangedFields[i]))
            {
                return true;
            }
        }

        return false;
    }
}
