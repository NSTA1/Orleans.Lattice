using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Whether a grain index's live declaration still matches the one its stored
/// entries were written under, and if not, exactly which declaration fields
/// moved.
/// </summary>
/// <remarks>
/// <para>
/// This is the operator-visible form of the drift the registry reconciler
/// evaluates at silo start. It answers two different questions on purpose.
/// <see cref="HasDrift"/> covers any change at all, including the drift-safe
/// ones (a replication opt-in, say) that only refresh the stored record.
/// <see cref="HasBreakingChange"/> covers the subset that invalidates the
/// entries already written, which is what schedules a rebuild or fails start-up
/// depending on <see cref="GrainIndexOptions.DriftPolicy"/>.
/// </para>
/// <para>
/// An index with no stored record - one this cluster has never reconciled -
/// reports <see cref="None"/> rather than drift: there is nothing to have
/// drifted from.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexDriftStatus)]
public sealed class GrainIndexDriftStatus
{
    private static readonly GrainIndexDefinitionField[] NoFields = [];

    /// <summary>Initialises a drift status.</summary>
    /// <param name="changedFields">
    /// The declaration fields that changed, in
    /// <see cref="GrainIndexDefinitionField"/> order. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="changedFields"/> is <c>null</c>.</exception>
    public GrainIndexDriftStatus(IReadOnlyList<GrainIndexDefinitionField> changedFields)
    {
        ArgumentNullException.ThrowIfNull(changedFields);
        ChangedFields = changedFields;
    }

    /// <summary>
    /// The declaration fields that changed, in
    /// <see cref="GrainIndexDefinitionField"/> order. Empty when nothing
    /// drifted.
    /// </summary>
    [Id(0)]
    public IReadOnlyList<GrainIndexDefinitionField> ChangedFields { get; }

    /// <summary>Whether anything drifted at all, breaking or not.</summary>
    public bool HasDrift => ChangedFields.Count > 0;

    /// <summary>
    /// Whether any changed field is one
    /// <see cref="GrainIndexDriftClassification"/> classifies as
    /// drift-breaking, meaning the entries already written no longer describe
    /// the live declaration.
    /// </summary>
    public bool HasBreakingChange
    {
        get
        {
            for (var i = 0; i < ChangedFields.Count; i++)
            {
                if (GrainIndexDriftClassification.IsBreaking(ChangedFields[i]))
                    return true;
            }

            return false;
        }
    }

    /// <summary>
    /// The status of a declaration that matches its stored record exactly, and
    /// of an index that has no stored record yet.
    /// </summary>
    public static GrainIndexDriftStatus None { get; } = new(NoFields);
}
