namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// Compares a live grain-index declaration against the record persisted for it
/// and reports exactly which declaration fields changed.
/// <para>
/// The fingerprint answers "did anything drift-significant change" in one
/// equality check. This detector answers the follow-up question the operator
/// actually needs - <i>which</i> fields - and additionally covers the drift-safe
/// fields the fingerprint deliberately excludes, so a replication opt-in flipped
/// on its own is still reported and still refreshes the stored record.
/// </para>
/// </summary>
internal static class GrainIndexDriftDetector
{
    /// <summary>
    /// Reports the declaration fields on which <paramref name="current"/>
    /// differs from <paramref name="stored"/>.
    /// </summary>
    /// <param name="stored">The persisted record of truth. Must not be <c>null</c>.</param>
    /// <param name="current">The live declaration's descriptor. Must not be <c>null</c>.</param>
    /// <param name="currentKeyCodecId">
    /// The live declaration's grain-key codec identity. Must not be <c>null</c>.
    /// </param>
    /// <returns>The drift report.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    internal static GrainIndexDriftReport Detect(
        GrainIndexRegistryRecord stored,
        GrainIndexDescriptor current,
        string currentKeyCodecId)
    {
        ArgumentNullException.ThrowIfNull(stored);
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(currentKeyCodecId);

        var previous = stored.Descriptor;

        // Reconciliation runs once per index per silo start, so the readable
        // list build is the right trade here; nothing on this path is hot.
        List<GrainIndexDefinitionField>? changed = null;

        // Reported for completeness even though the name is the registry key a
        // record is filed under, so in practice a mismatch cannot reach here.
        Compare(ref changed, GrainIndexDefinitionField.Name, previous.Name, current.Name);
        Compare(ref changed, GrainIndexDefinitionField.TreeName, previous.TreeName, current.TreeName);
        Compare(
            ref changed,
            GrainIndexDefinitionField.GrainInterfaceType,
            previous.GrainInterfaceTypeName,
            current.GrainInterfaceTypeName);
        Compare(
            ref changed,
            GrainIndexDefinitionField.StateType,
            previous.StateTypeName,
            current.StateTypeName);
        Compare(
            ref changed,
            GrainIndexDefinitionField.KeyCodec,
            stored.KeyCodecId,
            currentKeyCodecId);

        if (!PropertiesEqual(previous.Properties, current.Properties))
        {
            (changed ??= []).Add(GrainIndexDefinitionField.Properties);
        }

        if (previous.AllowReplication != current.AllowReplication)
        {
            (changed ??= []).Add(GrainIndexDefinitionField.AllowReplication);
        }

        return changed is null ? GrainIndexDriftReport.None : new GrainIndexDriftReport(changed);
    }

    private static void Compare(
        ref List<GrainIndexDefinitionField>? changed,
        GrainIndexDefinitionField field,
        string previous,
        string current)
    {
        if (!string.Equals(previous, current, StringComparison.Ordinal))
        {
            (changed ??= []).Add(field);
        }
    }

    /// <summary>
    /// Whether the two projected-property lists are equal as ordered sequences.
    /// Order is significant: the projected set is an ordered tuple in the entry
    /// encoding, so reordering it is as breaking as replacing a member.
    /// </summary>
    private static bool PropertiesEqual(
        IReadOnlyList<GrainIndexPropertyDescriptor> previous,
        IReadOnlyList<GrainIndexPropertyDescriptor> current)
    {
        if (previous.Count != current.Count)
        {
            return false;
        }

        for (var i = 0; i < previous.Count; i++)
        {
            // GrainIndexPropertyDescriptor is a record struct over two strings,
            // so its generated equality is ordinal value equality.
            if (previous[i] != current[i])
            {
                return false;
            }
        }

        return true;
    }
}
