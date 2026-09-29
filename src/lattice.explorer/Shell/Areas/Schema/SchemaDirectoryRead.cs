namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>One read of the Schema directory.</summary>
/// <param name="Rows">Every inspected tree, ordered by id, governed or not.</param>
/// <param name="TreeCount">How many logical trees the catalogue listed.</param>
/// <param name="Truncated">Whether some trees were not inspected, because the cluster holds more than one listing asks about.</param>
/// <param name="ReadAt">When the listing was read.</param>
internal sealed record SchemaDirectoryRead(
    IReadOnlyList<SchemaTreeRow> Rows,
    int TreeCount,
    bool Truncated,
    DateTimeOffset ReadAt)
{
    /// <summary>The trees under schema.</summary>
    public IEnumerable<SchemaTreeRow> Governed => Rows.Where(row => row.IsGoverned);

    /// <summary>The row of <paramref name="treeId"/>, or <see langword="null"/> when it was not inspected.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The row.</returns>
    public SchemaTreeRow? Find(string treeId)
    {
        foreach (var row in Rows)
        {
            if (string.Equals(row.TreeId, treeId, StringComparison.Ordinal))
            {
                return row;
            }
        }

        return null;
    }

    /// <summary>This read with <paramref name="row"/> in place of the row of the same tree, when there is one.</summary>
    /// <param name="row">The fresh row.</param>
    /// <returns>The updated read.</returns>
    public SchemaDirectoryRead Replace(SchemaTreeRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        var rows = Rows.ToArray();
        for (var i = 0; i < rows.Length; i++)
        {
            if (string.Equals(rows[i].TreeId, row.TreeId, StringComparison.Ordinal))
            {
                rows[i] = row;
                return this with { Rows = rows };
            }
        }

        return this;
    }
}
