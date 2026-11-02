namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// What the caller may change in a tree or at one key of it, as the data-plane
/// capability probe answered. Default-deny: <see cref="None"/> draws no write control.
/// </summary>
/// <param name="CanWrite">Whether the caller may write (create or replace a value).</param>
/// <param name="CanDelete">Whether the caller may delete.</param>
internal readonly record struct DataWriteRights(bool CanWrite, bool CanDelete)
{
    /// <summary>No write and no delete.</summary>
    public static DataWriteRights None => default;

    /// <summary>Whether any write control is drawn at all.</summary>
    public bool Any => CanWrite || CanDelete;
}
