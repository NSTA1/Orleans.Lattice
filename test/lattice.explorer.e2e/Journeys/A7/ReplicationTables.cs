using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A7;

/// <summary>
/// Reads the Replication area's tables the way an operator does: a row by the tree (or peer)
/// that heads it, and a cell by its column's title. At the desktop width a table is a
/// booktabs table; below the compact width it is a list of two-line rows, so the row is the
/// list item that names the same thing.
/// </summary>
internal static class ReplicationTables
{
    /// <summary>The desktop rows, or the compact list items, in <paramref name="scope"/> headed by <paramref name="name"/>.</summary>
    /// <param name="scope">The area's content.</param>
    /// <param name="name">The row header's text: a tree id, or a peer region on a tree's page.</param>
    /// <param name="compact">Whether the page draws its compact chrome.</param>
    public static ILocator Rows(ILocator scope, string name, bool compact)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(name);
        return compact
            ? scope.GetByRole(AriaRole.Listitem).Filter(new() { Has = scope.Page.GetByText(name, new() { Exact = true }) })
            : scope.Locator("tbody tr").Filter(new() { Has = scope.Page.GetByRole(AriaRole.Rowheader, new() { Name = name, Exact = true }) });
    }

    /// <summary>The text of <paramref name="row"/>'s cell under the column titled <paramref name="column"/> (desktop only).</summary>
    /// <param name="row">A desktop table row.</param>
    /// <param name="column">The column's title.</param>
    public static async Task<string> CellAsync(ILocator row, string column)
    {
        ArgumentNullException.ThrowIfNull(row);
        var table = row.Locator("xpath=ancestor::table[1]");
        var titles = await table.Locator("thead th").AllInnerTextsAsync();
        var index = titles.Select(title => title.Trim()).ToList().IndexOf(column);
        if (index < 0)
        {
            throw new InvalidOperationException($"The table has no column titled '{column}'; its columns are {string.Join(", ", titles.Select(title => $"'{title.Trim()}'"))}.");
        }

        return (await row.Locator("th, td").Nth(index).InnerTextAsync()).Trim();
    }
}
