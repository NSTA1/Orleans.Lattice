using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>
/// The Explorer's tables (<c>LtTable</c>): a booktabs table on a desktop, a list of
/// two-line rows on a phone. A row is found by its row header and one of its cells.
/// </summary>
internal static class Tables
{
    /// <summary>
    /// The rows under <paramref name="within"/> whose row header reads exactly
    /// <paramref name="header"/> and, when given, one of whose cells reads <paramref name="cell"/>
    /// as a whole word.
    /// </summary>
    /// <param name="page">The page.</param>
    /// <param name="within">Where the table is.</param>
    /// <param name="header">The row header, such as a rule id.</param>
    /// <param name="cell">A cell's text, such as a rule's scope, or <see langword="null"/>.</param>
    public static ILocator Rows(IPage page, ILocator within, string header, string? cell = null)
    {
        var rows = within.Locator("tbody tr, .lt-table-list li")
            .Filter(new() { Has = page.Locator("th, .lt-compact-row__primary", new() { HasTextRegex = Pickers.Exactly(header) }) });
        return cell is null
            ? rows
            : rows.Filter(new()
            {
                Has = page.Locator("td, .lt-compact-row__secondary", new() { HasTextRegex = new Regex($"(^|\\s){Regex.Escape(cell)}(\\s|$)") }),
            });
    }
}
