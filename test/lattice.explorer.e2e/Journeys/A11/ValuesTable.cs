namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A11;

/// <summary>
/// A Telemetry chart's values table, as the board's table view draws it: the time column then
/// one column per series, and each row's cell text.
/// </summary>
internal sealed class ValuesTable
{
    /// <summary>What a cell with no reading says.</summary>
    public const string NoReading = "no reading";

    /// <summary>The column headers: <c>Time (UTC)</c>, then each series' name.</summary>
    public string[] Columns { get; set; } = [];

    /// <summary>Each row's cells: the time, then each series' value as written.</summary>
    public string[][] Rows { get; set; } = [];

    /// <summary>The cell of <paramref name="series"/> in row <paramref name="row"/>.</summary>
    /// <param name="row">The row index.</param>
    /// <param name="series">The series name.</param>
    public string Cell(int row, string series)
    {
        var column = Array.IndexOf(Columns, series);
        Assert.That(column, Is.GreaterThan(0), $"the values table has a column for series '{series}' (columns: {string.Join(", ", Columns)})");
        return Rows[row][column];
    }

    /// <summary>The last reading of <paramref name="series"/> that is not <see cref="NoReading"/>, or <see cref="NoReading"/>.</summary>
    /// <param name="series">The series name.</param>
    public string LatestOf(string series)
    {
        for (var row = Rows.Length - 1; row >= 0; row--)
        {
            var cell = Cell(row, series);
            if (cell != NoReading)
            {
                return cell;
            }
        }

        return NoReading;
    }
}
