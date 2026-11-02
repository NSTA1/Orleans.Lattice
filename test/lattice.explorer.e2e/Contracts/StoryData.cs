using System.Globalization;
using System.Text;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>One key and its value, as a file or a seeder writes it.</summary>
/// <param name="Key">The key.</param>
/// <param name="Json">The value: a single-line JSON document, written verbatim as UTF-8.</param>
public sealed record StoryEntry(string Key, string Json)
{
    /// <summary>The value's bytes.</summary>
    public byte[] Value => Encoding.UTF8.GetBytes(Json);
}

/// <summary>
/// Every data set the story imports, pastes or seeds, generated deterministically in one place
/// (#5364): a journey imports the file <see cref="WriteFile"/> writes, and a seeder writes the
/// same entries through the Data API, so the two paths produce the same keys and values. A
/// JSON line is written to the tree verbatim, so an NDJSON line and the value a seeder writes
/// are byte-identical.
/// </summary>
public static class StoryData
{
    private static readonly string[] Statuses = ["open", "shipped", "cancelled"];

    /// <summary>The first order number of the October archive (U1).</summary>
    public const int ArchiveFirstOrder = 200_001;

    /// <summary>The first order the corrections file corrects.</summary>
    public const int FirstCorrectedOrder = 110;

    /// <summary>The order the corrections refuse for a negative total.</summary>
    public const int RefusedNegativeOrder = FirstCorrectedOrder + Names.Files.CorrectionsApplied;

    /// <summary>The order the corrections refuse for a status outside the set.</summary>
    public const int RefusedStatusOrder = RefusedNegativeOrder + 1;

    /// <summary>The status <see cref="RefusedStatusOrder"/>'s correction carries, and U1's diverted edit of <c>order/000101</c>.</summary>
    public const string InvalidStatus = "lost";

    /// <summary>The order's seeded total: positive, except the three negative orders of the decision record.</summary>
    /// <param name="number">The order number, 1 to 240.</param>
    public static decimal SeededTotal(int number)
    {
        var total = 10m + (number % 97) + ((number % 4) * 0.25m);
        return Names.Orders.NegativeTotals.Contains(Names.Orders.Key(number)) ? -total : total;
    }

    /// <summary>The order's seeded status.</summary>
    /// <param name="number">The order number.</param>
    public static string SeededStatus(int number) => Statuses[number % Statuses.Length];

    /// <summary>An order's JSON, in the line feed's shape plus the <c>id</c> the key template reads.</summary>
    /// <param name="number">The order number.</param>
    /// <param name="total">The total.</param>
    /// <param name="status">The status.</param>
    public static string OrderJson(int number, decimal total, string status) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"id\":\"{number:D6}\",\"order\":{number},\"total\":{total},\"status\":\"{status}\",\"line\":\"line-{(number % 3) + 1}\"}}");

    /// <summary>A3's 240 orders: <c>orders-2026-10.ndjson</c>, key template <c>order/{/id}</c>.</summary>
    public static IReadOnlyList<StoryEntry> Orders { get; } =
        [.. Enumerable.Range(1, Names.Orders.SeededCount)
            .Select(number => new StoryEntry(Names.Orders.Key(number), OrderJson(number, SeededTotal(number), SeededStatus(number))))];

    /// <summary>The seeded order <paramref name="number"/>.</summary>
    /// <param name="number">The order number, 1 to 240.</param>
    public static StoryEntry Order(int number) => Orders[number - 1];

    /// <summary>
    /// A3's twelve production lines, pasted into the Bulk load panel as <c>key=value</c> lines
    /// (<see cref="LinesPaste"/>): keys <c>line/01</c> to <c>line/12</c>, values JSON.
    /// </summary>
    public static IReadOnlyList<StoryEntry> Lines { get; } =
        [.. Enumerable.Range(1, Names.Lines.Count).Select(index => new StoryEntry(LineKey(index), LineJson(index, 40 + index)))];

    /// <summary>The key of line <paramref name="index"/>.</summary>
    /// <param name="index">1 to 12.</param>
    public static string LineKey(int index) => string.Create(CultureInfo.InvariantCulture, $"line/{index:D2}");

    /// <summary>Line <paramref name="index"/>'s JSON at <paramref name="rate"/>.</summary>
    /// <param name="index">1 to 12.</param>
    /// <param name="rate">The line's rate.</param>
    public static string LineJson(int index, int rate)
    {
        var tags = (index % 3) switch
        {
            0 => "[\"press\"]",
            1 => "[\"oven\",\"heat\"]",
            _ => "[\"conveyor\"]",
        };
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"line\":\"L{index}\",\"rate\":{rate},\"owner\":\"ops@contoso.example\",\"since\":\"2026-10-{index:D2}\",\"tags\":{tags}}}");
    }

    /// <summary>The twelve lines as the Bulk load panel takes them: one <c>key=value</c> per line.</summary>
    public static string LinesPaste => string.Join('\n', Lines.Select(line => line.Key + "=" + line.Json));

    /// <summary>The two lines A10 edits before its incremental capture, and their edited rates.</summary>
    public static IReadOnlyList<StoryEntry> LinesEditedBeforeIncrement { get; } =
        [new(LineKey(3), LineJson(3, 90)), new(LineKey(7), LineJson(7, 91))];

    /// <summary>The line A10 deletes and repairs back from <c>lines-full</c>.</summary>
    public static string LineDeletedBeforeRepair => LineKey(5);

    /// <summary>
    /// U1's October archive: <c>archive-2026-10.csv</c>, 5,000 rows, key template
    /// <c>order/{id}</c>, each row stored as a JSON object of its columns as text.
    /// </summary>
    public static IReadOnlyList<StoryEntry> Archive { get; } =
        [.. Enumerable.Range(ArchiveFirstOrder, Names.Files.ArchiveRows).Select(number =>
        {
            var row = ArchiveRow(number);
            return new StoryEntry(
                "order/" + row[0],
                string.Create(CultureInfo.InvariantCulture, $"{{\"id\":\"{row[0]}\",\"total\":\"{row[1]}\",\"status\":\"{row[2]}\",\"line\":\"{row[3]}\"}}"));
        })];

    /// <summary>
    /// U1's corrections: <c>corrections.ndjson</c>, 50 upserts of <c>factory-orders</c>. The first
    /// 48 restate an order with a total one and a half higher; the last two are refused by A4's
    /// policy: one total is negative, one status is outside the set.
    /// </summary>
    public static IReadOnlyList<StoryEntry> Corrections { get; } =
        [
            .. Enumerable.Range(FirstCorrectedOrder, Names.Files.CorrectionsApplied)
                .Select(number => new StoryEntry(Names.Orders.Key(number), OrderJson(number, CorrectedTotal(number), SeededStatus(number)))),
            new(Names.Orders.Key(RefusedNegativeOrder), OrderJson(RefusedNegativeOrder, -1m, SeededStatus(RefusedNegativeOrder))),
            new(Names.Orders.Key(RefusedStatusOrder), OrderJson(RefusedStatusOrder, SeededTotal(RefusedStatusOrder), InvalidStatus)),
        ];

    /// <summary>The total a correction gives order <paramref name="number"/>.</summary>
    /// <param name="number">A corrected order.</param>
    public static decimal CorrectedTotal(int number) => SeededTotal(number) + 1.5m;

    /// <summary>The orders the corrections apply to.</summary>
    public static IEnumerable<int> CorrectedOrders => Enumerable.Range(FirstCorrectedOrder, Names.Files.CorrectionsApplied);

    /// <summary>The content of a story file named in <see cref="StoryNames"/>.</summary>
    /// <param name="name">A file name from <see cref="StoryNames.Contoso.Files"/>.</param>
    /// <exception cref="ArgumentException">The story has no such file, or it is generated on demand.</exception>
    public static string FileContent(string name) => name switch
    {
        Names.Files.OrdersNdjson => Ndjson(Orders),
        Names.Files.CorrectionsNdjson => Ndjson(Corrections),
        Names.Files.ArchiveCsv => ArchiveCsv(),
        _ => throw new ArgumentException($"The story has no generated file '{name}'.", nameof(name)),
    };

    /// <summary>Writes the story file <paramref name="name"/> under <paramref name="directory"/> and returns its path.</summary>
    /// <param name="directory">The directory, created if missing.</param>
    /// <param name="name">A file name from <see cref="StoryNames.Contoso.Files"/>.</param>
    public static string WriteFile(string directory, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, name);
        File.WriteAllText(path, FileContent(name), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        return path;
    }

    /// <summary>
    /// Writes U1's 500,000-line <c>history-2025.ndjson</c>, which the journey imports and cancels,
    /// under <paramref name="directory"/> and returns its path. It is streamed, never held whole.
    /// </summary>
    /// <param name="directory">The directory, created if missing.</param>
    public static string WriteHistoryFile(string directory)
    {
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, Names.Files.History2025Ndjson);
        using var writer = new StreamWriter(path, append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        for (var number = 1; number <= Names.Files.History2025Lines; number++)
        {
            writer.Write(OrderJson(number, Math.Abs(SeededTotal(1 + (number % 239))), SeededStatus(number)));
            writer.Write('\n');
        }

        return path;
    }

    private static string[] ArchiveRow(int number) =>
    [
        number.ToString(CultureInfo.InvariantCulture),
        (12m + (number % 89) + ((number % 2) * 0.5m)).ToString("0.00", CultureInfo.InvariantCulture),
        Statuses[number % Statuses.Length],
        string.Create(CultureInfo.InvariantCulture, $"line-{(number % 3) + 1}"),
    ];

    private static string ArchiveCsv()
    {
        var builder = new StringBuilder("id,total,status,line\n");
        for (var number = ArchiveFirstOrder; number < ArchiveFirstOrder + Names.Files.ArchiveRows; number++)
        {
            builder.AppendJoin(',', ArchiveRow(number)).Append('\n');
        }

        return builder.ToString();
    }

    private static string Ndjson(IEnumerable<StoryEntry> entries) =>
        string.Concat(entries.Select(entry => entry.Json + "\n"));
}
