using System.Globalization;

namespace Orleans.Lattice.Storage.AzureTable.Tests.Fakes;

/// <summary>
/// Parses the exact OData filter subset the WAL provider emits into a
/// predicate over <see cref="AzureTableWalEntity"/>.
/// <para>
/// The provider builds every filter itself from a fixed set of shapes, so the
/// grammar needed here is small and closed: <c>and</c>-joined comparisons of
/// <c>PartitionKey</c> or <c>RowKey</c> against a single-quoted literal, and of
/// <c>Offset</c> against a bare integer, using <c>eq</c>, <c>ne</c>, <c>ge</c>,
/// <c>gt</c>, <c>le</c>, or <c>lt</c>. String comparisons are ordinal, matching
/// how Azure Tables orders and ranges keys.
/// </para>
/// <para>
/// The parser is deliberately strict: an operator, column, or clause shape it
/// does not recognise throws rather than silently degrading to "match
/// everything". A permissive parser would turn a provider-side filter
/// regression into a passing test, which is the opposite of what these
/// fixtures exist to detect.
/// </para>
/// </summary>
internal static class WalTableFilter
{
    /// <summary>Compiles <paramref name="filter"/> into a row predicate.</summary>
    public static Func<AzureTableWalEntity, bool> Parse(string filter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);

        var clauses = SplitClauses(filter).Select(ParseClause).ToArray();
        return entity => clauses.All(clause => clause(entity));
    }

    /// <summary>
    /// Splits on the top-level <c>and</c> separators. Splitting on the literal
    /// <c>" and "</c> would corrupt any key containing that substring, so the
    /// scan tracks whether it is inside a single-quoted literal.
    /// </summary>
    private static List<string> SplitClauses(string filter)
    {
        var clauses = new List<string>();
        var start = 0;
        var inQuotes = false;

        for (var i = 0; i < filter.Length; i++)
        {
            if (filter[i] == '\'')
            {
                // An escaped quote inside a literal is doubled, exactly as the
                // provider's Escape helper writes it; skipping the pair keeps
                // the quote-depth tracking correct.
                if (inQuotes && i + 1 < filter.Length && filter[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                inQuotes = !inQuotes;
                continue;
            }

            if (inQuotes)
            {
                continue;
            }

            if (i + 5 <= filter.Length && filter.AsSpan(i, 5).SequenceEqual(" and "))
            {
                clauses.Add(filter[start..i]);
                i += 4;
                start = i + 1;
            }
        }

        clauses.Add(filter[start..]);
        return clauses;
    }

    private static Func<AzureTableWalEntity, bool> ParseClause(string clause)
    {
        var trimmed = clause.Trim();
        var firstSpace = trimmed.IndexOf(' ', StringComparison.Ordinal);
        if (firstSpace < 0)
        {
            throw new InvalidOperationException($"Malformed filter clause '{clause}'.");
        }

        var column = trimmed[..firstSpace];
        var rest = trimmed[(firstSpace + 1)..].TrimStart();
        var secondSpace = rest.IndexOf(' ', StringComparison.Ordinal);
        if (secondSpace < 0)
        {
            throw new InvalidOperationException($"Malformed filter clause '{clause}'.");
        }

        var op = rest[..secondSpace];
        var literal = rest[(secondSpace + 1)..].Trim();

        return column switch
        {
            nameof(AzureTableWalEntity.PartitionKey) =>
                StringClause(op, Unquote(literal), entity => entity.PartitionKey),
            nameof(AzureTableWalEntity.RowKey) =>
                StringClause(op, Unquote(literal), entity => entity.RowKey),
            nameof(AzureTableWalEntity.Offset) =>
                LongClause(op, long.Parse(literal, CultureInfo.InvariantCulture), entity => entity.Offset),
            _ => throw new InvalidOperationException($"Unsupported filter column '{column}'."),
        };
    }

    private static string Unquote(string literal)
    {
        if (literal.Length < 2 || literal[0] != '\'' || literal[^1] != '\'')
        {
            throw new InvalidOperationException($"Expected a quoted literal but found '{literal}'.");
        }

        return literal[1..^1].Replace("''", "'", StringComparison.Ordinal);
    }

    private static Func<AzureTableWalEntity, bool> StringClause(
        string op,
        string literal,
        Func<AzureTableWalEntity, string> selector) =>
        op switch
        {
            "eq" => entity => string.CompareOrdinal(selector(entity), literal) == 0,
            "ne" => entity => string.CompareOrdinal(selector(entity), literal) != 0,
            "ge" => entity => string.CompareOrdinal(selector(entity), literal) >= 0,
            "gt" => entity => string.CompareOrdinal(selector(entity), literal) > 0,
            "le" => entity => string.CompareOrdinal(selector(entity), literal) <= 0,
            "lt" => entity => string.CompareOrdinal(selector(entity), literal) < 0,
            _ => throw new InvalidOperationException($"Unsupported filter operator '{op}'."),
        };

    private static Func<AzureTableWalEntity, bool> LongClause(
        string op,
        long literal,
        Func<AzureTableWalEntity, long> selector) =>
        op switch
        {
            "eq" => entity => selector(entity) == literal,
            "ne" => entity => selector(entity) != literal,
            "ge" => entity => selector(entity) >= literal,
            "gt" => entity => selector(entity) > literal,
            "le" => entity => selector(entity) <= literal,
            "lt" => entity => selector(entity) < literal,
            _ => throw new InvalidOperationException($"Unsupported filter operator '{op}'."),
        };
}
