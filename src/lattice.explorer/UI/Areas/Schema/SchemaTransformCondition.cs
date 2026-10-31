namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The condition of a conditional set step (#5476): the step writes its constant only to a
/// value whose member compares as stated with this constant, and every other value keeps the
/// member it has.
/// </summary>
/// <param name="Operator">How the member is compared with <paramref name="Value"/>.</param>
/// <param name="ValueKind">The kind of constant the member is compared with.</param>
/// <param name="Value">The constant as typed; <see langword="null"/> for <see cref="SchemaConstantKind.Null"/>.</param>
internal sealed record SchemaTransformCondition(
    LatticeComparisonOperator Operator,
    SchemaConstantKind ValueKind,
    string? Value)
{
    /// <summary>The comparisons a condition offers, in the order the editor lists them, with their words.</summary>
    public static IReadOnlyList<(LatticeComparisonOperator Operator, string Words)> Comparisons { get; } =
    [
        (LatticeComparisonOperator.LessThan, "less than"),
        (LatticeComparisonOperator.LessThanOrEqual, "at most"),
        (LatticeComparisonOperator.Equal, "equal to"),
        (LatticeComparisonOperator.NotEqual, "not equal to"),
        (LatticeComparisonOperator.GreaterThanOrEqual, "at least"),
        (LatticeComparisonOperator.GreaterThan, "greater than"),
    ];

    /// <summary>The words for <paramref name="comparison"/>, such as "less than".</summary>
    /// <param name="comparison">The comparison.</param>
    public static string WordsOf(LatticeComparisonOperator comparison)
    {
        foreach (var (candidate, words) in Comparisons)
        {
            if (candidate == comparison)
            {
                return words;
            }
        }

        return comparison.ToString();
    }

    /// <summary>The condition read back, such as "is less than 0".</summary>
    public string Describe() => $"is {WordsOf(Operator)} {SchemaTransformStep.Constant(ValueKind, Value)}";
}
