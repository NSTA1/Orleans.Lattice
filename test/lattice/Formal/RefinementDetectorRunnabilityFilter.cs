namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Evaluates the vstest filter expressions <c>plan-test-matrix.py</c> emits
/// against one test, so <see cref="RefinementDetectorRunnabilityLanes"/> can
/// answer "does any CI lane select this test" from the planner's own output
/// rather than from a hand-kept copy of its tiers.
/// <para>
/// THE GRAMMAR IS THE PLANNER'S SUBSET: conditions on <c>FullyQualifiedName</c>
/// and <c>TestCategory</c> with <c>=</c>, <c>!=</c>, <c>~</c> and <c>!~</c>,
/// joined by <c>&amp;</c> (binding tighter) and <c>|</c>, with parentheses.
/// Comparison is case-insensitive, as in vstest. Anything else throws rather
/// than being guessed, because a guessed filter answers "selected" or "not
/// selected" with equal confidence.
/// </para>
/// <para>
/// A NEGATED CATEGORY CONDITION IS UNIVERSAL. <c>TestCategory!=X</c> holds only
/// when NO category of the test is <c>X</c>, which is how NUnit3TestAdapter
/// translates it (the epic #5148 trace spike and the TLC partition check both
/// measured it). The toolchain-free TLC partition check in
/// <c>CiMemberPullRequestTieringTests</c> evaluates the same grammar with the
/// same semantics through a copy private to that fixture.
/// </para>
/// </summary>
internal static class RefinementDetectorRunnabilityFilter
{
    /// <summary>
    /// Compiles <paramref name="filter"/> into a predicate over a test's full
    /// name and categories. A null or empty filter selects every test.
    /// </summary>
    public static Func<string, IReadOnlySet<string>, bool> Compile(string? filter)
    {
        if (string.IsNullOrEmpty(filter))
        {
            return static (_, _) => true;
        }

        var position = 0;
        var predicate = Or(filter, ref position);
        if (position != filter.Length)
        {
            throw new FormatException($"unparsed filter tail at {position}: {filter}");
        }

        return predicate;
    }

    private static Func<string, IReadOnlySet<string>, bool> Or(string text, ref int position)
    {
        var terms = new List<Func<string, IReadOnlySet<string>, bool>> { And(text, ref position) };
        while (position < text.Length && text[position] == '|')
        {
            position++;
            terms.Add(And(text, ref position));
        }

        return terms.Count == 1 ? terms[0] : (name, categories) => terms.Any(term => term(name, categories));
    }

    private static Func<string, IReadOnlySet<string>, bool> And(string text, ref int position)
    {
        var factors = new List<Func<string, IReadOnlySet<string>, bool>> { Factor(text, ref position) };
        while (position < text.Length && text[position] == '&')
        {
            position++;
            factors.Add(Factor(text, ref position));
        }

        return factors.Count == 1 ? factors[0] : (name, categories) => factors.All(factor => factor(name, categories));
    }

    private static Func<string, IReadOnlySet<string>, bool> Factor(string text, ref int position)
    {
        if (position >= text.Length)
        {
            throw new FormatException($"filter ends where a condition was expected: {text}");
        }

        if (text[position] == '(')
        {
            position++;
            var inner = Or(text, ref position);
            if (position >= text.Length || text[position] != ')')
            {
                throw new FormatException($"unbalanced parentheses: {text}");
            }

            position++;
            return inner;
        }

        var end = position;
        while (end < text.Length && text[end] is not ('&' or '|' or ')'))
        {
            end++;
        }

        var condition = text[position..end];
        position = end;
        return Condition(condition);
    }

    private static Func<string, IReadOnlySet<string>, bool> Condition(string condition)
    {
        foreach (var op in new[] { "!=", "!~", "=", "~" })
        {
            var at = condition.IndexOf(op, StringComparison.Ordinal);
            if (at <= 0)
            {
                continue;
            }

            var property = condition[..at];
            var value = condition[(at + op.Length)..];
            Func<string, bool> test = op.EndsWith('~')
                ? candidate => candidate.Contains(value, StringComparison.OrdinalIgnoreCase)
                : candidate => string.Equals(candidate, value, StringComparison.OrdinalIgnoreCase);
            Func<string, IReadOnlySet<string>, IEnumerable<string>> values = property switch
            {
                "FullyQualifiedName" => static (name, _) => [name],
                "TestCategory" or "Category" => static (_, categories) => categories,
                _ => throw new FormatException($"filter property '{property}' is not modelled by this evaluator"),
            };
            return op.StartsWith('!')
                ? (name, categories) => !values(name, categories).Any(test)
                : (name, categories) => values(name, categories).Any(test);
        }

        throw new FormatException($"'{condition}' is not a filter condition");
    }
}
