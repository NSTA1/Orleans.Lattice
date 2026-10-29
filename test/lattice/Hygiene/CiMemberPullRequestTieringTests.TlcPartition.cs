using System.Text.Json;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;
using NUnit.Framework.Internal.Builders;
using Orleans.Lattice.Tests.Formal;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The toolchain-free TLC partition check (#5149). A change that touches only
/// TLC routing (test-shards.json, the planner, <c>TlcCiShard</c>) skips the
/// exhaustive TLC shards, so nothing would notice a routing change that drops a
/// TLC case from every leg or runs it twice. This check builds every TLC case
/// with NUnit's own suite builder (so names and categories are what the adapter
/// reports), evaluates the filters <c>plan-test-matrix.py</c> actually emits for
/// a full plan, and requires every TLC case to be selected by exactly one item,
/// of a TLC shard, and the smoke shard to select exactly the smoke case. No JVM
/// is needed, so it runs wherever the hygiene fixtures run.
/// </summary>
public sealed partial class CiMemberPullRequestTieringTests
{
    private const string SmokeCaseFullName = "Orleans.Lattice.Tests.Formal.TlcModelCheckTests." + SpecModuleCases.SmokeTestName;

    private sealed record TlcCase(string FullName, IReadOnlySet<string> Categories);

    private sealed record FilteredItem(string Label, string Shard, string? Filter, bool Tlc, bool Smoke);

    [Test]
    public void Every_TLC_case_is_selected_by_exactly_one_TLC_shard_and_smoke_selects_only_the_smoke_case()
    {
        var cases = TlcCases();
        var items = FullPlanItems();

        Assert.Multiple(() =>
        {
            Assert.That(cases, Is.Not.Empty, "no TLC case was built, so the partition check proves nothing");
            Assert.That(items.Where(item => item.Tlc), Is.Not.Empty);
            Assert.That(items.Where(item => item.Smoke), Is.Not.Empty);
            Assert.That(PartitionProblems(cases, items), Is.Empty);
        });
    }

    [Test]
    public void The_TLC_case_population_matches_the_case_sources()
    {
        var cases = TlcCases();
        var sourced = SpecModuleCases.TlcModules().Count() + SpecModuleCases.TlcVariants().Count() + SpecModuleCases.TlcMutations().Count();
        string[] sourcedMethods = ["The_base_specification_holds(", "Each_variant_configuration_holds(", "Each_property_fires_under_its_mutation_and_not_on_the_base("];

        Assert.That(sourced, Is.GreaterThan(0));
        Assert.That(
            cases.Count(c => sourcedMethods.Any(method => c.FullName.Contains(".TlcModelCheckTests." + method, StringComparison.Ordinal))),
            Is.EqualTo(sourced),
            "the suite builder must yield one case per TlcModules/TlcVariants/TlcMutations entry");
        Assert.That(cases.Select(c => c.FullName), Does.Contain(SmokeCaseFullName));
        Assert.That(cases.Select(c => c.FullName), Is.Unique);
    }

    [Test]
    public void Negative_control_a_shard_whose_filter_drops_its_cases_fails()
    {
        var cases = TlcCases();
        var items = FullPlanItems();
        var victim = items.First(item => item.Tlc && item.Filter is not null
            && item.Filter.Contains("TestCategory=TlcShard", StringComparison.Ordinal)
            && cases.Any(c => SelectsCase(item.Filter, c)));
        var category = victim.Filter!.Split("TestCategory=", 2)[1].Split(')', '&', '|')[0];
        var dropped = cases.First(c => SelectsCase(victim.Filter, c));
        var mutated = items.Select(item => item == victim
            ? item with { Filter = $"({item.Filter})&TestCategory!={category}" }
            : item).ToList();

        Assert.That(PartitionProblems(cases, mutated), Has.Some.Contains(dropped.FullName).And.Some.Contains("no item"));
    }

    [Test]
    public void Negative_control_two_shards_claiming_one_case_fails()
    {
        var cases = TlcCases();
        var items = FullPlanItems();
        var claimed = items.First(item => item.Tlc && cases.Any(c => SelectsCase(item.Filter, c)));
        var duplicated = items.Append(claimed with { Label = claimed.Label + " (copy)", Shard = claimed.Shard + "-copy" }).ToList();

        Assert.That(PartitionProblems(cases, duplicated), Has.Some.Contains("2 items"));
    }

    [Test]
    public void Negative_control_a_smoke_shard_that_selects_more_than_the_smoke_case_fails()
    {
        var cases = TlcCases();
        var items = FullPlanItems();
        var widened = items.Select(item => item.Smoke
            ? item with { Filter = "FullyQualifiedName~Orleans.Lattice.Tests.Formal.TlcModelCheckTests" }
            : item).ToList();

        Assert.That(PartitionProblems(cases, widened), Has.Some.Contains("smoke"));
    }

    [TestCase("TestCategory=A", "X.Y.M", "A", true)]
    [TestCase("TestCategory!=A", "X.Y.M", "A", false)]
    [TestCase("FullyQualifiedName~X.Y", "X.Y.M(a)", "", true)]
    [TestCase("FullyQualifiedName!~X.Y", "X.Y.M(a)", "", false)]
    [TestCase("(TestCategory=A|TestCategory=B)&FullyQualifiedName!~Q", "X.Y.M", "B", true)]
    [TestCase("(FullyQualifiedName~X&TestCategory!=A)&(TestCategory!=Chaos&TestCategory!=Coyote)", "X.Y.M", "Coyote", false)]
    [TestCase("TestCategory=A|TestCategory=B&TestCategory=C", "X", "A", true)]
    [TestCase("TestCategory=A|TestCategory=B&TestCategory=C", "X", "B", false)]
    public void The_filter_evaluator_reads_vstest_filters(string filter, string fullName, string categories, bool expected)
    {
        var testCase = new TlcCase(fullName, categories.Split(',', StringSplitOptions.RemoveEmptyEntries).ToHashSet(StringComparer.Ordinal));

        Assert.That(SelectsCase(filter, testCase), Is.EqualTo(expected));
    }

    private static List<string> PartitionProblems(IReadOnlyList<TlcCase> cases, IReadOnlyList<FilteredItem> items)
    {
        var problems = new List<string>();
        var compiled = items.Select(item => (Item: item, Predicate: VsTestFilter.Compile(item.Filter))).ToArray();

        foreach (var testCase in cases)
        {
            var selecting = compiled.Where(entry => entry.Predicate(testCase)).Select(entry => entry.Item).ToArray();
            if (selecting.Length == 0)
            {
                problems.Add($"TLC case {testCase.FullName} is selected by no item, so no leg runs it");
            }
            else if (selecting.Length > 1)
            {
                problems.Add($"TLC case {testCase.FullName} is selected by {selecting.Length} items ({string.Join(", ", selecting.Select(item => item.Label))})");
            }
            else if (!selecting[0].Tlc && !selecting[0].Smoke)
            {
                problems.Add($"TLC case {testCase.FullName} is selected by non-TLC item {selecting[0].Label}");
            }
        }

        var smokeSelected = cases.Where(testCase => compiled.Any(entry => entry.Item.Smoke && entry.Predicate(testCase)))
            .Select(testCase => testCase.FullName).ToArray();
        if (smokeSelected.Length != 1 || smokeSelected[0] != SmokeCaseFullName)
        {
            problems.Add($"the smoke shard must select exactly {SmokeCaseFullName}, but selects {smokeSelected.Length} case(s): {string.Join(", ", smokeSelected.Take(5))}");
        }

        foreach (var shard in items.Where(item => item.Tlc || item.Smoke).Select(item => item.Shard).Distinct(StringComparer.Ordinal))
        {
            if (!compiled.Any(entry => entry.Item.Shard == shard && cases.Any(entry.Predicate)))
            {
                problems.Add($"TLC shard {shard} selects no TLC case, so its leg runs nothing");
            }
        }

        return problems;
    }

    private static bool SelectsCase(string? filter, TlcCase testCase) => VsTestFilter.Compile(filter)(testCase);

    private static List<FilteredItem> FullPlanItems()
    {
        var (exitCode, _, stderr) = RunPlanner(["lattice"], skipTiers: null, out var matrix);
        Assert.That(exitCode, Is.Zero, "plan-test-matrix.py failed: " + stderr);

        using var document = JsonDocument.Parse(matrix);
        var items = new List<FilteredItem>();
        foreach (var item in document.RootElement.EnumerateArray().SelectMany(leg => leg.GetProperty("items").EnumerateArray()))
        {
            Assert.That(item.TryGetProperty("skip", out _), Is.False, "a full plan skips nothing");
            items.Add(new FilteredItem(
                item.GetProperty("label").GetString()!,
                item.GetProperty("shard").GetString()!,
                item.GetProperty("filter").ValueKind == JsonValueKind.String ? item.GetProperty("filter").GetString() : null,
                item.TryGetProperty("tlc", out var tlc) && tlc.GetBoolean(),
                item.TryGetProperty("tlcSmoke", out var smoke) && smoke.GetBoolean()));
        }

        return items;
    }

    /// <summary>
    /// Every runnable test NUnit builds under <see cref="TlcModelCheckTests"/>
    /// that carries the <c>Tlc</c> category, directly or from its fixture.
    /// </summary>
    private static List<TlcCase> TlcCases()
    {
        var fixture = new DefaultSuiteBuilder().BuildFrom(new TypeWrapper(typeof(TlcModelCheckTests)));
        var cases = new List<TlcCase>();
        Collect(fixture, cases);
        return cases;

        static void Collect(ITest test, List<TlcCase> into)
        {
            if (test.IsSuite)
            {
                Assert.That(test.RunState, Is.Not.EqualTo(RunState.NotRunnable), $"{test.FullName}: {test.Properties.Get(PropertyNames.SkipReason)}");
                foreach (var child in test.Tests)
                {
                    Collect(child, into);
                }

                return;
            }

            Assert.That(test.RunState, Is.Not.EqualTo(RunState.NotRunnable), $"{test.FullName}: {test.Properties.Get(PropertyNames.SkipReason)}");
            var categories = new HashSet<string>(StringComparer.Ordinal);
            for (var node = test; node is not null; node = node.Parent)
            {
                foreach (var category in node.Properties[PropertyNames.Category])
                {
                    categories.Add((string)category);
                }
            }

            if (categories.Contains("Tlc"))
            {
                into.Add(new TlcCase(test.FullName, categories));
            }
        }
    }

    /// <summary>
    /// The subset of the vstest filter grammar the planner emits: conditions
    /// on <c>FullyQualifiedName</c> and <c>TestCategory</c> with <c>=</c>,
    /// <c>!=</c>, <c>~</c> and <c>!~</c>, joined by <c>&amp;</c> (binding tighter)
    /// and <c>|</c>, with parentheses. Comparison is case-insensitive, as in
    /// vstest. Anything else is refused rather than guessed.
    /// </summary>
    private static class VsTestFilter
    {
        public static Func<TlcCase, bool> Compile(string? filter)
        {
            if (string.IsNullOrEmpty(filter))
            {
                return static _ => true;
            }

            var position = 0;
            var predicate = Or(filter, ref position);
            Assert.That(position, Is.EqualTo(filter.Length), $"unparsed filter tail at {position}: {filter}");
            return predicate;
        }

        private static Func<TlcCase, bool> Or(string text, ref int position)
        {
            var terms = new List<Func<TlcCase, bool>> { And(text, ref position) };
            while (position < text.Length && text[position] == '|')
            {
                position++;
                terms.Add(And(text, ref position));
            }

            return terms.Count == 1 ? terms[0] : testCase => terms.Any(term => term(testCase));
        }

        private static Func<TlcCase, bool> And(string text, ref int position)
        {
            var factors = new List<Func<TlcCase, bool>> { Factor(text, ref position) };
            while (position < text.Length && text[position] == '&')
            {
                position++;
                factors.Add(Factor(text, ref position));
            }

            return factors.Count == 1 ? factors[0] : testCase => factors.All(factor => factor(testCase));
        }

        private static Func<TlcCase, bool> Factor(string text, ref int position)
        {
            if (text[position] == '(')
            {
                position++;
                var inner = Or(text, ref position);
                Assert.That(position < text.Length && text[position] == ')', Is.True, $"unbalanced parentheses: {text}");
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

        private static Func<TlcCase, bool> Condition(string condition)
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
                Func<TlcCase, IEnumerable<string>> values = property switch
                {
                    "FullyQualifiedName" => static testCase => [testCase.FullName],
                    "TestCategory" or "Category" => static testCase => testCase.Categories,
                    _ => throw new AssertionException($"filter property '{property}' is not modelled by this check"),
                };
                return op.StartsWith('!')
                    ? testCase => !values(testCase).Any(test)
                    : testCase => values(testCase).Any(test);
            }

            throw new AssertionException($"'{condition}' is not a filter condition");
        }
    }
}
