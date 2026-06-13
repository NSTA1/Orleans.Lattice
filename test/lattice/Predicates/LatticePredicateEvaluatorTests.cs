using System.Linq.Expressions;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Unit tests for the server-side <see cref="LatticePredicateEvaluator"/>:
/// folding the IR against a value's JSON document view. The headline test is a
/// round-trip oracle: lambda to IR to evaluator must return the same boolean as
/// compiling and invoking the original lambda, across the full allowlist.
/// </summary>
[TestFixture]
public class LatticePredicateEvaluatorTests
{
    private static byte[] Encode(PredicatePerson person) =>
        JsonLatticeSerializer<PredicatePerson>.Default.Serialize(person);

    private static bool Eval(Expression<Func<PredicatePerson, bool>> predicate, PredicatePerson person)
    {
        var ir = LatticePredicateTranslator.Translate(predicate);
        return LatticePredicateEvaluator.Matches(Encode(person), ir);
    }

    private static IEnumerable<PredicatePerson> Population()
    {
        yield return new PredicatePerson("Alice", 30, true, 0.9, "Al", new PredicateAddress("London", "UK"));
        yield return new PredicatePerson("Bob", 17, false, 0.4, null, new PredicateAddress("Paris", "FR"));
        yield return new PredicatePerson("Carol", 18, true, 0.5, "C", new PredicateAddress("Berlin", "DE"));
        yield return new PredicatePerson("Anil", 65, false, 1.0, "A", null);
        yield return new PredicatePerson("zoe", 0, true, 0.0, "", new PredicateAddress("Rome", "IT"));
    }

    private static IEnumerable<Expression<Func<PredicatePerson, bool>>> Predicates()
    {
        yield return p => p.Age >= 18;
        yield return p => p.Age < 18;
        yield return p => p.Age == 18;
        yield return p => p.Age != 30;
        yield return p => p.Score <= 0.5;
        yield return p => p.Score > 0.5;
        yield return p => p.Active;
        yield return p => !p.Active;
        yield return p => p.Name.StartsWith("A");
        yield return p => p.Name.EndsWith("e");
        yield return p => p.Name.Contains("o");
        yield return p => p.Name.Equals("Bob");
        yield return p => p.Age >= 18 && p.Active;
        yield return p => p.Age < 18 || p.Score > 0.8;
        yield return p => p.Name.StartsWith("A") && p.Age > 20;
        yield return p => !(p.Age < 18) && p.Name.Contains("a");
        yield return p => p.Nickname == null;
        yield return p => p.Nickname != null;
        yield return p => p.Address!.City == "London";
        yield return p => p.Address!.Country == "FR" || p.Age == 18;
    }

    [Test]
    public void Evaluator_matches_compiled_lambda_across_allowlist()
    {
        var mismatches = new List<string>();
        foreach (var predicate in Predicates())
        {
            var compiled = predicate.Compile();
            var ir = LatticePredicateTranslator.Translate(predicate);
            foreach (var person in Population())
            {
                bool expected;
                try
                {
                    expected = compiled(person);
                }
                catch (NullReferenceException)
                {
                    // The evaluator's null-safe member resolution intentionally
                    // diverges from a client-side NRE on a null intermediate
                    // member (the push-down is strictly subtractive), so there
                    // is no ground truth to compare against here.
                    continue;
                }

                var actual = LatticePredicateEvaluator.Matches(Encode(person), ir);
                if (expected != actual)
                    mismatches.Add($"'{predicate}' on {person.Name}: expected {expected}, got {actual}");
            }
        }

        Assert.That(mismatches, Is.Empty, string.Join("\n", mismatches));
    }

    [Test]
    public void Matches_null_value_returns_false()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 0);
        Assert.That(LatticePredicateEvaluator.Matches(null, ir), Is.False);
    }

    [Test]
    public void Matches_empty_value_returns_false()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 0);
        Assert.That(LatticePredicateEvaluator.Matches([], ir), Is.False);
    }

    [Test]
    public void Matches_non_json_value_returns_false()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 0);
        Assert.That(LatticePredicateEvaluator.Matches([0x01, 0x02, 0x03], ir), Is.False);
    }

    [Test]
    public void Matches_resolves_member_path_case_insensitively()
    {
        // System.Text.Json default options write PascalCase; lower-cased JSON
        // must still resolve so camelCase serializer options interoperate.
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 18);
        var camelBytes = System.Text.Encoding.UTF8.GetBytes("{\"age\":21,\"name\":\"x\"}");
        Assert.That(LatticePredicateEvaluator.Matches(camelBytes, ir), Is.True);
    }

    [Test]
    public void Matches_missing_member_fails_predicate()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 18);
        var bytes = System.Text.Encoding.UTF8.GetBytes("{\"name\":\"x\"}");
        Assert.That(LatticePredicateEvaluator.Matches(bytes, ir), Is.False);
    }

    private static LatticePredicateNode NestNot(LatticePredicateNode inner, int levels)
    {
        var node = inner;
        for (var i = 0; i < levels; i++)
            node = LatticePredicateNode.Bool(LatticeBooleanOperator.Not, node);
        return node;
    }

    [Test]
    public void Matches_deeply_nested_predicate_throws_rather_than_overflowing_the_stack()
    {
        // The IR is a serializable, client-supplied tree. A crafted or corrupt
        // payload nested far beyond any legitimate translator output must be
        // rejected with a catchable exception, never allowed to recurse until
        // the silo crashes with an uncatchable StackOverflowException.
        var leaf = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(0)));
        var bomb = NestNot(leaf, 5000);
        var bytes = System.Text.Encoding.UTF8.GetBytes("{\"age\":5}");

        Assert.That(
            () => LatticePredicateEvaluator.Matches(bytes, bomb),
            Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void Matches_nesting_within_the_depth_limit_still_evaluates()
    {
        // A predicate nested below the ceiling must fold normally: 100 Not
        // wrappers over a true comparison leave the result unchanged (even
        // number of negations), proving the guard does not clip valid trees.
        var leaf = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(0)));
        var nested = NestNot(leaf, 100);
        var bytes = System.Text.Encoding.UTF8.GetBytes("{\"age\":5}");

        Assert.That(LatticePredicateEvaluator.Matches(bytes, nested), Is.True);
    }
}
