using System.Linq.Expressions;
using Orleans.Lattice.Tests.Predicates;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticePredicateEvaluation"/> helper: the
/// tightly-scoped value-evaluation surface a companion enforcement package uses
/// to fold a <see cref="LatticePredicateNode"/> against a value's JSON document.
/// It must share exactly the internal evaluator's semantics.
/// </summary>
[TestFixture]
public class LatticePredicateEvaluationTests
{
    private static byte[] Encode(PredicatePerson person) =>
        JsonLatticeSerializer<PredicatePerson>.Default.Serialize(person);

    private static LatticePredicateNode Ir(Expression<Func<PredicatePerson, bool>> predicate) =>
        LatticePredicateTranslator.Translate(predicate);

    [Test]
    public void Matches_returns_true_when_the_document_satisfies_the_predicate()
    {
        var person = new PredicatePerson("Alice", 30, true, 0.9, "Al", new PredicateAddress("London", "UK"));

        Assert.That(LatticePredicateEvaluation.Matches(Encode(person), Ir(p => p.Age >= 18)), Is.True);
    }

    [Test]
    public void Matches_returns_false_when_the_document_does_not_satisfy_the_predicate()
    {
        var person = new PredicatePerson("Bob", 17, false, 0.4, null, new PredicateAddress("Paris", "FR"));

        Assert.That(LatticePredicateEvaluation.Matches(Encode(person), Ir(p => p.Age >= 18)), Is.False);
    }

    [Test]
    public void Matches_evaluates_a_nested_member_path()
    {
        var person = new PredicatePerson("Alice", 30, true, 0.9, "Al", new PredicateAddress("London", "UK"));

        Assert.That(LatticePredicateEvaluation.Matches(Encode(person), Ir(p => p.Address!.City == "London")), Is.True);
    }

    [Test]
    public void Matches_returns_false_for_a_null_value()
    {
        Assert.That(LatticePredicateEvaluation.Matches(null, Ir(p => p.Age >= 18)), Is.False);
    }

    [Test]
    public void Matches_returns_false_for_an_empty_value()
    {
        Assert.That(LatticePredicateEvaluation.Matches([], Ir(p => p.Age >= 18)), Is.False);
    }

    [Test]
    public void Matches_returns_false_for_a_non_json_payload()
    {
        Assert.That(LatticePredicateEvaluation.Matches([0xFF, 0x00, 0xFF], Ir(p => p.Age >= 18)), Is.False);
    }

    [Test]
    public void Matches_agrees_with_the_internal_evaluator()
    {
        var person = new PredicatePerson("Carol", 18, true, 0.5, "C", new PredicateAddress("Berlin", "DE"));
        var bytes = Encode(person);
        var ir = Ir(p => p.Age == 18 && p.Active);

        Assert.That(
            LatticePredicateEvaluation.Matches(bytes, ir),
            Is.EqualTo(LatticePredicateEvaluator.Matches(bytes, ir)));
    }
}
