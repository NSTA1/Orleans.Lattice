using System.Linq.Expressions;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// Query-planner edge cases: mirrored comparison operators (constant on the
/// left-hand side), two prefix constraints on the same property combining their
/// residuals, and the TooComplex rejection guard.
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    [Test]
    public async Task Where_constant_less_than_member_mirrors_to_greater_than()
    {
        // Line 480 of GrainIndexQueryPlanner: Mirror(LessThan) -> GreaterThan.
        // "18 < s.Age" is equivalent to "s.Age > 18".
        var index = Populated();

        var reversed = await KeysAsync(index.Index.Where(s => 18 < s.Age));

        Assert.That(reversed, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_constant_greater_than_member_mirrors_to_less_than()
    {
        // Line 482 of GrainIndexQueryPlanner: Mirror(GreaterThan) -> LessThan.
        // "18 > s.Age" is equivalent to "s.Age < 18".
        var index = Populated();

        var reversed = await KeysAsync(index.Index.Where(s => 18 > s.Age));

        Assert.That(reversed, Is.EquivalentTo(new[] { "alice" }));
    }

    [Test]
    public async Task Where_constant_greater_than_or_equal_mirrors_to_less_than_or_equal()
    {
        // Line 483 of GrainIndexQueryPlanner: Mirror(GreaterThanOrEqual) -> LessThanOrEqual.
        // "18 >= s.Age" is equivalent to "s.Age <= 18".
        var index = Populated();

        var reversed = await KeysAsync(index.Index.Where(s => 18 >= s.Age));

        Assert.That(reversed, Is.EquivalentTo(new[] { "alice", "bob" }));
    }

    [Test]
    public async Task Where_constant_equal_mirrors_to_equal()
    {
        // Line 484 of GrainIndexQueryPlanner: Mirror default branch (Equal -> Equal).
        // "\"GB\" == s.Country" is equivalent to "s.Country == \"GB\"".
        var index = Populated();

        var reversed = await KeysAsync(index.Index.Where(s => "GB" == s.Country));

        Assert.That(reversed, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_two_starts_with_on_same_property_combines_both_residuals()
    {
        // Lines 445-447 of GrainIndexQueryPlanner: Combine called with two non-null
        // residuals on the same property.  Both StartsWith clauses on Country produce
        // a residual predicate; the second call to Combine receives (residual1,
        // residual2) where both are non-null, so line 447 fires.
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country.StartsWith("G") && s.Country.StartsWith("GB")));

        // Only "GB" starts with both "G" and "GB"; "DE" and "FR" do not.
        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public void Where_with_too_many_disjunctions_throws_NotSupportedException()
    {
        // Line 119 of GrainIndexQueryPlanner: Concatenate rejects when the total
        // number of conjunctions would exceed MaxConjunctions (64).
        // Build an OR chain of 65 leaf clauses; the 65th Concatenate call sees
        // left.Count (64) + right.Count (1) = 65 > 64 and throws.
        var index = Populated();

        var param = Expression.Parameter(typeof(IndexedTestState), "s");
        var body = Enumerable.Range(0, 65)
            .Select(i => (Expression)Expression.Equal(
                Expression.Property(param, nameof(IndexedTestState.Age)),
                Expression.Constant(i)))
            .Aggregate(Expression.OrElse);
        var predicate = Expression.Lambda<Func<IndexedTestState, bool>>(body, param);

        Assert.That(
            () => index.Index.Where(predicate),
            Throws.TypeOf<NotSupportedException>());
    }
}
