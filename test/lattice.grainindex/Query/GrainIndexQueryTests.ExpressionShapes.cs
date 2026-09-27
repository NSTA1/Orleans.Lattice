using System.Linq.Expressions;
using System.Reflection;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The planner's expression-shape routing: the allocation-free
/// <c>ContainsParameter</c> switch that answers "does this sub-expression mention
/// the lambda parameter?", and the <c>ParameterFinder</c> visitor it falls back to
/// for a node kind the switch does not enumerate.
/// </summary>
/// <remarks>
/// <para>
/// The switch replaced a visitor that walked every sub-tree, so it now decides on
/// its own which side of a comparison is the value side. The fallback exists
/// precisely so an unenumerated node kind degrades to the old behaviour instead of
/// guessing, and that guarantee had no test.
/// </para>
/// <para>
/// <b>Why these tests assert the scanned range, not only the rows.</b> A wrong
/// answer on the value side of a <c>StartsWith</c> does not corrupt the result
/// set: the clause stops matching the prefix arm and falls through to
/// <c>property.FullRange</c>, keeping the residual predicate, which the
/// server-side evaluator then applies to every entry. The rows come back correct
/// and the plan is silently a whole-property scan. A result-set assertion here is
/// therefore insensitive to the exact defect, and only the range the tree was
/// asked for separates the two plans. (A wrong answer on a comparison's member
/// side is different - two parameter-referencing sides are rejected outright - so
/// those tests assert rows.)
/// </para>
/// <para>
/// Each predicate is hand-built because the C# compiler will not emit these node
/// kinds from ordinary lambda syntax; it constant-folds most of them at the call
/// site. They are reached through the same public <c>Where</c> seam a caller uses.
/// </para>
/// </remarks>
public sealed partial class GrainIndexQueryTests
{
    private const string CountryRangeStart = "Country\u0000";

    private static readonly MethodInfo StringStartsWith =
        typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!;

    private static Expression<Func<IndexedTestState, bool>> CountryStartsWith(
        ParameterExpression parameter,
        Expression value)
        => Expression.Lambda<Func<IndexedTestState, bool>>(
            Expression.Call(
                Expression.Property(parameter, nameof(IndexedTestState.Country)),
                StringStartsWith,
                value),
            parameter);

    /// <summary>
    /// Runs a Country prefix predicate and asserts both the rows and that the
    /// clause was routed as a narrowed prefix scan rather than a whole-property
    /// one.
    /// </summary>
    private static async Task AssertPrefixRoutedAsync(Expression<Func<IndexedTestState, bool>> predicate)
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(predicate));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
            Assert.That(index.Tree.ScannedRanges, Is.Not.Empty, "the query must have scanned something");
            Assert.That(
                index.Tree.ScannedRanges.Select(r => r.Start),
                Has.None.EqualTo(CountryRangeStart),
                "a parameter-free value side must narrow to a prefix range; scanning the whole Country "
                + "range means ContainsParameter answered 'references the parameter' and the clause fell back");
        });
    }

    [Test]
    public Task A_conditional_value_side_is_recognised_as_parameter_free()
    {
        // ConditionalExpression arm: the test, the true branch and the false branch
        // are each probed, and none mentions the parameter.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var value = Expression.Condition(
            Expression.Constant(true), Expression.Constant("G"), Expression.Constant("D"));

        return AssertPrefixRoutedAsync(CountryStartsWith(parameter, value));
    }

    [Test]
    public Task A_constructor_call_on_the_value_side_is_recognised_as_parameter_free()
    {
        // NewExpression arm: only the constructor arguments can mention the
        // parameter, and here they do not.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var value = Expression.New(
            typeof(string).GetConstructor([typeof(char), typeof(int)])!,
            Expression.Constant('G'),
            Expression.Constant(1));

        return AssertPrefixRoutedAsync(CountryStartsWith(parameter, value));
    }

    [Test]
    public Task An_array_construction_on_the_value_side_is_recognised_as_parameter_free()
    {
        // NewArrayExpression arm, reached through the enclosing call's argument
        // list - which also exercises the argument-collection walk.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var join = typeof(string).GetMethod(nameof(string.Join), [typeof(string), typeof(string[])])!;
        var value = Expression.Call(
            join,
            Expression.Constant(string.Empty),
            Expression.NewArrayInit(typeof(string), Expression.Constant("G")));

        return AssertPrefixRoutedAsync(CountryStartsWith(parameter, value));
    }

    [Test]
    public Task An_invoked_lambda_on_the_value_side_is_recognised_as_parameter_free()
    {
        // InvocationExpression arm, which recurses into the invoked
        // LambdaExpression - so this covers the lambda arm's parameter list and
        // body as well.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        Expression<Func<string>> prefix = () => "G";

        return AssertPrefixRoutedAsync(CountryStartsWith(parameter, Expression.Invoke(prefix)));
    }

    [Test]
    public Task An_indexer_read_on_the_value_side_is_recognised_as_parameter_free()
    {
        // IndexExpression arm: the indexed object and the index arguments are both
        // probed.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var prefixes = new List<string> { "G", "D" };
        var value = Expression.MakeIndex(
            Expression.Constant(prefixes),
            typeof(List<string>).GetProperty("Item")!,
            [Expression.Constant(0)]);

        return AssertPrefixRoutedAsync(CountryStartsWith(parameter, value));
    }

    [Test]
    public async Task An_object_initialiser_on_the_value_side_falls_back_to_the_visitor()
    {
        // MemberInitExpression is not one of the node kinds the switch enumerates,
        // so it takes the default arm and the ExpressionVisitor fallback decides.
        // Here the rows ARE the discriminator: a comparison whose two sides both
        // appear to reference the parameter is rejected outright, so a wrong answer
        // throws rather than degrading to a wider scan.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var built = Expression.MemberInit(
            Expression.New(typeof(IndexedTestState)),
            Expression.Bind(
                typeof(IndexedTestState).GetProperty(nameof(IndexedTestState.Age))!,
                Expression.Constant(18)));

        var predicate = Expression.Lambda<Func<IndexedTestState, bool>>(
            Expression.GreaterThanOrEqual(
                Expression.Property(parameter, nameof(IndexedTestState.Age)),
                Expression.Property(built, nameof(IndexedTestState.Age))),
            parameter);

        var keys = await KeysAsync(Populated().Index.Where(predicate));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }),
            "the fallback must answer 'parameter-free', so the bound folds to 18");
    }

    [Test]
    public async Task A_collection_initialiser_on_the_value_side_falls_back_to_the_visitor()
    {
        // ListInitExpression is the second unenumerated shape, reaching the same
        // fallback by a different route through the switch's default arm.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var built = Expression.ListInit(
            Expression.New(typeof(List<int>)),
            Expression.Constant(41));

        var predicate = Expression.Lambda<Func<IndexedTestState, bool>>(
            Expression.GreaterThanOrEqual(
                Expression.Property(parameter, nameof(IndexedTestState.Age)),
                Expression.Property(built, nameof(List<int>.Capacity))),
            parameter);

        var keys = await KeysAsync(Populated().Index.Where(predicate));

        // A list initialised with one element has capacity 4, so the bound is 4.
        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
    }

    [Test]
    public async Task A_foreign_lambda_parameter_inside_the_fallback_is_not_mistaken_for_this_one()
    {
        // The fallback visitor sees a ParameterExpression here, but it belongs to
        // an unrelated inner lambda, not to the predicate. The visitor must
        // discriminate by parameter IDENTITY: answering "yes, a parameter" for any
        // parameter at all would make this look like a two-member comparison, which
        // the planner rejects outright.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var inner = Expression.Parameter(typeof(int), "x");
        var identity = Expression.Lambda<Func<int, int>>(inner, inner);

        var built = Expression.MemberInit(
            Expression.New(typeof(IndexedTestState)),
            Expression.Bind(
                typeof(IndexedTestState).GetProperty(nameof(IndexedTestState.Age))!,
                Expression.Invoke(identity, Expression.Constant(18))));

        var predicate = Expression.Lambda<Func<IndexedTestState, bool>>(
            Expression.GreaterThanOrEqual(
                Expression.Property(parameter, nameof(IndexedTestState.Age)),
                Expression.Property(built, nameof(IndexedTestState.Age))),
            parameter);

        var keys = await KeysAsync(Populated().Index.Where(predicate));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }),
            "the inner lambda's own parameter must not be read as a reference to the predicate's parameter");
    }

    [Test]
    public void A_type_test_on_the_value_side_is_rejected_by_the_predicate_dialect()
    {
        // 'x is T' never reaches the planner's shape switch: every atom is
        // validated against the core predicate dialect first, and the dialect does
        // not model a type test. Pinning that here records why the switch's
        // TypeBinaryExpression arm cannot be reached from a query.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var predicate = Expression.Lambda<Func<IndexedTestState, bool>>(
            Expression.Equal(
                Expression.TypeIs(
                    Expression.Property(parameter, nameof(IndexedTestState.Country)), typeof(string)),
                Expression.Constant(true)),
            parameter);

        var index = Populated();

        Assert.That(() => index.Index.Where(predicate), Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void A_parameter_rooted_value_side_is_rejected_before_the_shape_switch_sees_it()
    {
        // The complement of the fallback tests above, and the reason the visitor's
        // 'this IS my parameter' arm cannot be reached from a query: the dialect
        // requires a value-side member access to be rooted at the lambda parameter
        // or foldable to a constant, so an unenumerated node kind that carries the
        // parameter is refused before the planner routes anything.
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var built = Expression.MemberInit(
            Expression.New(typeof(IndexedTestState)),
            Expression.Bind(
                typeof(IndexedTestState).GetProperty(nameof(IndexedTestState.Age))!,
                Expression.Property(parameter, nameof(IndexedTestState.Age))));

        var predicate = Expression.Lambda<Func<IndexedTestState, bool>>(
            Expression.GreaterThanOrEqual(
                Expression.Property(built, nameof(IndexedTestState.Age)),
                Expression.Constant(18)),
            parameter);

        var index = Populated();

        Assert.That(() => index.Index.Where(predicate), Throws.TypeOf<NotSupportedException>());
    }
}
