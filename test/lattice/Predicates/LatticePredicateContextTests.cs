using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Unit tests for <see cref="LatticePredicateContext"/>: the ambient scope that
/// carries a compiled predicate IR down to the owning leaf so filtering happens
/// server-side and non-matching values never cross the wire.
/// <para>
/// The scope is designed to cost nothing when unused - every operation probes
/// <see cref="LatticePredicateContext.IsActive"/> and takes its un-predicated path -
/// so these tests pin that the default really is "inactive", that setting
/// <see langword="null"/> removes the entry rather than storing a null, and that
/// nested and repeated disposal restore exactly.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticePredicateContextTests
{
    [SetUp]
    [TearDown]
    public void ClearAmbientContext() => RequestContext.Clear();

    private static LatticePredicateNode Node(long value) =>
        LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Score"),
            LatticePredicateNode.Const(LatticeConstant.Integer(value)));

    [Test]
    public void IsActive_is_false_and_Current_is_null_by_default()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticePredicateContext.IsActive, Is.False);
            Assert.That(LatticePredicateContext.Current, Is.Null);
        });
    }

    [Test]
    public void IsActive_is_true_inside_a_scope()
    {
        using (LatticePredicateContext.With(Node(1)))
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticePredicateContext.IsActive, Is.True);
                Assert.That(LatticePredicateContext.Current, Is.Not.Null);
            });
        }

        Assert.That(LatticePredicateContext.IsActive, Is.False);
    }

    [Test]
    public void Setting_Current_to_null_removes_the_entry_rather_than_storing_a_null()
    {
        LatticePredicateContext.Current = Node(1);
        Assert.That(LatticePredicateContext.IsActive, Is.True);

        LatticePredicateContext.Current = null;

        Assert.Multiple(() =>
        {
            Assert.That(LatticePredicateContext.IsActive, Is.False);
            Assert.That(RequestContext.Get(LatticeEventConstants.PredicateRequestContextKey), Is.Null);
        });
    }

    [Test]
    public void With_null_suppresses_an_enclosing_predicate_for_the_scope()
    {
        using (LatticePredicateContext.With(Node(1)))
        {
            using (LatticePredicateContext.With(null))
            {
                Assert.That(LatticePredicateContext.IsActive, Is.False);
            }

            Assert.That(LatticePredicateContext.IsActive, Is.True);
        }
    }

    [Test]
    public void Disposing_a_nested_scope_restores_the_enclosing_predicate()
    {
        var outer = Node(1);

        using (LatticePredicateContext.With(outer))
        {
            using (LatticePredicateContext.With(Node(2)))
            {
                Assert.That(LatticePredicateContext.Current, Is.Not.EqualTo(outer));
            }

            Assert.That(LatticePredicateContext.Current, Is.EqualTo(outer));
        }

        Assert.That(LatticePredicateContext.Current, Is.Null);
    }

    [Test]
    public void Disposing_a_scope_twice_is_idempotent()
    {
        var outer = Node(1);

        using (LatticePredicateContext.With(outer))
        {
            var inner = LatticePredicateContext.With(Node(2));
            inner.Dispose();
            Assert.That(LatticePredicateContext.Current, Is.EqualTo(outer));

            inner.Dispose();
            Assert.That(LatticePredicateContext.Current, Is.EqualTo(outer));
        }
    }

    [Test]
    public void Current_ignores_a_foreign_value_on_the_request_context()
    {
        RequestContext.Set(LatticeEventConstants.PredicateRequestContextKey, "not-a-predicate");

        Assert.Multiple(() =>
        {
            Assert.That(LatticePredicateContext.Current, Is.Null);
            Assert.That(LatticePredicateContext.IsActive, Is.False);
        });
    }
}
