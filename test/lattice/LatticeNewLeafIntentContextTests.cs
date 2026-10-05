using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeNewLeafIntentContext"/> create
/// intent a path creating a leaf carries (issue #4654).
/// </summary>
[TestFixture]
public class LatticeNewLeafIntentContextTests
{
    private static readonly GrainId Leaf = GrainId.Create("bplusleaf", Guid.NewGuid().ToString("N"));
    private static readonly GrainId Other = GrainId.Create("bplusleaf", Guid.NewGuid().ToString("N"));

    [SetUp]
    [TearDown]
    public void EnsureCleanContext() =>
        RequestContext.Remove(LatticeEventConstants.NewLeafIntentRequestContextKey);

    [Test]
    public void IsFor_is_false_when_no_scope_is_active() =>
        Assert.That(LatticeNewLeafIntentContext.IsFor(Leaf), Is.False);

    [TestCase(true)]
    [TestCase("")]
    [TestCase(42)]
    public void IsFor_is_false_when_the_context_carries_a_foreign_value(object foreign)
    {
        RequestContext.Set(LatticeEventConstants.NewLeafIntentRequestContextKey, foreign);

        Assert.That(LatticeNewLeafIntentContext.IsFor(Leaf), Is.False);
    }

    [Test]
    public void BeginScope_names_one_leaf_only_and_removes_the_key_on_dispose()
    {
        using (LatticeNewLeafIntentContext.BeginScope(Leaf))
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticeNewLeafIntentContext.IsFor(Leaf), Is.True);
                Assert.That(LatticeNewLeafIntentContext.IsFor(Other), Is.False,
                    "an intent that propagates past its call must confer nothing on another leaf");
            });
        }

        Assert.That(RequestContext.Get(LatticeEventConstants.NewLeafIntentRequestContextKey), Is.Null);
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_intent_and_dispose_is_idempotent()
    {
        using (LatticeNewLeafIntentContext.BeginScope(Leaf))
        {
            var inner = LatticeNewLeafIntentContext.BeginScope(Other);
            Assert.That(LatticeNewLeafIntentContext.IsFor(Other), Is.True);
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeNewLeafIntentContext.IsFor(Leaf), Is.True);
        }

        Assert.That(LatticeNewLeafIntentContext.IsFor(Leaf), Is.False);
    }
}
