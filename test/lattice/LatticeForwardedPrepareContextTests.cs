using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeForwardedPrepareContext"/>
/// ambient marker that flags a saga prepare as forwarded shard to shard and
/// names the tree whose registry records its decision (issue #4445).
/// </summary>
[TestFixture]
public class LatticeForwardedPrepareContextTests
{
    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
    {
        RequestContext.Remove(LatticeEventConstants.ForwardedPrepareRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.PreparedRequestContextKey);
        LatticeTransactionContext.Set(Guid.Empty);
    }

    [Test]
    public void Current_defaults_to_false_when_no_scope_is_active()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeForwardedPrepareContext.Current, Is.False);
            Assert.That(LatticeForwardedPrepareContext.RegistryTreeId, Is.Null);
        });
    }

    [TestCase(true)]
    [TestCase("")]
    [TestCase(42)]
    public void Current_is_false_when_the_context_carries_a_foreign_value(object foreign)
    {
        RequestContext.Set(LatticeEventConstants.ForwardedPrepareRequestContextKey, foreign);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeForwardedPrepareContext.Current, Is.False);
            Assert.That(LatticeForwardedPrepareContext.RegistryTreeId, Is.Null);
        });
    }

    [Test]
    public void BeginScope_sets_the_tree_id_and_removes_the_key_on_dispose()
    {
        using (LatticeForwardedPrepareContext.BeginScope("logical"))
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticeForwardedPrepareContext.Current, Is.True);
                Assert.That(LatticeForwardedPrepareContext.RegistryTreeId, Is.EqualTo("logical"));
            });
        }

        Assert.That(
            RequestContext.Get(LatticeEventConstants.ForwardedPrepareRequestContextKey),
            Is.Null,
            "an outermost scope had no prior value, so disposal must remove the key");
    }

    [Test]
    public void BeginScope_rejects_an_empty_tree_id()
    {
        Assert.That(() => LatticeForwardedPrepareContext.BeginScope(""), Throws.ArgumentException);
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_tree_id_and_dispose_is_idempotent()
    {
        using (LatticeForwardedPrepareContext.BeginScope("outer"))
        {
            var inner = LatticeForwardedPrepareContext.BeginScope("inner");
            Assert.That(LatticeForwardedPrepareContext.RegistryTreeId, Is.EqualTo("inner"));
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeForwardedPrepareContext.RegistryTreeId, Is.EqualTo("outer"));
        }

        Assert.That(LatticeForwardedPrepareContext.Current, Is.False);
    }

    [Test]
    public void BeginScopeIfPrepared_is_a_no_op_outside_a_saga_prepare()
    {
        using (LatticeForwardedPrepareContext.BeginScopeIfPrepared("logical"))
        {
            Assert.That(LatticeForwardedPrepareContext.Current, Is.False,
                "a forward of an ordinary write must not carry the marker");
        }

        using (LatticePreparedContext.BeginScope())
        using (LatticeForwardedPrepareContext.BeginScopeIfPrepared("logical"))
        {
            Assert.That(LatticeForwardedPrepareContext.Current, Is.False,
                "a prepared flag without a transaction id is not a saga prepare");
        }
    }

    [Test]
    public void BeginScopeIfPrepared_marks_a_saga_prepare()
    {
        LatticeTransactionContext.Set(Guid.NewGuid());
        using (LatticePreparedContext.BeginScope())
        {
            using (LatticeForwardedPrepareContext.BeginScopeIfPrepared("logical"))
            {
                Assert.That(LatticeForwardedPrepareContext.RegistryTreeId, Is.EqualTo("logical"));
            }

            Assert.That(LatticeForwardedPrepareContext.Current, Is.False);
        }
    }

    [Test]
    public void BeginScopeIfPrepared_keeps_the_tree_id_of_a_marker_already_in_force()
    {
        // A forward of a forward: the second hop's shard only knows its own
        // physical tree, while the first hop named the logical one.
        LatticeTransactionContext.Set(Guid.NewGuid());
        using (LatticePreparedContext.BeginScope())
        using (LatticeForwardedPrepareContext.BeginScope("logical"))
        using (LatticeForwardedPrepareContext.BeginScopeIfPrepared("physical"))
        {
            Assert.That(LatticeForwardedPrepareContext.RegistryTreeId, Is.EqualTo("logical"));
        }
    }
}
