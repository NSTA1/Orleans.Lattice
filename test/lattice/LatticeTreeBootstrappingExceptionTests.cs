using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeTreeBootstrappingException"/>
/// (issue #4526): construction overloads, its base type, its stable wire alias,
/// and that it round-trips through the Orleans serializer with its tree id.
/// </summary>
[TestFixture]
public class LatticeTreeBootstrappingExceptionTests
{
    [Test]
    public void Parameterless_constructor_leaves_an_empty_tree_id()
    {
        var ex = new LatticeTreeBootstrappingException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.Empty);
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_constructors_preserve_message_and_inner()
    {
        var inner = new TimeoutException("inner");
        var withMessage = new LatticeTreeBootstrappingException("refused");
        var withInner = new LatticeTreeBootstrappingException("refused", inner);
        Assert.Multiple(() =>
        {
            Assert.That(withMessage.Message, Is.EqualTo("refused"));
            Assert.That(withMessage.TreeId, Is.Empty);
            Assert.That(withInner.InnerException, Is.SameAs(inner));
            Assert.That(withInner.TreeId, Is.Empty);
        });
    }

    [Test]
    public void Attributed_constructor_carries_the_tree_and_rejects_null()
    {
        var ex = new LatticeTreeBootstrappingException("refused", "orders");
        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("orders"));
            Assert.That(ex.Message, Is.EqualTo("refused"));
            Assert.That(() => new LatticeTreeBootstrappingException("refused", (string)null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Derives_directly_from_Exception_so_a_broad_InvalidOperationException_handler_never_absorbs_it()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeTreeBootstrappingException).BaseType, Is.EqualTo(typeof(Exception)));
            Assert.That(typeof(LatticeTreeBootstrappingException).IsSealed, Is.True);
        });
    }

    [Test]
    public void Carries_a_stable_Orleans_alias()
    {
        var alias = typeof(LatticeTreeBootstrappingException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .Single();
        Assert.That(alias.Alias, Is.EqualTo("ol.tbf"));
    }

    [Test]
    public void Round_trips_through_the_Orleans_serializer_with_its_tree_id()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<LatticeTreeBootstrappingException>>();

        var copy = serializer.Deserialize(serializer.SerializeToArray(new LatticeTreeBootstrappingException("refused", "orders")));

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Message, Is.EqualTo("refused"));
        });
    }
}
