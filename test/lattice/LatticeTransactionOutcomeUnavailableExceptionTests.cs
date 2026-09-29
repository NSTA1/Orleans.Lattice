using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeTransactionOutcomeUnavailableException"/>
/// (issues #2215 / #3641): its construction overloads and factory, the sealed /
/// public contract, its derivation from <see cref="TimeoutException"/> and its
/// <see cref="ILatticeDomainFault"/> marker, and the stable Orleans
/// serialization surface (alias, <c>[GenerateSerializer]</c>, a full round-trip,
/// and the same-silo deep copy). The round-trip is load-bearing: a leaf read is
/// routinely issued from a peer silo, so the exception must cross the grain
/// boundary as itself, carrying the tree and transaction ids that make it
/// actionable.
/// </summary>
[TestFixture]
public class LatticeTransactionOutcomeUnavailableExceptionTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeTransactionOutcomeUnavailableException> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeTransactionOutcomeUnavailableException>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Parameterless_constructor_initialises_with_empty_context()
    {
        var ex = new LatticeTransactionOutcomeUnavailableException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.Empty);
            Assert.That(ex.Key, Is.Null);
            Assert.That(ex.KeyCount, Is.Zero);
            Assert.That(ex.TransactionIds, Is.Empty);
        });
    }

    [Test]
    public void Message_constructor_preserves_message()
    {
        var ex = new LatticeTransactionOutcomeUnavailableException("registry unreachable");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("registry unreachable"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new TimeoutException("response timeout");
        var ex = new LatticeTransactionOutcomeUnavailableException("registry unreachable", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("registry unreachable"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Create_populates_every_attribution_slot_and_keeps_the_key_out_of_the_message()
    {
        var txid = Guid.NewGuid();
        var inner = new TimeoutException("response timeout");

        var ex = LatticeTransactionOutcomeUnavailableException.Create("orders", "secret-key", 1, [txid], inner);

        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("orders"));
            Assert.That(ex.Key, Is.EqualTo("secret-key"));
            Assert.That(ex.KeyCount, Is.EqualTo(1));
            Assert.That(ex.TransactionIds, Is.EqualTo(new[] { txid }));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.Message, Does.Contain("orders"));
            Assert.That(ex.Message, Does.Not.Contain("secret-key"),
                "a logged message must never disclose key content");
        });
    }

    [Test]
    public void Create_without_an_inner_exception_leaves_InnerException_null()
    {
        var ex = LatticeTransactionOutcomeUnavailableException.Create("orders", null, 3, [Guid.NewGuid()], null);
        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.Key, Is.Null);
            Assert.That(ex.KeyCount, Is.EqualTo(3));
        });
    }

    [Test]
    public void Derives_from_TimeoutException_and_is_a_domain_fault()
    {
        // TimeoutException keeps existing catch (TimeoutException) handlers
        // observing the failure they observed when the raw registry timeout
        // propagated; the marker lets a broad handler decline it.
        var ex = new LatticeTransactionOutcomeUnavailableException("m");
        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.InstanceOf<TimeoutException>());
            Assert.That(ex, Is.InstanceOf<ILatticeDomainFault>());
        });
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeTransactionOutcomeUnavailableException).IsSealed, Is.True);
            Assert.That(typeof(LatticeTransactionOutcomeUnavailableException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeTransactionOutcomeUnavailableException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.tou"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LatticeTransactionOutcomeUnavailableException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void Round_trips_every_attribution_slot_through_the_Orleans_serializer()
    {
        var txids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        var original = LatticeTransactionOutcomeUnavailableException.Create(
            "orders", "k", 2, txids, new TimeoutException("response timeout"));

        var restored = _serializer.Deserialize(_serializer.SerializeToArray(original));

        Assert.Multiple(() =>
        {
            Assert.That(restored, Is.Not.Null);
            Assert.That(restored.Message, Is.EqualTo(original.Message));
            Assert.That(restored.InnerException, Is.Not.Null);
            Assert.That(restored.TreeId, Is.EqualTo("orders"));
            Assert.That(restored.Key, Is.EqualTo("k"));
            Assert.That(restored.KeyCount, Is.EqualTo(2));
            Assert.That(restored.TransactionIds, Is.EqualTo(txids));
        });
    }

    [Test]
    public void Deep_copies_through_the_Orleans_copier_on_a_same_silo_boundary()
    {
        var copier = _services.GetRequiredService<DeepCopier<LatticeTransactionOutcomeUnavailableException>>();
        var original = LatticeTransactionOutcomeUnavailableException.Create(
            "orders", null, 4, [Guid.NewGuid()], null);

        var copy = copier.Copy(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.Not.Null);
            Assert.That(copy.Message, Is.EqualTo(original.Message));
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.KeyCount, Is.EqualTo(4));
        });
    }
}
