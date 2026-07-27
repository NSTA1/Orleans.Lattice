namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeIdempotencyKeyMismatchException"/>:
/// its construction overloads, the
/// <see cref="LatticeIdempotencyKeyMismatchException.OperationId"/> attribution
/// slot, the inheritance / sealed contract that lets existing
/// <see cref="System.InvalidOperationException"/> handlers still absorb it while
/// the API bindings map the typed slot to a client-error status, and the stable
/// Orleans serialization surface (alias + <c>[Id]</c> member) the manifest relies
/// on to surface the typed exception across grain boundaries.
/// </summary>
[TestFixture]
public class LatticeIdempotencyKeyMismatchExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_empty_context()
    {
        var ex = new LatticeIdempotencyKeyMismatchException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.OperationId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Message_constructor_preserves_message_with_empty_operationId()
    {
        var ex = new LatticeIdempotencyKeyMismatchException("different set of keys");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("different set of keys"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.OperationId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("underlying");
        var ex = new LatticeIdempotencyKeyMismatchException("mismatch", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("mismatch"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.OperationId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndOperationId_constructor_preserves_both_arguments()
    {
        var ex = new LatticeIdempotencyKeyMismatchException("mismatch", operationId: "op-42");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("mismatch"));
            Assert.That(ex.OperationId, Is.EqualTo("op-42"));
        });
    }

    [Test]
    public void MessageAndOperationId_constructor_rejects_null_operationId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeIdempotencyKeyMismatchException("m", operationId: null!));
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        var ex = new LatticeIdempotencyKeyMismatchException("m");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeIdempotencyKeyMismatchException).IsSealed, Is.True);
            Assert.That(typeof(LatticeIdempotencyKeyMismatchException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeIdempotencyKeyMismatchException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.ikm"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LatticeIdempotencyKeyMismatchException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void OperationId_carries_the_first_Id_attribute()
    {
        var prop = typeof(LatticeIdempotencyKeyMismatchException)
            .GetProperty(nameof(LatticeIdempotencyKeyMismatchException.OperationId));
        Assert.That(prop, Is.Not.Null);
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, "OperationId must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(0u), "OperationId must be [Id(0)]");
    }
}
