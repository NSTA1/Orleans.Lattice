namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeCrdtShapeNotRegisteredException"/>:
/// its construction overloads, the
/// <see cref="LatticeCrdtShapeNotRegisteredException.TreeId"/> attribution slot,
/// the inheritance / sealed contract that lets existing
/// <see cref="System.InvalidOperationException"/> handlers still absorb it while
/// the API bindings map the typed slot to a client-error status, and the stable
/// Orleans serialization surface (alias + <c>[Id]</c> member) the manifest relies
/// on to surface the typed exception across grain boundaries.
/// </summary>
[TestFixture]
public class LatticeCrdtShapeNotRegisteredExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_empty_context()
    {
        var ex = new LatticeCrdtShapeNotRegisteredException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Message_constructor_preserves_message_with_empty_treeId()
    {
        var ex = new LatticeCrdtShapeNotRegisteredException("no shape registered");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("no shape registered"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("underlying");
        var ex = new LatticeCrdtShapeNotRegisteredException("unresolved", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("unresolved"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndTreeId_constructor_preserves_both_arguments()
    {
        var ex = new LatticeCrdtShapeNotRegisteredException("unresolved", treeId: "orders");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("unresolved"));
            Assert.That(ex.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void MessageAndTreeId_constructor_rejects_null_treeId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeCrdtShapeNotRegisteredException("m", treeId: null!));
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        var ex = new LatticeCrdtShapeNotRegisteredException("m");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeCrdtShapeNotRegisteredException).IsSealed, Is.True);
            Assert.That(typeof(LatticeCrdtShapeNotRegisteredException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeCrdtShapeNotRegisteredException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.csnr"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LatticeCrdtShapeNotRegisteredException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void TreeId_carries_the_first_Id_attribute()
    {
        var prop = typeof(LatticeCrdtShapeNotRegisteredException)
            .GetProperty(nameof(LatticeCrdtShapeNotRegisteredException.TreeId));
        Assert.That(prop, Is.Not.Null);
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, "TreeId must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(0u), "TreeId must be [Id(0)]");
    }
}
