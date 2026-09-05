namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="AtomicActionHandlerNotRegisteredException"/>:
/// every construction overload, the
/// <see cref="AtomicActionHandlerNotRegisteredException.HandlerId"/> attribution
/// slot, the inheritance contract that keeps it deep-copyable without a companion
/// copier, and the stable Orleans serialization surface (alias + <c>[Id]</c>
/// member) the manifest relies on to surface the typed exception across a grain
/// boundary.
/// </summary>
[TestFixture]
public class AtomicActionHandlerNotRegisteredExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_an_empty_handler_id()
    {
        var ex = new AtomicActionHandlerNotRegisteredException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.HandlerId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Message_constructor_preserves_the_message_with_an_empty_handler_id()
    {
        var ex = new AtomicActionHandlerNotRegisteredException("no handler registered");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("no handler registered"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.HandlerId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("resolution failed");
        var ex = new AtomicActionHandlerNotRegisteredException("no handler registered", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("no handler registered"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.HandlerId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void HandlerId_constructor_preserves_the_unresolved_handler_id()
    {
        var ex = new AtomicActionHandlerNotRegisteredException(
            "no handler registered for 'transfer'", handlerId: "transfer");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("no handler registered for 'transfer'"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.HandlerId, Is.EqualTo("transfer"));
        });
    }

    [Test]
    public void Derives_directly_from_Exception_so_no_companion_copier_is_required()
    {
        Assert.That(typeof(AtomicActionHandlerNotRegisteredException).BaseType, Is.EqualTo(typeof(Exception)),
            "a [GenerateSerializer] exception deriving from a BCL exception subclass needs a "
            + "[RegisterCopier] IDeepCopier<T>; deriving directly from Exception avoids that");
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(AtomicActionHandlerNotRegisteredException).IsSealed, Is.True);
            Assert.That(typeof(AtomicActionHandlerNotRegisteredException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(AtomicActionHandlerNotRegisteredException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.ehn"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(AtomicActionHandlerNotRegisteredException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void HandlerId_carries_Id_zero()
    {
        var prop = typeof(AtomicActionHandlerNotRegisteredException)
            .GetProperty(nameof(AtomicActionHandlerNotRegisteredException.HandlerId));
        Assert.That(prop, Is.Not.Null);
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, "HandlerId must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(0u));
    }
}
