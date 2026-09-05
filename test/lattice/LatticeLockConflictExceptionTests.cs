namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeLockConflictException"/>: every
/// construction overload, the <see cref="LatticeLockConflictException.LockName"/>
/// attribution slot, the inheritance contract that keeps it deep-copyable without
/// a companion copier, and the stable Orleans serialization surface (alias +
/// <c>[Id]</c> member) the manifest relies on to surface the typed exception
/// across a grain boundary.
/// </summary>
[TestFixture]
public class LatticeLockConflictExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_an_empty_lock_name()
    {
        var ex = new LatticeLockConflictException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.LockName, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Message_constructor_preserves_the_message_with_an_empty_lock_name()
    {
        var ex = new LatticeLockConflictException("lock is held by another owner");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("lock is held by another owner"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.LockName, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("lease read failed");
        var ex = new LatticeLockConflictException("lock is held", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("lock is held"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.LockName, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void LockName_constructor_preserves_the_contended_lock()
    {
        var ex = new LatticeLockConflictException("lock 'rebalance' is held", lockName: "rebalance");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("lock 'rebalance' is held"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.LockName, Is.EqualTo("rebalance"));
        });
    }

    [Test]
    public void Derives_directly_from_Exception_so_no_companion_copier_is_required()
    {
        Assert.That(typeof(LatticeLockConflictException).BaseType, Is.EqualTo(typeof(Exception)),
            "a [GenerateSerializer] exception deriving from a BCL exception subclass needs a "
            + "[RegisterCopier] IDeepCopier<T>; deriving directly from Exception avoids that");
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeLockConflictException).IsSealed, Is.True);
            Assert.That(typeof(LatticeLockConflictException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeLockConflictException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.elc"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LatticeLockConflictException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void LockName_carries_Id_zero()
    {
        var prop = typeof(LatticeLockConflictException).GetProperty(nameof(LatticeLockConflictException.LockName));
        Assert.That(prop, Is.Not.Null);
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, "LockName must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(0u));
    }
}
