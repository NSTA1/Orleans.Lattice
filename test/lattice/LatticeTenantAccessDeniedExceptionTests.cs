namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeTenantAccessDeniedException"/>:
/// every construction overload (including the parameterless one, whose canned
/// message is the fail-closed default the tenancy seam surfaces when no valid
/// active tenant is present), the inheritance contract that keeps it deep-copyable
/// without a companion copier, and the stable Orleans serialization surface.
/// </summary>
[TestFixture]
public class LatticeTenantAccessDeniedExceptionTests
{
    [Test]
    public void Parameterless_constructor_carries_the_canned_fail_closed_message()
    {
        var ex = new LatticeTenantAccessDeniedException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("no valid active tenant"),
                "the default message must state the fail-closed reason without the caller supplying one");
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_constructor_preserves_the_message()
    {
        var ex = new LatticeTenantAccessDeniedException("tenant 'acme' may not write to 'orders'");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("tenant 'acme' may not write to 'orders'"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("membership lookup failed");
        var ex = new LatticeTenantAccessDeniedException("denied", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("denied"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_directly_from_Exception_so_no_companion_copier_is_required()
    {
        Assert.That(typeof(LatticeTenantAccessDeniedException).BaseType, Is.EqualTo(typeof(Exception)),
            "a [GenerateSerializer] exception deriving from a BCL exception subclass needs a "
            + "[RegisterCopier] IDeepCopier<T>; deriving directly from Exception avoids that");
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeTenantAccessDeniedException).IsSealed, Is.True);
            Assert.That(typeof(LatticeTenantAccessDeniedException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeTenantAccessDeniedException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.tad"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LatticeTenantAccessDeniedException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }
}
