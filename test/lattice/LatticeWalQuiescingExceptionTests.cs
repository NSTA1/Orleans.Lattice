namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeWalQuiescingException"/>: every
/// construction overload, the <see cref="InvalidOperationException"/> inheritance
/// that keeps existing broad catch sites working, the companion same-silo deep
/// copier that inheritance obliges, and the stable Orleans serialization alias.
/// </summary>
[TestFixture]
public class LatticeWalQuiescingExceptionTests
{
    [Test]
    public void Parameterless_constructor_yields_a_non_null_message_and_no_inner()
    {
        var ex = new LatticeWalQuiescingException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_constructor_preserves_the_message()
    {
        var ex = new LatticeWalQuiescingException("partition 3 is quiescing for a WAL move");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("partition 3 is quiescing for a WAL move"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new TimeoutException("drain timed out");
        var ex = new LatticeWalQuiescingException("partition 3 is quiescing", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("partition 3 is quiescing"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException_so_existing_broad_catch_sites_still_match()
    {
        var ex = new LatticeWalQuiescingException("quiescing");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Registers_a_same_silo_deep_copier_because_it_derives_from_a_BCL_exception_subclass()
    {
        var copierInterface = typeof(Orleans.Serialization.Cloning.IDeepCopier<LatticeWalQuiescingException>);
        var registeredCopiers = typeof(LatticeWalQuiescingException).Assembly
            .GetTypes()
            .Where(t => copierInterface.IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract)
            .Where(t => t.GetCustomAttributes(inherit: false).Any(a => a.GetType().Name == "RegisterCopierAttribute"))
            .ToList();

        Assert.That(registeredCopiers, Is.Not.Empty,
            "LatticeWalQuiescingException derives from InvalidOperationException, so it needs a companion "
            + "[RegisterCopier] IDeepCopier<T>; without one a co-located grain result fails its deep copy with an "
            + "opaque KeyNotFoundException that masks the real fault");
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeWalQuiescingException).IsSealed, Is.True);
            Assert.That(typeof(LatticeWalQuiescingException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeWalQuiescingException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.Not.Empty,
            "the alias value pins the Orleans wire format; it must be present");
    }
}
