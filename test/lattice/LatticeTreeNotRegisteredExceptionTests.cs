namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeTreeNotRegisteredException"/>:
/// every construction overload, the
/// <see cref="LatticeTreeNotRegisteredException.TreeId"/> attribution slot, the
/// <see cref="KeyNotFoundException"/> inheritance the API bindings map to a
/// not-found status, the domain-fault marker, the companion same-silo deep copier
/// that inheritance obliges, and the stable Orleans serialization surface.
/// </summary>
[TestFixture]
public class LatticeTreeNotRegisteredExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_an_empty_tree_id()
    {
        var ex = new LatticeTreeNotRegisteredException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Message_constructor_preserves_the_message_with_an_empty_tree_id()
    {
        var ex = new LatticeTreeNotRegisteredException("no row");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("no row"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("cause");
        var ex = new LatticeTreeNotRegisteredException("no row", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("no row"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void TreeId_constructor_names_the_tree_and_the_refused_operation()
    {
        var ex = new LatticeTreeNotRegisteredException("orders", "SetShardMapAsync");
        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("orders"));
            Assert.That(ex.Message, Does.Contain("'orders'"));
            Assert.That(ex.Message, Does.Contain("SetShardMapAsync"));
            Assert.That(ex.Message, Does.Contain("nothing was created"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void TreeId_constructor_normalises_a_null_tree_id_to_empty()
    {
        var ex = new LatticeTreeNotRegisteredException(treeId: null!, operation: "op");
        Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
    }

    [Test]
    public void Derives_from_KeyNotFoundException_so_the_api_bindings_report_not_found()
    {
        Assert.That(new LatticeTreeNotRegisteredException("x"), Is.InstanceOf<KeyNotFoundException>());
    }

    [Test]
    public void Is_a_domain_fault()
    {
        Assert.That(new LatticeTreeNotRegisteredException("x"), Is.InstanceOf<ILatticeDomainFault>());
    }

    [Test]
    public void Registers_a_same_silo_deep_copier_that_returns_the_same_instance()
    {
        var copierInterface = typeof(Orleans.Serialization.Cloning.IDeepCopier<LatticeTreeNotRegisteredException>);
        var copierType = typeof(LatticeTreeNotRegisteredException).Assembly
            .GetTypes()
            .Single(t => copierInterface.IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract
                && t.GetCustomAttributes(inherit: false).Any(a => a.GetType().Name == "RegisterCopierAttribute"));
        Assert.That(copierType.IsPublic, Is.False, "the hand-written copier is internal");

        var copier = (Orleans.Serialization.Cloning.IDeepCopier<LatticeTreeNotRegisteredException>)Activator.CreateInstance(copierType)!;
        var ex = new LatticeTreeNotRegisteredException("orders", "op");
        Assert.That(copier.DeepCopy(ex, null!), Is.SameAs(ex));
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeTreeNotRegisteredException).IsSealed, Is.True);
            Assert.That(typeof(LatticeTreeNotRegisteredException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeTreeNotRegisteredException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.tnr"));
    }

    [Test]
    public void TreeId_carries_Id_zero()
    {
        var idAttr = typeof(LatticeTreeNotRegisteredException)
            .GetProperty(nameof(LatticeTreeNotRegisteredException.TreeId))!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null);
        Assert.That(idAttr!.Id, Is.EqualTo(0u));
    }
}
