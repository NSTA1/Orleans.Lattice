namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeReservedTreeNamespaceException"/>:
/// every construction overload, the
/// <see cref="LatticeReservedTreeNamespaceException.TreeId"/> attribution slot
/// (including its null-tolerant normalisation), the
/// <see cref="InvalidOperationException"/> inheritance that keeps existing broad
/// catch sites working, the companion same-silo deep copier that inheritance
/// obliges, and the stable Orleans serialization surface.
/// </summary>
[TestFixture]
public class LatticeReservedTreeNamespaceExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_an_empty_tree_id()
    {
        var ex = new LatticeReservedTreeNamespaceException();
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
        var ex = new LatticeReservedTreeNamespaceException("'_lattice_x' is reserved");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("'_lattice_x' is reserved"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("routing rejected the id");
        var ex = new LatticeReservedTreeNamespaceException("reserved namespace", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("reserved namespace"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void TreeId_constructor_preserves_the_rejected_tree_id()
    {
        var ex = new LatticeReservedTreeNamespaceException(
            treeId: "t/acme/orders", message: "'t/' is a reserved structural namespace");
        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("t/acme/orders"));
            Assert.That(ex.Message, Is.EqualTo("'t/' is a reserved structural namespace"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void TreeId_constructor_normalises_a_null_tree_id_to_empty()
    {
        var ex = new LatticeReservedTreeNamespaceException(treeId: null!, message: "reserved");
        Assert.That(ex.TreeId, Is.EqualTo(string.Empty),
            "TreeId is a non-nullable attribution slot; a null id normalises rather than faulting the throw site");
    }

    [Test]
    public void Derives_from_InvalidOperationException_so_existing_broad_catch_sites_still_match()
    {
        var ex = new LatticeReservedTreeNamespaceException("reserved");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Registers_a_same_silo_deep_copier_because_it_derives_from_a_BCL_exception_subclass()
    {
        // A [GenerateSerializer] exception whose base is a BCL exception subclass
        // fails a co-located grain-result deep copy with an opaque
        // KeyNotFoundException unless a no-op copier is registered beside it.
        var copierInterface = typeof(Orleans.Serialization.Cloning.IDeepCopier<LatticeReservedTreeNamespaceException>);
        var registeredCopiers = typeof(LatticeReservedTreeNamespaceException).Assembly
            .GetTypes()
            .Where(t => copierInterface.IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract)
            .Where(t => t.GetCustomAttributes(inherit: false).Any(a => a.GetType().Name == "RegisterCopierAttribute"))
            .ToList();

        Assert.That(registeredCopiers, Is.Not.Empty,
            "LatticeReservedTreeNamespaceException derives from InvalidOperationException, so it needs a companion "
            + "[RegisterCopier] IDeepCopier<T>; without one a co-located grain result fails its deep copy with an "
            + "opaque KeyNotFoundException that masks the real fault");
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeReservedTreeNamespaceException).IsSealed, Is.True);
            Assert.That(typeof(LatticeReservedTreeNamespaceException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeReservedTreeNamespaceException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.rtn"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void TreeId_carries_Id_zero()
    {
        var prop = typeof(LatticeReservedTreeNamespaceException)
            .GetProperty(nameof(LatticeReservedTreeNamespaceException.TreeId));
        Assert.That(prop, Is.Not.Null);
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, "TreeId must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(0u));
    }
}
