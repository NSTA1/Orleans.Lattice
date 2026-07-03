namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeQuotaExceededException"/>: its
/// construction overloads, the inheritance / sealed contract, the
/// <see cref="LatticeQuotaExceededException.TreeId"/> /
/// <see cref="LatticeQuotaExceededException.Dimension"/> /
/// <see cref="LatticeQuotaExceededException.Current"/> /
/// <see cref="LatticeQuotaExceededException.Limit"/> attribution slots, and the
/// stable Orleans serialization surface (alias + <c>[Id]</c> members) the
/// manifest relies on to surface the typed exception across grain boundaries.
/// </summary>
[TestFixture]
public class LatticeQuotaExceededExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_empty_context()
    {
        var ex = new LatticeQuotaExceededException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
            Assert.That(ex.Dimension, Is.EqualTo(string.Empty));
            Assert.That(ex.Current, Is.EqualTo(0L));
            Assert.That(ex.Limit, Is.EqualTo(0L));
        });
    }

    [Test]
    public void Message_constructor_preserves_message_with_empty_context()
    {
        var ex = new LatticeQuotaExceededException("live-key cap reached");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("live-key cap reached"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
            Assert.That(ex.Dimension, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("underlying");
        var ex = new LatticeQuotaExceededException("cap reached", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("cap reached"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Full_constructor_preserves_all_diagnostic_context()
    {
        var ex = new LatticeQuotaExceededException(
            "tree 'sessions' rejected: live key count 5000 has reached the cap of 5000.",
            treeId: "sessions",
            dimension: LatticeQuotaExceededException.KeysDimension,
            current: 5000,
            limit: 5000);
        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("sessions"));
            Assert.That(ex.Dimension, Is.EqualTo("keys"));
            Assert.That(ex.Current, Is.EqualTo(5000L));
            Assert.That(ex.Limit, Is.EqualTo(5000L));
        });
    }

    [Test]
    public void Full_constructor_rejects_null_treeId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeQuotaExceededException("m", treeId: null!, dimension: "keys", current: 1, limit: 1));
    }

    [Test]
    public void Full_constructor_rejects_null_dimension()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeQuotaExceededException("m", treeId: "t", dimension: null!, current: 1, limit: 1));
    }

    [Test]
    public void Dimension_constants_expose_the_two_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeQuotaExceededException.KeysDimension, Is.EqualTo("keys"));
            Assert.That(LatticeQuotaExceededException.BytesDimension, Is.EqualTo("bytes"));
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        var ex = new LatticeQuotaExceededException("m");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_distinct_from_LatticeSaturatedException()
    {
        var quota = new LatticeQuotaExceededException("q");
        var sat = new LatticeSaturatedException("s");
        Assert.Multiple(() =>
        {
            Assert.That(quota, Is.Not.InstanceOf<LatticeSaturatedException>());
            Assert.That(sat, Is.Not.InstanceOf<LatticeQuotaExceededException>());
        });
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeQuotaExceededException).IsSealed, Is.True);
            Assert.That(typeof(LatticeQuotaExceededException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeQuotaExceededException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.lqe"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LatticeQuotaExceededException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void Serialized_members_carry_sequential_Id_attributes()
    {
        Assert.Multiple(() =>
        {
            AssertHasId(nameof(LatticeQuotaExceededException.TreeId), 0);
            AssertHasId(nameof(LatticeQuotaExceededException.Dimension), 1);
            AssertHasId(nameof(LatticeQuotaExceededException.Current), 2);
            AssertHasId(nameof(LatticeQuotaExceededException.Limit), 3);
        });
    }

    private static void AssertHasId(string propertyName, uint expectedId)
    {
        var prop = typeof(LatticeQuotaExceededException).GetProperty(propertyName);
        Assert.That(prop, Is.Not.Null, $"{propertyName} must exist");
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, $"{propertyName} must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(expectedId), $"{propertyName} must be [Id({expectedId})]");
    }
}
