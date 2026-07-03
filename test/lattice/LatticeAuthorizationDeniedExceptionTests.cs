using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeAuthorizationDeniedException"/>:
/// its construction overloads, the sealed / public contract, its derivation from
/// <see cref="UnauthorizedAccessException"/>, the
/// <see cref="LatticeAuthorizationDeniedException.TreeId"/> /
/// <see cref="LatticeAuthorizationDeniedException.Operation"/> /
/// <see cref="LatticeAuthorizationDeniedException.SubjectId"/> /
/// <see cref="LatticeAuthorizationDeniedException.Reason"/> attribution slots, and
/// the stable Orleans serialization surface (alias, <c>[Id]</c> members, and a
/// full serialize/deserialize round-trip) the manifest relies on to surface the
/// typed denial across the grain boundary from the enforcing <c>LatticeGrain</c>
/// back to the client.
/// </summary>
[TestFixture]
public class LatticeAuthorizationDeniedExceptionTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeAuthorizationDeniedException> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeAuthorizationDeniedException>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Parameterless_constructor_initialises_with_empty_context()
    {
        var ex = new LatticeAuthorizationDeniedException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
            Assert.That(ex.Operation, Is.EqualTo(LatticeOperation.None));
            Assert.That(ex.SubjectId, Is.EqualTo(string.Empty));
            Assert.That(ex.Reason, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Message_constructor_preserves_message_with_empty_context()
    {
        var ex = new LatticeAuthorizationDeniedException("denied");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("denied"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
            Assert.That(ex.SubjectId, Is.EqualTo(string.Empty));
            Assert.That(ex.Reason, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("underlying");
        var ex = new LatticeAuthorizationDeniedException("denied", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("denied"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Full_constructor_preserves_all_diagnostic_context()
    {
        var ex = new LatticeAuthorizationDeniedException(
            treeId: "sessions",
            operation: LatticeOperation.Write,
            subjectId: "alice",
            reason: "no matching rule");
        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("sessions"));
            Assert.That(ex.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(ex.SubjectId, Is.EqualTo("alice"));
            Assert.That(ex.Reason, Is.EqualTo("no matching rule"));
        });
    }

    [Test]
    public void Full_constructor_message_names_the_subject_operation_and_tree()
    {
        var ex = new LatticeAuthorizationDeniedException("sessions", LatticeOperation.Delete, "alice", "no rule");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("alice"));
            Assert.That(ex.Message, Does.Contain("Delete"));
            Assert.That(ex.Message, Does.Contain("sessions"));
            Assert.That(ex.Message, Does.Contain("no rule"));
        });
    }

    [Test]
    public void Full_constructor_rejects_null_treeId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeAuthorizationDeniedException(null!, LatticeOperation.Write, "alice", "r"));
    }

    [Test]
    public void Full_constructor_rejects_null_subjectId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeAuthorizationDeniedException("t", LatticeOperation.Write, null!, "r"));
    }

    [Test]
    public void Full_constructor_rejects_null_reason()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeAuthorizationDeniedException("t", LatticeOperation.Write, "alice", null!));
    }

    [Test]
    public void Derives_from_UnauthorizedAccessException()
    {
        var ex = new LatticeAuthorizationDeniedException("m");
        Assert.That(ex, Is.InstanceOf<UnauthorizedAccessException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeAuthorizationDeniedException).IsSealed, Is.True);
            Assert.That(typeof(LatticeAuthorizationDeniedException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LatticeAuthorizationDeniedException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.azd"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LatticeAuthorizationDeniedException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void Serialized_members_carry_sequential_Id_attributes()
    {
        Assert.Multiple(() =>
        {
            AssertHasId(nameof(LatticeAuthorizationDeniedException.TreeId), 0);
            AssertHasId(nameof(LatticeAuthorizationDeniedException.Operation), 1);
            AssertHasId(nameof(LatticeAuthorizationDeniedException.SubjectId), 2);
            AssertHasId(nameof(LatticeAuthorizationDeniedException.Reason), 3);
        });
    }

    [Test]
    public void Round_trips_all_context_through_the_Orleans_serializer()
    {
        var original = new LatticeAuthorizationDeniedException(
            treeId: "orders",
            operation: LatticeOperation.RangeDelete,
            subjectId: "carol",
            reason: "range not fully authorized");

        var bytes = _serializer.SerializeToArray(original);
        var restored = _serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(restored.TreeId, Is.EqualTo("orders"));
            Assert.That(restored.Operation, Is.EqualTo(LatticeOperation.RangeDelete));
            Assert.That(restored.SubjectId, Is.EqualTo("carol"));
            Assert.That(restored.Reason, Is.EqualTo("range not fully authorized"));
            Assert.That(restored.Message, Is.EqualTo(original.Message));
        });
    }

    private static void AssertHasId(string propertyName, uint expectedId)
    {
        var prop = typeof(LatticeAuthorizationDeniedException).GetProperty(propertyName);
        Assert.That(prop, Is.Not.Null, $"{propertyName} must exist");
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, $"{propertyName} must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(expectedId), $"{propertyName} must be [Id({expectedId})]");
    }
}
