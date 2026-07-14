using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Lattice.Api.State;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Guards the central risk of the contract-extraction refactor: every
/// Orleans-serializable API DTO now lives in the
/// <c>Orleans.Lattice.Api.Abstractions</c> assembly rather than in its former
/// facade assembly. This fixture reflects over every <c>[GenerateSerializer]</c>
/// type in the abstractions assembly and round-trips it through a real Orleans
/// serializer, proving each type still has a generated codec and a resolvable
/// wire identity from its new assembly home. A moved type that lost its
/// serialization wiring (or whose alias no longer resolves) fails here.
/// </summary>
[TestFixture]
public class AbstractionsSerializationTests
{
    private static readonly Assembly AbstractionsAssembly = typeof(ILatticeStateQuery).Assembly;

    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <summary>
    /// The reflection filter must actually discover serializable DTOs; a zero
    /// count would let the round-trip test pass vacuously if the filter broke.
    /// </summary>
    [Test]
    public void The_abstractions_assembly_exposes_serializable_dtos()
    {
        Assert.That(SerializableTypes(), Is.Not.Empty,
            "Expected the abstractions assembly to contain [GenerateSerializer] DTO types.");
    }

    [TestCaseSource(nameof(SerializableTypes))]
    public void Every_serializable_type_round_trips_from_the_abstractions_assembly(Type type)
    {
        var instance = RuntimeHelpers.GetUninitializedObject(type);

        var bytes = _serializer.SerializeToArray(instance);
        var roundTripped = _serializer.Deserialize<object>(bytes);

        Assert.That(roundTripped, Is.Not.Null,
            $"{type.FullName} deserialized to null.");
        Assert.That(roundTripped.GetType(), Is.EqualTo(type),
            $"{type.FullName} did not round-trip to its own runtime type (wire identity did not resolve).");
    }

    /// <summary>
    /// Every serializable DTO must carry a stable <see cref="AliasAttribute"/>
    /// so its wire identity survives a CLR rename. This convention is what makes
    /// the assembly move safe, so it is asserted alongside the round-trip.
    /// </summary>
    [TestCaseSource(nameof(SerializableTypes))]
    public void Every_serializable_type_declares_a_stable_alias(Type type)
    {
        var alias = type.GetCustomAttribute<AliasAttribute>(inherit: false);

        Assert.That(alias, Is.Not.Null,
            $"{type.FullName} has [GenerateSerializer] but no [Alias]; its wire identity is not stable.");
        Assert.That(alias!.Alias, Is.Not.Null.And.Not.Empty,
            $"{type.FullName} has an empty [Alias] value.");
    }

    private static IEnumerable<Type> SerializableTypes()
        => AbstractionsAssembly
            .GetTypes()
            .Where(t =>
                !t.IsAbstract
                && !t.IsInterface
                && !t.IsGenericTypeDefinition
                && t.GetCustomAttributes().Any(a => a.GetType().Name == "GenerateSerializerAttribute"))
            .OrderBy(t => t.FullName, StringComparer.Ordinal);
}
