using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Assembly-wide guard over the <c>Orleans.Lattice.GrainIndex</c> wire format.
/// It rebuilds its subject list from reflection on every run, so a
/// <c>[GenerateSerializer]</c> type added later is audited automatically rather
/// than only if somebody remembers to hand-write a test for it. The sibling
/// <see cref="SerializableExceptionDeepCopyContractTests"/> sweeps the same
/// assembly for the <i>same-silo</i> deep-copy contract; this fixture covers the
/// <i>cross-silo</i> one.
/// <para>
/// <b>What it proves.</b> For every hand-written <c>[GenerateSerializer]</c>
/// type in the package: a generated codec exists and resolves, so the type
/// survives a real serializer round trip and comes back as its own runtime type
/// (which is what a resolvable wire identity means); the type carries an
/// <c>[Alias]</c> drawn from this package's own <see cref="TypeAliases"/> table
/// rather than an ad-hoc literal, so the alias is covered by the table's own
/// prefix, length, uniqueness and single-referrer gates in
/// <see cref="TypeAliasesTests"/>; and its serialized members are numbered
/// <c>[Id(0)]</c> upwards with no gap and no duplicate, so a member cannot be
/// removed leaving a hole that a later member silently reuses.
/// </para>
/// <para>
/// <b>What it deliberately does not prove.</b> The round trip is driven from an
/// <i>uninitialized</i> instance, so it proves the codec exists and the wire
/// identity resolves - not that every member carries its value across. Proving
/// that needs a fully-populated value and an assertion per member, which is what
/// <see cref="GrainIndexReportSerializationTests"/>,
/// <see cref="GrainIndexProjectionSerializationTests"/> and
/// <c>OrleansGrainIndexSerializerTests</c> do type by type. The numbering check
/// is a shape check, not a compatibility check: it cannot see that an
/// <c>[Id]</c> was renumbered between two builds, only that today's numbering is
/// self-consistent. A type that declares no numbered member passes it
/// vacuously.
/// </para>
/// <para>
/// <b>Where it sits relative to the Orleans analyzers.</b> Orleans' own build
/// analyzers already fail the compile for a serialized member with no
/// <c>[Id]</c> (<c>ORLEANS0004</c>), a duplicated <c>[Id]</c>
/// (<c>ORLEANS0012</c>) and a serialized property with no accessible setter
/// (<c>ORLEANS0101</c>), so the uniqueness assertion here is belt-and-braces
/// that documents the invariant rather than the only thing enforcing it. What
/// this fixture adds on top is the part no analyzer looks at: that the
/// numbering is gap-free from zero, that the <c>[Alias]</c> came from this
/// package's table instead of an ad-hoc literal, and that a codec actually
/// resolves at run time for every declared type.
/// </para>
/// <para>
/// The sweep covers hand-written declarations only: Orleans' own generated
/// codecs, copiers and invokable request types are excluded, because their wire
/// contract belongs to the code generator rather than to this package.
/// </para>
/// </summary>
[TestFixture]
public sealed class SerializableTypeWireContractTests
{
    private static readonly Assembly PackageAssembly = typeof(GrainIndexDescriptor).Assembly;

    /// <summary>
    /// A spread of types the sweep must find: a public class report, a public
    /// record struct, an internal persisted record, and a serializable
    /// exception. Naming them keeps the reflection filter honest - a filter that
    /// silently stopped matching one shape would otherwise leave the whole
    /// fixture passing on a shrunken subject list.
    /// </summary>
    private static readonly Type[] RequiredSubjects =
    [
        typeof(GrainIndexStatus),
        typeof(GrainIndexProgress),
        typeof(GrainIndexDriftStatus),
        typeof(GrainIndexBackfillStatus),
        typeof(GrainIndexBackfillBatchResult),
        typeof(GrainIndexMatch),
        typeof(GrainIndexEnrollmentRecord),
        typeof(GrainIndexKeyEncodingException),
    ];

    /// <summary>
    /// Every alias value this package's <see cref="TypeAliases"/> table declares,
    /// excluding the constant that describes the table's own prefix convention
    /// rather than naming a type.
    /// </summary>
    private static readonly HashSet<string> AliasTable = typeof(TypeAliases)
        .GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static)
        .Where(f => f.IsLiteral
            && f.FieldType == typeof(string)
            && f.Name != nameof(TypeAliases.Prefix))
        .Select(f => (string)f.GetRawConstantValue()!)
        .ToHashSet(StringComparer.Ordinal);

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

    [Test]
    public void The_sweep_discovers_this_packages_hand_written_serializable_types()
    {
        var discovered = SerializableTypes().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(discovered, Is.Not.Empty,
                $"No [GenerateSerializer] types were discovered in {PackageAssembly.GetName().Name}; "
                + "the whole fixture would be inert. Verify the reflection filter.");

            foreach (var subject in RequiredSubjects)
            {
                Assert.That(discovered, Does.Contain(subject),
                    $"{subject.FullName} carries [GenerateSerializer] but the sweep did not find it, "
                    + "so the filter no longer matches every declaration shape this package uses.");
            }
        });
    }

    [TestCaseSource(nameof(SerializableTypes))]
    public void Every_serializable_type_round_trips_through_the_real_serializer(Type type)
    {
        var instance = RuntimeHelpers.GetUninitializedObject(type);

        var roundTripped = _serializer.Deserialize<object>(_serializer.SerializeToArray(instance));

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped, Is.Not.Null,
                $"{type.FullName} deserialized to null.");
            Assert.That(roundTripped.GetType(), Is.EqualTo(type),
                $"{type.FullName} did not round-trip to its own runtime type, so its wire identity "
                + "does not resolve: the generated codec is missing or its alias is not registered.");
        });
    }

    [TestCaseSource(nameof(SerializableTypes))]
    public void Every_serializable_type_declares_an_alias_from_this_packages_table(Type type)
    {
        var alias = type.GetCustomAttribute<AliasAttribute>(inherit: false);

        Assert.That(alias, Is.Not.Null,
            $"{type.FullName} has [GenerateSerializer] but no [Alias], so its wire identity is its "
            + "CLR name and a rename would break every persisted and in-flight payload.");
        Assert.That(AliasTable.Contains(alias!.Alias), Is.True,
            $"{type.FullName} carries the ad-hoc alias '{alias.Alias}'. Every alias must be a constant "
            + $"on {nameof(TypeAliases)} so it is covered by that table's prefix, length, uniqueness "
            + "and single-referrer gates.");
    }

    [TestCaseSource(nameof(SerializableTypes))]
    public void Every_serializable_type_numbers_its_members_sequentially_from_zero(Type type)
    {
        var ids = DeclaredIds(type);

        Assert.Multiple(() =>
        {
            Assert.That(ids, Is.Unique,
                $"{type.FullName} declares the same [Id] on two members, which makes its payload "
                + $"ambiguous. Ids: {Render(ids)}.");
            Assert.That(
                ids.OrderBy(id => id),
                Is.EqualTo(Enumerable.Range(0, ids.Count).Select(i => (uint)i)).AsCollection,
                $"{type.FullName} must number its serialized members [Id(0)] upwards with no gap. "
                + $"A hole left by a removed member is how a later member silently inherits a retired "
                + $"field's wire slot. Ids: {Render(ids)}.");
        });
    }

    /// <summary>
    /// The hand-written, concrete <c>[GenerateSerializer]</c> types declared in
    /// the package assembly, ordered for a stable report and a stable test-case
    /// naming.
    /// </summary>
    private static IEnumerable<Type> SerializableTypes() =>
        PackageAssembly
            .GetTypes()
            .Where(t => !t.IsAbstract
                && !t.IsInterface
                && !t.IsGenericTypeDefinition
                && t.GetCustomAttribute<System.CodeDom.Compiler.GeneratedCodeAttribute>(inherit: false) is null
                && t.GetCustomAttribute<GenerateSerializerAttribute>(inherit: false) is not null)
            .OrderBy(t => t.FullName, StringComparer.Ordinal);

    /// <summary>
    /// The <c>[Id]</c> values the type itself declares, across its instance
    /// properties and fields of any visibility. Only declared members count: no
    /// base type in this package contributes numbered members, and inherited
    /// ones would belong to the base type's own contract.
    /// </summary>
    private static IReadOnlyList<uint> DeclaredIds(Type type)
    {
        const BindingFlags Members =
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;

        var ids = new List<uint>();

        foreach (var property in type.GetProperties(Members))
        {
            if (property.GetCustomAttribute<IdAttribute>(inherit: false) is { } id)
            {
                ids.Add(id.Id);
            }
        }

        foreach (var field in type.GetFields(Members))
        {
            if (field.GetCustomAttribute<IdAttribute>(inherit: false) is { } id)
            {
                ids.Add(id.Id);
            }
        }

        return ids;
    }

    private static string Render(IReadOnlyList<uint> ids) =>
        ids.Count == 0 ? "(none)" : string.Join(", ", ids.OrderBy(id => id));
}
