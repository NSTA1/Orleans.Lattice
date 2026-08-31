namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers the two persisted descriptor types,
/// <see cref="GrainIndexPropertyDescriptor"/> and
/// <see cref="GrainIndexDescriptor"/>: their construction, their guards, and the
/// serialization metadata the index registry depends on.
/// </summary>
[TestFixture]
public sealed class GrainIndexDescriptorTests
{
    private static GrainIndexPropertyDescriptor[] SampleProperties() =>
    [
        new("Age", "System.Int32"),
        new("Country", "System.String"),
    ];

    [Test]
    public void Property_descriptor_carries_the_name_and_the_declared_type_name()
    {
        var descriptor = new GrainIndexPropertyDescriptor("Age", "System.Int32");

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Name, Is.EqualTo("Age"));
            Assert.That(descriptor.PropertyTypeName, Is.EqualTo("System.Int32"));
        });
    }

    [TestCase(null, "System.Int32")]
    [TestCase("Age", null)]
    public void Property_descriptor_rejects_a_null_argument(string? name, string? typeName) =>
        Assert.That(
            () => new GrainIndexPropertyDescriptor(name!, typeName!),
            Throws.ArgumentNullException);

    [Test]
    public void Property_descriptors_with_the_same_values_compare_equal() =>
        Assert.That(
            new GrainIndexPropertyDescriptor("Age", "System.Int32"),
            Is.EqualTo(new GrainIndexPropertyDescriptor("Age", "System.Int32")),
            "Drift detection compares stored descriptors against live ones, so value equality "
            + "is load bearing.");

    [Test]
    public void Property_descriptors_differing_in_declared_type_do_not_compare_equal() =>
        Assert.That(
            new GrainIndexPropertyDescriptor("Age", "System.Int64"),
            Is.Not.EqualTo(new GrainIndexPropertyDescriptor("Age", "System.Int32")));

    [Test]
    public void Property_descriptor_members_are_settable_on_construction_so_the_serializer_can_rehydrate_it()
    {
        var descriptor = new GrainIndexPropertyDescriptor("Age", "System.Int32")
        {
            Name = "Years",
            PropertyTypeName = "System.Int64",
        };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Name, Is.EqualTo("Years"));
            Assert.That(descriptor.PropertyTypeName, Is.EqualTo("System.Int64"));
        });
    }

    [Test]
    public void Index_descriptor_carries_every_declared_field()
    {
        var properties = SampleProperties();

        var descriptor = new GrainIndexDescriptor(
            "users",
            "__grainindex/users",
            "MyApp.IUserGrain",
            "MyApp.UserState",
            properties,
            allowReplication: true);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Name, Is.EqualTo("users"));
            Assert.That(descriptor.TreeName, Is.EqualTo("__grainindex/users"));
            Assert.That(descriptor.GrainInterfaceTypeName, Is.EqualTo("MyApp.IUserGrain"));
            Assert.That(descriptor.StateTypeName, Is.EqualTo("MyApp.UserState"));
            Assert.That(descriptor.Properties, Is.EqualTo(properties));
            Assert.That(descriptor.AllowReplication, Is.True);
        });
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public void Index_descriptor_rejects_a_null_reference_argument(int nullArgumentIndex)
    {
        var arguments = new string?[] { "users", "__grainindex/users", "MyApp.IUserGrain", "MyApp.UserState" };
        arguments[nullArgumentIndex] = null;

        Assert.That(
            () => new GrainIndexDescriptor(
                arguments[0]!,
                arguments[1]!,
                arguments[2]!,
                arguments[3]!,
                SampleProperties(),
                allowReplication: false),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Index_descriptor_rejects_a_null_property_list() =>
        Assert.That(
            () => new GrainIndexDescriptor(
                "users", "__grainindex/users", "MyApp.IUserGrain", "MyApp.UserState", null!, false),
            Throws.ArgumentNullException);

    [Test]
    public void Persisted_descriptor_types_carry_the_serialization_metadata_the_registry_needs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                typeof(GrainIndexDescriptor).GetCustomAttributes(typeof(GenerateSerializerAttribute), false),
                Is.Not.Empty);
            Assert.That(
                typeof(GrainIndexPropertyDescriptor).GetCustomAttributes(typeof(GenerateSerializerAttribute), false),
                Is.Not.Empty);
        });
    }
}
