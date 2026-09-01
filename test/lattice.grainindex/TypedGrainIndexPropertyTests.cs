namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers the projected-property model: <see cref="GrainIndexProperty{TState}"/>
/// and its only concrete form
/// <see cref="TypedGrainIndexProperty{TState, TProperty}"/>. The accessor is what
/// the projection path calls per grain per mutation, so both the typed and the
/// boxing read are exercised.
/// </summary>
[TestFixture]
public sealed class TypedGrainIndexPropertyTests
{
    private static TypedGrainIndexProperty<TestGrainState, int> AgeProperty() =>
        new(nameof(TestGrainState.Age), static state => state.Age);

    [Test]
    public void Constructor_records_the_name_and_the_declared_property_type()
    {
        var property = AgeProperty();

        Assert.Multiple(() =>
        {
            Assert.That(property.Name, Is.EqualTo("Age"));
            Assert.That(property.PropertyType, Is.EqualTo(typeof(int)));
        });
    }

    [Test]
    public void Constructor_rejects_a_null_name() =>
        Assert.That(
            () => new TypedGrainIndexProperty<TestGrainState, int>(null!, static s => s.Age),
            Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("   ")]
    public void Constructor_rejects_an_empty_or_whitespace_name(string name) =>
        Assert.That(
            () => new TypedGrainIndexProperty<TestGrainState, int>(name, static s => s.Age),
            Throws.ArgumentException);

    [Test]
    public void Constructor_rejects_a_null_accessor() =>
        Assert.That(
            () => new TypedGrainIndexProperty<TestGrainState, int>("Age", null!),
            Throws.ArgumentNullException);

    [Test]
    public void Get_typed_value_reads_the_property_through_the_compiled_accessor()
    {
        var property = AgeProperty();

        Assert.That(property.GetTypedValue(new TestGrainState { Age = 41 }), Is.EqualTo(41));
    }

    [Test]
    public void Get_value_reads_the_same_property_through_the_base_contract()
    {
        GrainIndexProperty<TestGrainState> property = AgeProperty();

        Assert.That(property.GetValue(new TestGrainState { Age = 41 }), Is.EqualTo(41));
    }

    [Test]
    public void Accessor_is_the_delegate_the_projection_path_invokes()
    {
        Func<TestGrainState, int> accessor = static state => state.Age;

        var property = new TypedGrainIndexProperty<TestGrainState, int>("Age", accessor);

        Assert.That(property.Accessor, Is.SameAs(accessor),
            "The accessor is stored, not rebuilt: a declaration compiles its selector once and the "
            + "projection path reuses that delegate.");
    }

    [Test]
    public void Descriptor_is_computed_once_and_carries_the_name_and_declared_type_name()
    {
        var property = AgeProperty();

        Assert.Multiple(() =>
        {
            Assert.That(property.Descriptor.Name, Is.EqualTo("Age"));
            Assert.That(property.Descriptor.PropertyTypeName, Is.EqualTo(typeof(int).FullName));
        });
    }

    [Test]
    public void Reference_typed_property_is_read_and_described_the_same_way()
    {
        var property = new TypedGrainIndexProperty<TestGrainState, string>(
            nameof(TestGrainState.Country),
            static state => state.Country);

        Assert.Multiple(() =>
        {
            Assert.That(property.GetTypedValue(new TestGrainState { Country = "GB" }), Is.EqualTo("GB"));
            Assert.That(property.Descriptor.PropertyTypeName, Is.EqualTo("System.String"));
        });
    }

    [Test]
    public void Nullable_valued_property_reports_its_nullable_declared_type()
    {
        var property = new TypedGrainIndexProperty<TestGrainState, DateTimeOffset?>(
            nameof(TestGrainState.LastSeen),
            static state => state.LastSeen);

        Assert.Multiple(() =>
        {
            Assert.That(property.PropertyType, Is.EqualTo(typeof(DateTimeOffset?)));
            Assert.That(property.GetValue(new TestGrainState()), Is.Null);
        });
    }
}
