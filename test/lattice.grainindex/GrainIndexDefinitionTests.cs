namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexDefinition{TGrain, TState}"/>: the shape it
/// records, the guards on its construction, and the descriptor it produces when
/// combined with resolved options.
/// </summary>
[TestFixture]
public sealed class GrainIndexDefinitionTests
{
    private static GrainIndexProperty<TestGrainState>[] TwoProperties() =>
    [
        new TypedGrainIndexProperty<TestGrainState, int>("Age", static s => s.Age),
        new TypedGrainIndexProperty<TestGrainState, string>("Country", static s => s.Country),
    ];

    private static GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState> Definition(
        string name = "users") =>
        new(name, StringGrainKeyCodec<ITestStringKeyedGrain>.Instance, TwoProperties());

    [Test]
    public void Definition_reports_the_indexed_grain_and_state_types()
    {
        var definition = Definition();

        Assert.Multiple(() =>
        {
            Assert.That(definition.Name, Is.EqualTo("users"));
            Assert.That(definition.GrainInterfaceType, Is.EqualTo(typeof(ITestStringKeyedGrain)));
            Assert.That(definition.StateType, Is.EqualTo(typeof(TestGrainState)));
        });
    }

    [Test]
    public void Definition_exposes_the_typed_key_codec_and_the_same_codec_non_generically()
    {
        var definition = Definition();

        Assert.Multiple(() =>
        {
            Assert.That(
                definition.KeyCodec,
                Is.SameAs(StringGrainKeyCodec<ITestStringKeyedGrain>.Instance));
            Assert.That(
                ((IGrainIndexDefinition)definition).KeyCodec,
                Is.SameAs(definition.KeyCodec));
        });
    }

    [Test]
    public void Definition_preserves_the_declared_property_order_in_both_views()
    {
        var definition = Definition();

        Assert.Multiple(() =>
        {
            Assert.That(definition.Properties.Select(p => p.Name), Is.EqualTo(new[] { "Age", "Country" }));
            Assert.That(
                definition.PropertyDescriptors.Select(d => d.Name),
                Is.EqualTo(new[] { "Age", "Country" }));
        });
    }

    [Test]
    public void Property_descriptors_carry_the_declared_clr_type_the_entry_encoder_needs() =>
        Assert.That(
            Definition().PropertyDescriptors.Select(d => d.PropertyTypeName),
            Is.EqualTo(new[] { "System.Int32", "System.String" }),
            "The entry encoder chooses an order-preserving encoding from the declared type, and the "
            + "query router decides range-routability from it, so it has to survive into the "
            + "persisted descriptor.");

    [Test]
    public void Definition_rejects_a_null_name() =>
        Assert.That(
            () => new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
                null!, StringGrainKeyCodec<ITestStringKeyedGrain>.Instance, TwoProperties()),
            Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("   ")]
    public void Definition_rejects_an_empty_or_whitespace_name(string name) =>
        Assert.That(
            () => new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
                name, StringGrainKeyCodec<ITestStringKeyedGrain>.Instance, TwoProperties()),
            Throws.ArgumentException);

    [Test]
    public void Definition_rejects_a_null_key_codec() =>
        Assert.That(
            () => new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
                "users", null!, TwoProperties()),
            Throws.ArgumentNullException);

    [Test]
    public void Definition_rejects_a_null_property_list() =>
        Assert.That(
            () => new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
                "users", StringGrainKeyCodec<ITestStringKeyedGrain>.Instance, null!),
            Throws.ArgumentNullException);

    [Test]
    public void Definition_rejects_a_null_property_element() =>
        Assert.That(
            () => new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
                "users",
                StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
                [null!]),
            Throws.ArgumentException.With.Message.Contains("users"));

    [Test]
    public void Definition_accepts_an_empty_property_set_and_leaves_it_to_the_validator() =>
        Assert.That(
            new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
                "users", StringGrainKeyCodec<ITestStringKeyedGrain>.Instance, []).PropertyDescriptors,
            Is.Empty,
            "An empty projection set is reported at startup by the declaration validator, which can "
            + "name every offending index at once.");

    [Test]
    public void Describe_combines_the_declaration_shape_with_the_resolved_options()
    {
        var options = new GrainIndexOptions
        {
            TreeName = "__grainindex/users",
            AllowReplication = true,
        };

        var descriptor = Definition().Describe(options);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Name, Is.EqualTo("users"));
            Assert.That(descriptor.TreeName, Is.EqualTo("__grainindex/users"));
            Assert.That(
                descriptor.GrainInterfaceTypeName,
                Is.EqualTo(typeof(ITestStringKeyedGrain).FullName));
            Assert.That(descriptor.StateTypeName, Is.EqualTo(typeof(TestGrainState).FullName));
            Assert.That(descriptor.Properties.Select(p => p.Name), Is.EqualTo(new[] { "Age", "Country" }));
            Assert.That(descriptor.AllowReplication, Is.True);
        });
    }

    [Test]
    public void Describe_rejects_null_options() =>
        Assert.That(() => Definition().Describe(null!), Throws.ArgumentNullException);
}
