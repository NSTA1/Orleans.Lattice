using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The described runtime-view provider surface (#5369): the
/// <see cref="LatticeViewRegistrationBuilder"/> overload that carries a
/// <see cref="LatticeRuntimeViewProviderDescription"/>, the description and field
/// types, and the catalog listing that offers host providers for runtime creation
/// while withholding the library's own predicate provider.
/// </summary>
[TestFixture]
public sealed class RuntimeViewProviderDescriptionTests
{
    private static LatticeViewDefinition Define(LatticeRuntimeViewProjectionContext context) =>
        new(context.ViewName, new PredicateLatticeViewProjection());

    [Test]
    public void AddRuntimeProjectionProvider_with_a_description_records_it()
    {
        var description = new LatticeRuntimeViewProviderDescription(
            "Orders grouped by status",
            [new LatticeRuntimeViewProviderField("status", "Status") { Required = true }]);
        var builder = new LatticeViewRegistrationBuilder();

        var returned = builder.AddRuntimeProjectionProvider("app.by-status.v1", description, (_, context) => Define(context));

        Assert.Multiple(() =>
        {
            Assert.That(returned, Is.SameAs(builder));
            Assert.That(builder.RuntimeProviders.Single().Description, Is.SameAs(description));
        });
    }

    [Test]
    public void AddRuntimeProjectionProvider_without_a_description_records_none()
    {
        var builder = new LatticeViewRegistrationBuilder();

        builder.AddRuntimeProjectionProvider("app.orders.v1", (_, context) => Define(context));

        Assert.That(builder.RuntimeProviders.Single().Description, Is.Null);
    }

    [Test]
    public void AddRuntimeProjectionProvider_with_a_description_validates_its_arguments()
    {
        var builder = new LatticeViewRegistrationBuilder();
        var description = new LatticeRuntimeViewProviderDescription("Orders");

        Assert.Multiple(() =>
        {
            Assert.That(() => builder.AddRuntimeProjectionProvider("k", null!, (_, context) => Define(context)), Throws.ArgumentNullException);
            Assert.That(() => builder.AddRuntimeProjectionProvider("k", description, null!), Throws.ArgumentNullException);
            Assert.That(() => builder.AddRuntimeProjectionProvider("", description, (_, context) => Define(context)), Throws.ArgumentException);
            Assert.That(
                () => builder.AddRuntimeProjectionProvider(PredicateRuntimeViewProjectionCodec.ProviderKey, description, (_, context) => Define(context)),
                Throws.InvalidOperationException);
        });
    }

    [Test]
    public void Description_keeps_its_fields_in_order_and_defaults_to_none()
    {
        var fields = new[]
        {
            new LatticeRuntimeViewProviderField("status", "Status"),
            new LatticeRuntimeViewProviderField("limit", "Limit") { Kind = LatticeRuntimeViewProviderFieldKind.Number },
        };

        var described = new LatticeRuntimeViewProviderDescription("Orders", fields);
        var bare = new LatticeRuntimeViewProviderDescription("Orders");

        Assert.Multiple(() =>
        {
            Assert.That(described.Description, Is.EqualTo("Orders"));
            Assert.That(described.PayloadFields.Select(field => field.Name), Is.EqualTo(new[] { "status", "limit" }));
            Assert.That(bare.PayloadFields, Is.Empty);
        });
    }

    [Test]
    public void Description_rejects_an_empty_text_a_null_field_and_a_repeated_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeRuntimeViewProviderDescription(""), Throws.ArgumentException);
            Assert.That(() => new LatticeRuntimeViewProviderDescription("Orders", [null!]), Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeRuntimeViewProviderDescription("Orders", [new("a", "A"), new("a", "Again")]),
                Throws.ArgumentException.With.Message.Contains("'a'"));
        });
    }

    [Test]
    public void Field_carries_its_name_label_and_options()
    {
        var field = new LatticeRuntimeViewProviderField("enabled", "Enabled")
        {
            Description = "Whether to project",
            Required = true,
            Kind = LatticeRuntimeViewProviderFieldKind.Boolean,
        };

        Assert.Multiple(() =>
        {
            Assert.That(field.Name, Is.EqualTo("enabled"));
            Assert.That(field.Label, Is.EqualTo("Enabled"));
            Assert.That(field.Description, Is.EqualTo("Whether to project"));
            Assert.That(field.Required, Is.True);
            Assert.That(field.Kind, Is.EqualTo(LatticeRuntimeViewProviderFieldKind.Boolean));
            Assert.That(new LatticeRuntimeViewProviderField("a", "A").Kind, Is.EqualTo(LatticeRuntimeViewProviderFieldKind.Text));
            Assert.That(() => new LatticeRuntimeViewProviderField("", "A"), Throws.ArgumentException);
            Assert.That(() => new LatticeRuntimeViewProviderField("a", ""), Throws.ArgumentException);
        });
    }

    [Test]
    public void Catalog_offers_host_providers_by_key_and_withholds_the_predicate_provider()
    {
        var catalog = new RuntimeViewProjectionProviderCatalog(
        [
            new RuntimeViewProjectionProviderRegistration("zeta", (_, context) => Define(context)),
            new RuntimeViewProjectionProviderRegistration(PredicateRuntimeViewProjectionCodec.ProviderKey, (_, context) => Define(context)),
            new RuntimeViewProjectionProviderRegistration("alpha", (_, context) => Define(context), new LatticeRuntimeViewProviderDescription("First")),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.HostProviders.Select(provider => provider.ProviderKey), Is.EqualTo(new[] { "alpha", "zeta" }));
            Assert.That(catalog.TryGet(PredicateRuntimeViewProjectionCodec.ProviderKey), Is.Not.Null, "the predicate provider still rehydrates its views");
        });
    }
}
