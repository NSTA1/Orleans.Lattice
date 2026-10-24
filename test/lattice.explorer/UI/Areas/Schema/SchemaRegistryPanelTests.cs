using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>Discovery and safe runtime catalog registration in the Versions tab.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaRegistryPanelTests : SchemaTestContext
{
    [Test]
    public void An_administrator_can_discover_and_register_declarative_migration_definitions()
    {
        UseEstate();
        Schema.DefaultCapabilities = tree => FakeSchemaControl.All(tree) with { SupportsRuntimeRegistration = true };
        Schema.Registry = new()
        {
            Descriptors = [new(7, 3, "Orders")],
            SupportedTransformIds = ["orders-v4"],
        };
        var cut = RenderAt<SchemaTreePage>("schema/orders?tab=versions");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("orders-v4")));
        Assert.That(cut.Markup, Does.Contain("Orders"));

        cut.FindAll("button").Single(button => button.TextContent == "Register definitions").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.LastRegistration, Is.Not.Null);
            Assert.That(Schema.LastRegistration!.Descriptors.Select(item => item.Version), Is.EqualTo(new uint[] { 1, 2 }));
            var transform = Schema.LastRegistration.Upcasters.Single().Transform!.Value;
            Assert.That(transform.Kind, Is.EqualTo(LatticeValueTransformKind.Passthrough));
            Assert.That(transform.Children!.Single().Kind, Is.EqualTo(LatticeValueTransformKind.RenameMember));
            Assert.That(ToastService.Toasts.Any(toast => toast.Message == "Schema definitions are registered."), Is.True);
        });
    }

    [Test]
    public void Malformed_registration_is_explained_without_sending_it_or_echoing_input()
    {
        UseEstate();
        Schema.DefaultCapabilities = tree => FakeSchemaControl.All(tree) with { SupportsRuntimeRegistration = true };
        var cut = RenderAt<SchemaTreePage>("schema/orders?tab=versions");
        cut.WaitUntil(() => Assert.That(cut.FindAll("textarea"), Has.Count.EqualTo(1)));
        cut.Find("textarea").Change("secret-not-json");

        cut.FindAll("button").Single(button => button.TextContent == "Register definitions").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-error").TextContent, Does.Contain("Enter a registration JSON object"));
            Assert.That(cut.Find(".lt-schema-error").TextContent, Does.Not.Contain("secret-not-json"));
            Assert.That(Schema.CountOf("RegisterSchema"), Is.Zero);
        });
    }

    [Test]
    public void A_reader_can_inspect_definitions_but_cannot_register_them()
    {
        UseEstate();
        Schema.DefaultCapabilities = tree => FakeSchemaControl.ReadOnly(tree) with { SupportsRuntimeRegistration = true };
        var cut = RenderAt<SchemaTreePage>("schema/orders?tab=versions");
        cut.WaitUntil(() => Assert.That(Schema.CountOf("GetRegistry"), Is.EqualTo(1)));
        Assert.That(cut.FindAll("textarea"), Is.Empty);
        Assert.That(cut.FindAll("button").Any(button => button.TextContent == "Register definitions"), Is.False);
    }

    [TestCase(false, true)]
    [TestCase(true, false)]
    public void Installed_support_is_separate_from_permissions(bool enforcement, bool versioning)
    {
        UseEstate();
        Schema.DefaultCapabilities = tree => FakeSchemaControl.All(tree) with
        {
            SupportsEnforcement = enforcement,
            SupportsVersioning = versioning,
            SupportsCompliance = enforcement,
            SupportsDeadLetters = enforcement,
            SupportsRemediation = enforcement && versioning,
        };
        var cut = RenderAt<SchemaTreePage>("schema/orders?tab=versions");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tab]"), Has.Count.EqualTo(5)));
        Assert.That(Schema.CountOf("GetPolicy") > 0, Is.EqualTo(enforcement));
        Assert.That(Schema.CountOf("GetVersionConfig") > 0, Is.EqualTo(versioning));
        if (!versioning)
        {
            Assert.That(cut.Find("[role=tabpanel]").TextContent, Does.Contain("Versioning is not available"));
            Assert.That(cut.FindAll("[role=tabpanel] button").Any(button => button.TextContent == "Change config"), Is.False);
        }

    }

    [Test]
    public void A_direct_route_without_installed_features_explains_support_rather_than_denial()
    {
        UseEstate();
        Schema.DefaultCapabilities = tree => FakeSchemaControl.All(tree) with
        {
            SupportsEnforcement = false,
            SupportsVersioning = false,
            SupportsCompliance = false,
            SupportsDeadLetters = false,
            SupportsRemediation = false,
        };
        var cut = RenderAt<SchemaTreePage>("schema/orders");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("Schema features are not installed")));
        Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
    }
}
