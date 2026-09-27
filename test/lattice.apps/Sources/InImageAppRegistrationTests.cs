namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class InImageAppRegistrationTests
{
    private static readonly AppSlug Slug = AppSlug.Parse("demo-app");

    [Test]
    public void Constructor_sets_members_and_defaults_publisher_to_first_party()
    {
        var assembly = typeof(InImageAppRegistrationTests).Assembly;

        var registration = new InImageAppRegistration(Slug, assembly, "app.json");

        Assert.That(registration.Slug, Is.EqualTo(Slug));
        Assert.That(registration.Assembly, Is.SameAs(assembly));
        Assert.That(registration.ManifestResourceName, Is.EqualTo("app.json"));
        Assert.That(registration.Publisher, Is.EqualTo("first-party"));
    }

    [Test]
    public void Constructor_rejects_null_arguments_and_the_default_slug()
    {
        var assembly = typeof(InImageAppRegistrationTests).Assembly;

        Assert.Throws<ArgumentNullException>(() => new InImageAppRegistration(Slug, null!, "app.json"));
        Assert.Throws<ArgumentNullException>(() => new InImageAppRegistration(Slug, assembly, null!));
        Assert.Throws<ArgumentException>(() => new InImageAppRegistration(default, assembly, "app.json"));
    }

    [Test]
    public void Publisher_can_be_overridden_but_not_nulled()
    {
        var registration = new InImageAppRegistration(Slug, typeof(InImageAppRegistrationTests).Assembly, "app.json");

        Assert.That((registration with { Publisher = "contoso" }).Publisher, Is.EqualTo("contoso"));
        Assert.Throws<ArgumentNullException>(() => _ = registration with { Publisher = null! });
    }

    [Test]
    public void Options_register_appends_in_order_and_chains()
    {
        var assembly = typeof(InImageAppRegistrationTests).Assembly;
        var options = new InImageAppSourceOptions();

        var returned = options.Register(Slug, assembly, "a.json").Register(AppSlug.Parse("other-app"), assembly, "b.json");

        Assert.That(returned, Is.SameAs(options));
        Assert.That(options.Registrations.Select(r => r.ManifestResourceName), Is.EqualTo(new[] { "a.json", "b.json" }));
    }

    [Test]
    public void Options_register_validates_arguments()
    {
        var options = new InImageAppSourceOptions();

        Assert.Throws<ArgumentNullException>(() => options.Register(Slug, null!, "a.json"));
        Assert.That(options.Registrations, Is.Empty);
    }
}
