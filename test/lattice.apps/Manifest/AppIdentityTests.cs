namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppIdentityTests
{
    [TestCase(null)]
    [TestCase("")]
    [TestCase("a")]
    [TestCase("Ab")]
    [TestCase("1a")]
    [TestCase("-a")]
    [TestCase("ab_cd")]
    [TestCase("ab/cd")]
    [TestCase("ab.cd")]
    [TestCase("ab\n")]
    [TestCase("ab ")]
    [TestCase("\u00e9a")]
    public void TryParse_invalid_slug_returns_false(string? value)
    {
        Assert.That(AppSlug.TryParse(value, out var slug), Is.False);
        Assert.That(slug, Is.EqualTo(default(AppSlug)));
        Assert.That(slug.ToString(), Is.Empty);
    }

    [TestCase("ab")]
    [TestCase("a-")]
    [TestCase("a0")]
    [TestCase("repo-context")]
    public void Parse_valid_slug_preserves_ordinal_identity(string value)
    {
        var slug = AppSlug.Parse(value);
        Assert.That(slug.Value, Is.EqualTo(value));
        Assert.That(slug.ToString(), Is.EqualTo(value));
        Assert.That(AppSlug.TryParse(value, out var other), Is.True);
        Assert.That(other, Is.EqualTo(slug));
    }

    [TestCase(1, false)]
    [TestCase(2, true)]
    [TestCase(31, true)]
    [TestCase(32, false)]
    public void TryParse_slug_length_enforces_exact_boundaries(int length, bool valid) =>
        Assert.That(AppSlug.TryParse(new string('a', length), out _), Is.EqualTo(valid));

    [Test]
    public void Parse_bad_programmer_input_throws()
    {
        Assert.Throws<ArgumentNullException>(() => AppSlug.Parse(null!));
        Assert.Throws<FormatException>(() => AppSlug.Parse("bad_slug"));
        Assert.Throws<ArgumentNullException>(() => AppVersion.Parse(null!));
        Assert.Throws<FormatException>(() => AppVersion.Parse("v1"));
    }

    [TestCase("0.0.0")]
    [TestCase("1.2.3")]
    [TestCase("1.2.3-alpha.1+build.01")]
    [TestCase("99999999999999999999999.2.3")]
    public void Parse_valid_semver_preserves_exact_text(string value)
    {
        Assert.That(AppVersion.TryParse(value, out var version), Is.True);
        Assert.That(version.Value, Is.EqualTo(value));
        Assert.That(version.ToString(), Is.EqualTo(value));
        Assert.That(AppVersion.Parse(value), Is.EqualTo(version));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("1")]
    [TestCase("1.2")]
    [TestCase("01.2.3")]
    [TestCase("1.2.3.4")]
    [TestCase("1.2.3-01")]
    [TestCase("1.2.3-")]
    [TestCase("1.2.3+")]
    [TestCase("1.2.3\n")]
    [TestCase("1.2.3-alpha..1")]
    public void TryParse_invalid_semver_returns_false(string? value)
    {
        Assert.That(AppVersion.TryParse(value, out var version), Is.False);
        Assert.That(version.ToString(), Is.Empty);
    }

    [Test]
    public void Provenance_defaults_are_descriptive_and_overridable()
    {
        var provenance = new AppProvenance();
        Assert.That(provenance.Source, Is.EqualTo("in-image"));
        Assert.That(provenance.Publisher, Is.EqualTo("first-party"));
        Assert.That(provenance.Reference, Is.Null);
        var external = provenance with { Source = "catalog", Publisher = "vendor", Reference = "artifact-id" };
        Assert.That(external.Source, Is.EqualTo("catalog"));
        Assert.That(external.Publisher, Is.EqualTo("vendor"));
        Assert.That(external.Reference, Is.EqualTo("artifact-id"));
    }
}
