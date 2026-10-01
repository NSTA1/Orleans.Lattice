using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Tests for <see cref="AppManifestDigest"/>, the review pin an install is held to (issue #4021).</summary>
[TestFixture]
public sealed class AppManifestDigestTests
{
    private static string Json
    {
        get
        {
            using var stream = typeof(AppManifestDigestTests).Assembly.GetManifestResourceStream("test.tiny-app.json")!;
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }
    }

    private static AppManifest Parsed() => AppManifestParser.Parse(Json).Manifest!;

    private static AppManifest WithBridge(params string[] operations) =>
        UiTestManifests.WithUi(Parsed(), [.. operations.Select(op => UiTestManifests.Bridge(op))]);

    [Test]
    public void Compute_is_lower_case_hex_sha256_and_deterministic_across_parses()
    {
        var first = AppManifestDigest.Compute(Parsed());
        var second = AppManifestDigest.Compute(Parsed());

        Assert.That(first, Does.Match("^[0-9a-f]{64}$"));
        Assert.That(second, Is.EqualTo(first));
        Assert.That(AppManifestDigest.IsWellFormed(first), Is.True);
    }

    [Test]
    public void Compute_changes_when_a_bridge_operation_is_added()
    {
        Assert.That(
            AppManifestDigest.Compute(WithBridge(AppUiBridgeOperations.DataRead, AppUiBridgeOperations.ContextUser)),
            Is.Not.EqualTo(AppManifestDigest.Compute(WithBridge(AppUiBridgeOperations.DataRead))));
    }

    [Test]
    public void Compute_changes_when_a_role_widens()
    {
        var manifest = Parsed();
        var widened = manifest with
        {
            Roles = [.. manifest.Roles.Select((role, i) => i == 0 ? role with { Operations = role.Operations | LatticeOperation.Delete } : role)],
        };

        Assert.That(AppManifestDigest.Compute(widened), Is.Not.EqualTo(AppManifestDigest.Compute(manifest)));
    }

    [Test]
    public void Compute_covers_the_supplied_provenance()
    {
        var manifest = Parsed();
        var vouched = new AppProvenance { Source = "alpha", Publisher = "contoso" };

        Assert.Multiple(() =>
        {
            Assert.That(
                AppManifestDigest.Compute(manifest, vouched),
                Is.Not.EqualTo(AppManifestDigest.Compute(manifest, vouched with { Publisher = "fabrikam" })));
            Assert.That(
                AppManifestDigest.Compute(manifest, manifest.Identity.Provenance),
                Is.EqualTo(AppManifestDigest.Compute(manifest)));
        });
    }

    [Test]
    public void Compute_of_a_manifest_the_converters_refuse_to_write_is_null()
    {
        var manifest = Parsed();
        var invalid = manifest with
        {
            Roles = [.. manifest.Roles.Select((role, i) => i == 0 ? role with { Operations = LatticeOperation.None } : role)],
        };

        Assert.That(AppManifestDigest.Compute(invalid), Is.Null);
    }

    [Test]
    public void Compute_rejects_a_null_manifest() =>
        Assert.Throws<ArgumentNullException>(() => AppManifestDigest.Compute(null!));

    [TestCase(null, false)]
    [TestCase("", false)]
    [TestCase("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", true)]
    [TestCase("0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef", false)]
    [TestCase("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde", false)]
    [TestCase("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg", false)]
    public void IsWellFormed_accepts_only_64_lower_case_hex_characters(string? value, bool expected) =>
        Assert.That(AppManifestDigest.IsWellFormed(value), Is.EqualTo(expected));
}
