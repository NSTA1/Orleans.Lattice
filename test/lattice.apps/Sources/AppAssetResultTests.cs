using System.Text;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppAssetResultTests
{
    private static readonly byte[] Content = Encoding.UTF8.GetBytes("console.log('hi');");

    [Test]
    public void Verify_matching_digest_opens_with_the_bytes_and_media_type()
    {
        var result = AppAssetResult.Verify("js/app.js", Content, "text/javascript", SourceTestManifests.Sha256(Content));

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.Opened));
        Assert.That(result.IsOpened, Is.True);
        Assert.That(result.Path, Is.EqualTo("js/app.js"));
        Assert.That(result.Content.ToArray(), Is.EqualTo(Content));
        Assert.That(result.MediaType, Is.EqualTo("text/javascript"));
        Assert.That(result.ActualSha256, Is.Null);
        Assert.That(result.Errors, Is.Empty);
    }

    [Test]
    public void Verify_empty_content_opens_when_the_digest_is_of_empty_input()
    {
        var result = AppAssetResult.Verify("empty.json", ReadOnlyMemory<byte>.Empty, "application/json", SourceTestManifests.Sha256([]));

        Assert.That(result.IsOpened, Is.True);
        Assert.That(result.Content.Length, Is.Zero);
    }

    [Test]
    public void Verify_mismatched_digest_returns_no_bytes_and_reports_the_actual_digest()
    {
        var other = SourceTestManifests.Sha256(Encoding.UTF8.GetBytes("tampered"));

        var result = AppAssetResult.Verify("js/app.js", Content, "text/javascript", other);

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.DigestMismatch));
        Assert.That(result.IsOpened, Is.False);
        Assert.That(result.Content.IsEmpty, Is.True);
        Assert.That(result.MediaType, Is.Null);
        Assert.That(result.ActualSha256, Is.EqualTo(SourceTestManifests.Sha256(Content)));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("digest-mismatch"));
    }

    [Test]
    public void Verify_malformed_expected_digest_is_a_mismatch_without_an_actual_digest()
    {
        var upper = SourceTestManifests.Sha256(Content).ToUpperInvariant();

        foreach (var expected in new[] { upper, "", "abc", SourceTestManifests.Sha256(Content) + "0", new string('g', 64) })
        {
            var result = AppAssetResult.Verify("js/app.js", Content, "text/javascript", expected);

            Assert.That(result.Status, Is.EqualTo(AppAssetStatus.DigestMismatch), expected);
            Assert.That(result.ActualSha256, Is.Null);
            Assert.That(result.Errors.Single().Code, Is.EqualTo("digest-format"));
        }
    }

    [Test]
    public void Verify_rejects_null_arguments_and_an_empty_media_type()
    {
        var digest = SourceTestManifests.Sha256(Content);

        Assert.Throws<ArgumentNullException>(() => AppAssetResult.Verify(null!, Content, "text/css", digest));
        Assert.Throws<ArgumentNullException>(() => AppAssetResult.Verify("a.css", Content, null!, digest));
        Assert.Throws<ArgumentNullException>(() => AppAssetResult.Verify("a.css", Content, "text/css", null!));
        Assert.Throws<ArgumentException>(() => AppAssetResult.Verify("a.css", Content, "", digest));
    }

    [Test]
    public void NotFound_carries_no_bytes()
    {
        var result = AppAssetResult.NotFound("missing.css");

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.NotFound));
        Assert.That(result.Path, Is.EqualTo("missing.css"));
        Assert.That(result.Content.IsEmpty, Is.True);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("not-found"));
        Assert.Throws<ArgumentNullException>(() => AppAssetResult.NotFound(null!));
    }

    [Test]
    public void NotAvailable_carries_the_reason()
    {
        var result = AppAssetResult.NotAvailable("a.css", "Not acquired.");

        Assert.That(result.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
        Assert.That(result.Errors.Single(), Is.EqualTo(new AppManifestError("not-available", "$.path", "Not acquired.")));
        Assert.Throws<ArgumentNullException>(() => AppAssetResult.NotAvailable(null!, "r"));
        Assert.Throws<ArgumentNullException>(() => AppAssetResult.NotAvailable("a.css", null!));
        Assert.Throws<ArgumentException>(() => AppAssetResult.NotAvailable("a.css", ""));
    }

    [Test]
    public void IsSha256Hex_accepts_only_lower_case_sixty_four_character_hex()
    {
        var digest = SourceTestManifests.Sha256(Content);

        Assert.That(AppAssetResult.IsSha256Hex(digest), Is.True);
        Assert.That(AppAssetResult.IsSha256Hex(digest.ToUpperInvariant()), Is.False);
        Assert.That(AppAssetResult.IsSha256Hex(digest[..63]), Is.False);
        Assert.That(AppAssetResult.IsSha256Hex(null), Is.False);
        Assert.That(AppAssetResult.Sha256HexLength, Is.EqualTo(64));
    }

    [Test]
    public void AppAssetStatus_values_are_stable()
    {
        Assert.That((int)AppAssetStatus.Opened, Is.EqualTo(0));
        Assert.That((int)AppAssetStatus.NotFound, Is.EqualTo(1));
        Assert.That((int)AppAssetStatus.NotAvailable, Is.EqualTo(2));
        Assert.That((int)AppAssetStatus.DigestMismatch, Is.EqualTo(3));
    }
}
