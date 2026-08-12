using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="FileDigest"/>: the stable, self-describing content digest
/// that makes an unchanged file detectable across bootstrap runs. The modern
/// default is a tagged XxHash128 fingerprint; comparison stays correct against a
/// legacy SHA-256 store so the algorithm can migrate lazily without a forced
/// re-hash.
/// </summary>
[TestFixture]
public sealed class FileDigestTests
{
    private const string EmptyXxHash128 = "xx128:99aa06d3014798d86001c324468d497f";
    private const string AbcXxHash128 = "xx128:06b05ab6733a618578af5f94892f3950";

    [Test]
    public void Compute_returns_the_known_tagged_xxhash128_of_the_empty_input()
        => Assert.That(
            FileDigest.Compute(ReadOnlySpan<byte>.Empty),
            Is.EqualTo(EmptyXxHash128));

    [Test]
    public void Compute_returns_the_known_tagged_xxhash128_of_abc()
        => Assert.That(
            FileDigest.Compute(Encoding.ASCII.GetBytes("abc")),
            Is.EqualTo(AbcXxHash128));

    [Test]
    public void Compute_is_deterministic_for_identical_content()
    {
        var content = Encoding.UTF8.GetBytes("the quick brown fox");
        Assert.That(FileDigest.Compute(content), Is.EqualTo(FileDigest.Compute(content)));
    }

    [Test]
    public void Compute_differs_for_different_content()
    {
        var one = FileDigest.Compute(Encoding.UTF8.GetBytes("alpha"));
        var two = FileDigest.Compute(Encoding.UTF8.GetBytes("beta"));
        Assert.That(one, Is.Not.EqualTo(two));
    }

    [Test]
    public void Compute_returns_a_tagged_lower_case_hex_string()
    {
        var digest = FileDigest.Compute(Encoding.UTF8.GetBytes("payload"));

        Assert.Multiple(() =>
        {
            Assert.That(digest, Does.StartWith("xx128:"));
            Assert.That(digest, Is.EqualTo(digest.ToLowerInvariant()));
            Assert.That(digest, Does.Match("^xx128:[0-9a-f]{32}$"));
        });
    }

    [Test]
    public void Matches_is_true_for_unchanged_content_under_the_modern_digest()
    {
        var content = Encoding.UTF8.GetBytes("modern content");
        var digest = FileDigest.Compute(content);

        Assert.That(FileDigest.Matches(digest, content), Is.True);
    }

    [Test]
    public void Matches_is_false_for_changed_content_under_the_modern_digest()
    {
        var digest = FileDigest.Compute(Encoding.UTF8.GetBytes("original"));

        Assert.That(FileDigest.Matches(digest, Encoding.UTF8.GetBytes("edited")), Is.False);
    }

    [Test]
    public void Matches_recomputes_a_bare_hex_legacy_digest_as_sha256()
    {
        var content = Encoding.UTF8.GetBytes("legacy payload");
        var legacy = Convert.ToHexStringLower(SHA256.HashData(content));

        Assert.Multiple(() =>
        {
            Assert.That(legacy, Does.Match("^[0-9a-f]{64}$"));
            Assert.That(FileDigest.Matches(legacy, content), Is.True);
            Assert.That(FileDigest.Matches(legacy, Encoding.UTF8.GetBytes("other")), Is.False);
        });
    }

    [Test]
    public void Matches_recomputes_an_explicitly_prefixed_legacy_digest_as_sha256()
    {
        var content = Encoding.UTF8.GetBytes("prefixed legacy");
        var legacy = "sha256:" + Convert.ToHexStringLower(SHA256.HashData(content));

        Assert.Multiple(() =>
        {
            Assert.That(FileDigest.Matches(legacy, content), Is.True);
            Assert.That(FileDigest.Matches(legacy, Encoding.UTF8.GetBytes("nope")), Is.False);
        });
    }

    [Test]
    public void Matches_throws_when_the_stored_digest_is_null()
        => Assert.Throws<ArgumentNullException>(
            () => FileDigest.Matches(null!, ReadOnlySpan<byte>.Empty));
}
