using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="FileDigest"/>: the stable, lower-case hex SHA-256 content
/// digest that makes an unchanged file detectable across bootstrap runs.
/// </summary>
[TestFixture]
public sealed class FileDigestTests
{
    [Test]
    public void Compute_returns_the_known_sha256_of_the_empty_input()
        => Assert.That(
            FileDigest.Compute(ReadOnlySpan<byte>.Empty),
            Is.EqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));

    [Test]
    public void Compute_returns_the_known_sha256_of_abc()
        => Assert.That(
            FileDigest.Compute(Encoding.ASCII.GetBytes("abc")),
            Is.EqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));

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
    public void Compute_returns_a_64_character_lower_case_hex_string()
    {
        var digest = FileDigest.Compute(Encoding.UTF8.GetBytes("payload"));

        Assert.Multiple(() =>
        {
            Assert.That(digest, Has.Length.EqualTo(64));
            Assert.That(digest, Is.EqualTo(digest.ToLowerInvariant()));
            Assert.That(digest, Does.Match("^[0-9a-f]{64}$"));
        });
    }
}
