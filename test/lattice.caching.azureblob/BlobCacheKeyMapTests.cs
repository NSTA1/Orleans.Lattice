using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Unit tests for <see cref="BlobCacheKeyMap"/>: the key-to-blob-name mapping must
/// be deterministic, storage-legal, collision-resistant, and honour the key
/// prefix, including for keys longer than the on-stack hashing threshold.
/// </summary>
[TestFixture]
public sealed class BlobCacheKeyMapTests
{
    [Test]
    public void ToBlobName_is_a_64_char_lowercase_hex_digest()
    {
        var name = BlobCacheKeyMap.ToBlobName(string.Empty, "some-key");

        Assert.That(name, Has.Length.EqualTo(64));
        Assert.That(name, Does.Match("^[0-9a-f]{64}$"));
    }

    [Test]
    public void ToBlobName_is_deterministic()
    {
        var a = BlobCacheKeyMap.ToBlobName(string.Empty, "stable-key");
        var b = BlobCacheKeyMap.ToBlobName(string.Empty, "stable-key");

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void ToBlobName_matches_a_plain_sha256_of_the_utf8_key()
    {
        const string key = "MSAL.token.cache/user-42";
        var expected = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key)));

        Assert.That(BlobCacheKeyMap.ToBlobName(string.Empty, key), Is.EqualTo(expected));
    }

    [Test]
    public void ToBlobName_distinct_keys_produce_distinct_names()
    {
        var a = BlobCacheKeyMap.ToBlobName(string.Empty, "key-a");
        var b = BlobCacheKeyMap.ToBlobName(string.Empty, "key-b");

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void ToBlobName_prepends_the_prefix_verbatim()
    {
        const string key = "abc";
        var bare = BlobCacheKeyMap.ToBlobName(string.Empty, key);
        var prefixed = BlobCacheKeyMap.ToBlobName("tokens/", key);

        Assert.That(prefixed, Is.EqualTo("tokens/" + bare));
    }

    [Test]
    public void ToBlobName_hashes_only_the_key_not_the_prefix()
    {
        // Two different prefixes over the same key differ only by the prefix, so
        // the 64-char tail is identical.
        var withA = BlobCacheKeyMap.ToBlobName("a/", "shared");
        var withB = BlobCacheKeyMap.ToBlobName("b/", "shared");

        Assert.That(withA[2..], Is.EqualTo(withB[2..]));
    }

    [Test]
    public void ToBlobName_handles_keys_longer_than_the_stack_threshold()
    {
        // 4096 chars > the 512-byte on-stack hashing threshold, exercising the
        // ArrayPool rental path. Result must still match a plain SHA-256.
        var key = new string('x', 4096);
        var expected = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key)));

        Assert.That(BlobCacheKeyMap.ToBlobName(string.Empty, key), Is.EqualTo(expected));
    }

    [Test]
    public void ToBlobName_handles_multibyte_unicode_keys()
    {
        const string key = "clave-\u00f1-\u4e2d\u6587";
        var expected = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key)));

        Assert.That(BlobCacheKeyMap.ToBlobName(string.Empty, key), Is.EqualTo(expected));
    }

    [Test]
    public void ToBlobName_throws_on_null_key()
    {
        Assert.Throws<ArgumentNullException>(() => BlobCacheKeyMap.ToBlobName(string.Empty, null!));
    }

    [Test]
    public void ToBlobName_throws_on_null_prefix()
    {
        Assert.Throws<ArgumentNullException>(() => BlobCacheKeyMap.ToBlobName(null!, "key"));
    }
}
