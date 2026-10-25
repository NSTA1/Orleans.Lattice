using System.Security.Cryptography;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

[TestFixture]
public sealed class FileDigestStreamTests
{
    [TestCase(0)]
    [TestCase(3)]
    [TestCase(7999)]
    [TestCase(8000)]
    [TestCase(65537)]
    [TestCase(200000)]
    public void Read_short_reads_match_the_byte_digest_and_exact_length(int length)
    {
        var bytes = new byte[length];
        new Random(42).NextBytes(bytes);
        using var stream = new ShortReadStream(bytes);

        var result = FileDigest.Read(stream, false, CancellationToken.None);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result!.Value.Digest, Is.EqualTo(FileDigest.Compute(bytes)));
            Assert.That(result.Value.SizeBytes, Is.EqualTo(length));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Read_preserves_unchanged_legacy_digests_and_migrates_changed_content(bool prefixed)
    {
        var bytes = "legacy content"u8.ToArray();
        var legacy = (prefixed ? "sha256:" : "") + Convert.ToHexStringLower(SHA256.HashData(bytes));
        using var same = new MemoryStream(bytes);
        using var changed = new MemoryStream("edited"u8.ToArray());

        Assert.Multiple(() =>
        {
            Assert.That(FileDigest.Read(same, false, CancellationToken.None, legacy)!.Value.Digest,
                Is.EqualTo(legacy));
            Assert.That(FileDigest.Read(changed, false, CancellationToken.None, legacy)!.Value.Digest,
                Is.EqualTo(FileDigest.Compute("edited"u8)));
        });
    }

    [TestCase(7999, true)]
    [TestCase(8000, false)]
    public void Read_binary_sniff_honours_the_exact_window_with_short_reads(int nulOffset, bool rejected)
    {
        var bytes = Enumerable.Repeat((byte)'x', 200000).ToArray();
        bytes[nulOffset] = 0;
        using var stream = new ShortReadStream(bytes);

        var result = FileDigest.Read(stream, true, CancellationToken.None);

        Assert.That(result.HasValue, Is.EqualTo(!rejected));
        if (rejected)
        {
            Assert.That(stream.Position, Is.LessThanOrEqualTo(65536),
                "A rejected binary must not be drained into memory before classification.");
        }
    }

    [Test]
    public void Read_honours_cancellation()
    {
        using var stream = new MemoryStream("payload"u8.ToArray());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.That(() => FileDigest.Read(stream, false, cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    private sealed class ShortReadStream(byte[] bytes) : MemoryStream(bytes)
    {
        public override int Read(Span<byte> buffer)
        {
            Assert.That(buffer.Length, Is.LessThanOrEqualTo(65536),
                "Scratch space must not grow with the untrusted stream length.");
            return base.Read(buffer[..Math.Min(buffer.Length, 97)]);
        }
    }
}
