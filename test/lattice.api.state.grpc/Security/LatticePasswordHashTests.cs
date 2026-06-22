using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.State.Grpc.Tests.Security;

[TestFixture]
public class LatticePasswordHashTests
{
    // A fixed, documented vector shared with the credential-generation scripts:
    // password "Password1", salt = bytes 0x01..0x10, 210000 iterations.
    private const string KnownPassword = "Password1";
    private const string KnownVector =
        "pbkdf2-sha256$210000$AQIDBAUGBwgJCgsMDQ4PEA==$Qc/KlSS3jQS+Upam+rUnCYWhq5v8/JBbmCDEdGfOX8k=";

    private static byte[] KnownSalt =>
        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10];

    [Test]
    public void Encode_knownVector_producesDocumentedString()
    {
        var encoded = LatticePasswordHash.Encode(KnownPassword, KnownSalt, 210_000);

        Assert.That(encoded, Is.EqualTo(KnownVector));
    }

    [Test]
    public void Verify_knownVector_returnsTrue()
    {
        Assert.That(LatticePasswordHash.Verify(KnownPassword, KnownVector), Is.True);
    }

    [Test]
    public void Hash_thenVerify_roundtripsTrue()
    {
        var encoded = LatticePasswordHash.Hash("Sup3rSecret", 50_000);

        Assert.That(LatticePasswordHash.Verify("Sup3rSecret", encoded), Is.True);
    }

    [Test]
    public void Hash_usesDefaultIterations_whenUnspecified()
    {
        var encoded = LatticePasswordHash.Hash("Sup3rSecret");

        Assert.That(LatticePasswordHash.TryParse(encoded, out var parsed), Is.True);
        Assert.That(parsed.Iterations, Is.EqualTo(LatticePasswordHash.DefaultIterations));
        Assert.That(parsed.Salt, Has.Length.EqualTo(LatticePasswordHash.SaltSizeBytes));
        Assert.That(parsed.DerivedKey, Has.Length.EqualTo(LatticePasswordHash.DerivedKeySizeBytes));
    }

    [Test]
    public void Hash_usesFreshSalt_perCall()
    {
        var first = LatticePasswordHash.Hash("Sup3rSecret");
        var second = LatticePasswordHash.Hash("Sup3rSecret");

        Assert.That(first, Is.Not.EqualTo(second));
    }

    [Test]
    public void Verify_wrongPassword_returnsFalse()
    {
        Assert.That(LatticePasswordHash.Verify("WrongPassword1", KnownVector), Is.False);
    }

    [Test]
    public void Verify_tamperedDerivedKey_returnsFalse()
    {
        // Flip the first base64 character of the derived key segment.
        var parts = KnownVector.Split('$');
        var key = parts[3];
        var flipped = (key[0] == 'A' ? 'B' : 'A') + key[1..];
        var tampered = string.Join('$', parts[0], parts[1], parts[2], flipped);

        Assert.That(LatticePasswordHash.Verify(KnownPassword, tampered), Is.False);
    }

    [Test]
    public void Verify_nullPassword_returnsFalse()
    {
        Assert.That(LatticePasswordHash.Verify(null, KnownVector), Is.False);
    }

    [Test]
    public void Verify_nullHash_returnsFalse()
    {
        Assert.That(LatticePasswordHash.Verify(KnownPassword, null), Is.False);
    }

    [TestCase("")]
    [TestCase("not-a-hash")]
    [TestCase("pbkdf2-sha256$210000$onlythreeparts")]
    [TestCase("scrypt$210000$AQID$AQID")]
    [TestCase("pbkdf2-sha256$notanumber$AQID$AQID")]
    [TestCase("pbkdf2-sha256$0$AQID$AQID")]
    [TestCase("pbkdf2-sha256$210000$!!notbase64!!$AQID")]
    public void TryParse_malformed_returnsFalse(string encoded)
    {
        Assert.That(LatticePasswordHash.TryParse(encoded, out _), Is.False);
    }

    [Test]
    public void TryParse_validHash_returnsComponents()
    {
        Assert.That(LatticePasswordHash.TryParse(KnownVector, out var parsed), Is.True);
        Assert.That(parsed.Iterations, Is.EqualTo(210_000));
        Assert.That(parsed.Salt, Is.EqualTo(KnownSalt));
        Assert.That(parsed.DerivedKey, Has.Length.EqualTo(32));
    }

    [Test]
    public void Hash_nullPassword_throws()
    {
        Assert.That(() => LatticePasswordHash.Hash(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Hash_nonPositiveIterations_throws()
    {
        Assert.That(() => LatticePasswordHash.Hash("Sup3rSecret", 0), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Encode_emptySalt_throws()
    {
        Assert.That(() => LatticePasswordHash.Encode("Sup3rSecret", ReadOnlySpan<byte>.Empty, 210_000),
            Throws.ArgumentException);
    }
}
