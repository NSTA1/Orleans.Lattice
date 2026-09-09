using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpSubjectId"/>, the seam that stops a
/// caller's bearer token being used as its own identifier.
/// </summary>
/// <remarks>
/// The discovery surface previously reported <c>credential.Token</c> as the
/// subject id whenever no principal id had been resolved. That value reached the
/// server's log lines and the ungated <c>lattice_capabilities</c> response, so a
/// live bearer secret came to rest in two places that outlive the request. These
/// tests pin both halves of the replacement: it never reveals the token, and it
/// is still a usable, stable identifier.
/// </remarks>
[TestFixture]
public class LatticeApiMcpSubjectIdTests
{
    [Test]
    public void Resolve_prefers_the_principal_id()
    {
        var subject = LatticeApiMcpSubjectId.Resolve(
            new LatticeCredential("opaque-token", principalId: "alice"));

        Assert.That(subject, Is.EqualTo("alice"));
    }

    [Test]
    public void Resolve_never_returns_the_raw_token()
    {
        var subject = LatticeApiMcpSubjectId.Resolve(new LatticeCredential("super-secret-token"));

        Assert.Multiple(() =>
        {
            Assert.That(subject, Is.Not.Null);
            Assert.That(subject, Is.Not.EqualTo("super-secret-token"));
            Assert.That(subject, Does.Not.Contain("super-secret-token"));
        });
    }

    [Test]
    public void Resolve_marks_a_fingerprinted_subject_with_the_token_prefix()
    {
        var subject = LatticeApiMcpSubjectId.Resolve(new LatticeCredential("super-secret-token"));

        Assert.That(subject, Does.StartWith(LatticeApiMcpSubjectId.FingerprintPrefix));
    }

    [Test]
    public void Resolve_returns_null_when_the_credential_carries_neither_field()
    {
        var subject = LatticeApiMcpSubjectId.Resolve(new LatticeCredential(string.Empty));

        Assert.That(subject, Is.Null);
    }

    [Test]
    public void Resolve_is_stable_for_the_same_token()
    {
        var first = LatticeApiMcpSubjectId.Resolve(new LatticeCredential("super-secret-token"));
        var second = LatticeApiMcpSubjectId.Resolve(new LatticeCredential("super-secret-token"));

        Assert.That(second, Is.EqualTo(first), "a subject id that is not stable cannot correlate a caller");
    }

    [Test]
    public void Resolve_distinguishes_different_tokens()
    {
        var first = LatticeApiMcpSubjectId.Resolve(new LatticeCredential("token-one"));
        var second = LatticeApiMcpSubjectId.Resolve(new LatticeCredential("token-two"));

        Assert.That(second, Is.Not.EqualTo(first));
    }

    /// <summary>
    /// The fingerprint is one-way, which is the property that makes it safe to
    /// log: it is exactly the leading bytes of the SHA-256 of the token, so it
    /// cannot be inverted to recover the credential.
    /// </summary>
    [Test]
    public void The_fingerprint_is_a_truncated_sha256_of_the_token()
    {
        const string token = "super-secret-token";
        var expected = LatticeApiMcpSubjectId.FingerprintPrefix
            + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(token)))
                [..LatticeApiMcpSubjectId.FingerprintHexLength];

        Assert.That(LatticeApiMcpSubjectId.Resolve(new LatticeCredential(token)), Is.EqualTo(expected));
    }

    [Test]
    public void An_empty_principal_id_falls_through_to_the_fingerprint()
    {
        // The bridge yields an empty string rather than null on some paths, and
        // that must not be treated as a usable identifier.
        var subject = LatticeApiMcpSubjectId.Resolve(
            new LatticeCredential("super-secret-token", principalId: string.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(subject, Does.StartWith(LatticeApiMcpSubjectId.FingerprintPrefix));
            Assert.That(subject, Does.Not.Contain("super-secret-token"));
        });
    }
}
