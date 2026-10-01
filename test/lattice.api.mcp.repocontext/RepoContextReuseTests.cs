using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextReuse"/>, the deterministic opaque-token helper
/// behind the <c>repocontext_context</c> tool's reuse economics: content hashes,
/// per-unit receipts, whole-file possession tokens, and the fail-closed parse of a
/// wire <c>known</c> claim.
/// </summary>
[TestFixture]
public sealed class RepoContextReuseTests
{
    [Test]
    public void ContentHash_is_deterministic_and_lowercase_hex()
    {
        var a = RepoContextReuse.ContentHash("the quick brown fox");
        var b = RepoContextReuse.ContentHash("the quick brown fox");

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Has.Length.EqualTo(64), "A SHA-256 hex digest is 64 characters.");
            Assert.That(a, Does.Match("^[0-9a-f]+$"), "The digest is opaque lowercase hex.");
        });
    }

    [Test]
    public void ContentHash_differs_when_the_body_differs()
        => Assert.That(
            RepoContextReuse.ContentHash("alpha"),
            Is.Not.EqualTo(RepoContextReuse.ContentHash("beta")));

    [Test]
    public void ContentHash_rejects_null()
        => Assert.That(() => RepoContextReuse.ContentHash(null!), Throws.ArgumentNullException);

    [Test]
    public void Receipt_is_deterministic_for_identical_inputs()
    {
        var a = RepoContextReuse.Receipt("repo", "src/A.cs", "hash", RepoContextReuse.SpanKind, string.Empty);
        var b = RepoContextReuse.Receipt("repo", "src/A.cs", "hash", RepoContextReuse.SpanKind, string.Empty);

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Does.Match("^[0-9a-f]{64}$"));
    }

    [Test]
    public void Receipt_is_version_bound_to_the_content_hash()
        => Assert.That(
            RepoContextReuse.Receipt("repo", "src/A.cs", "hash1", RepoContextReuse.SpanKind, string.Empty),
            Is.Not.EqualTo(
                RepoContextReuse.Receipt("repo", "src/A.cs", "hash2", RepoContextReuse.SpanKind, string.Empty)),
            "A receipt embeds the version, so a changed file yields a different receipt.");

    [Test]
    public void Receipt_distinguishes_kind_path_repo_and_unit_key()
    {
        var baseline = RepoContextReuse.Receipt("repo", "src/A.cs", "hash", RepoContextReuse.OutlineKind, "Ns.A.M");

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextReuse.Receipt("other", "src/A.cs", "hash", RepoContextReuse.OutlineKind, "Ns.A.M"),
                Is.Not.EqualTo(baseline), "The repo id participates.");
            Assert.That(RepoContextReuse.Receipt("repo", "src/B.cs", "hash", RepoContextReuse.OutlineKind, "Ns.A.M"),
                Is.Not.EqualTo(baseline), "The path participates.");
            Assert.That(RepoContextReuse.Receipt("repo", "src/A.cs", "hash", RepoContextReuse.SpanKind, "Ns.A.M"),
                Is.Not.EqualTo(baseline), "The kind participates.");
            Assert.That(RepoContextReuse.Receipt("repo", "src/A.cs", "hash", RepoContextReuse.OutlineKind, "Ns.A.N"),
                Is.Not.EqualTo(baseline), "The unit key participates.");
        });
    }

    [Test]
    public void Receipt_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextReuse.Receipt(null!, "p", "h", "k", string.Empty), Throws.ArgumentNullException);
            Assert.That(() => RepoContextReuse.Receipt("r", null!, "h", "k", string.Empty), Throws.ArgumentNullException);
            Assert.That(() => RepoContextReuse.Receipt("r", "p", null!, "k", string.Empty), Throws.ArgumentNullException);
            Assert.That(() => RepoContextReuse.Receipt("r", "p", "h", null!, string.Empty), Throws.ArgumentNullException);
            Assert.That(() => RepoContextReuse.Receipt("r", "p", "h", "k", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void PossessionToken_pairs_path_and_hash_with_a_nul_separator()
        => Assert.That(
            RepoContextReuse.PossessionToken("src/A.cs", "hash"),
            Is.EqualTo("src/A.cs\u0000hash"));

    [Test]
    public void PossessionToken_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextReuse.PossessionToken(null!, "h"), Throws.ArgumentNullException);
            Assert.That(() => RepoContextReuse.PossessionToken("p", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void TryParseKnown_splits_on_the_last_at_so_a_path_with_at_parses()
    {
        var ok = RepoContextReuse.TryParseKnown("src/@odd/A.cs@deadbeef", out var path, out var hash);

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That(path, Is.EqualTo("src/@odd/A.cs"));
            Assert.That(hash, Is.EqualTo("deadbeef"));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("no-separator")]
    [TestCase("@hash-only")]
    [TestCase("path-only@")]
    public void TryParseKnown_fails_closed_on_a_malformed_claim(string? claim)
    {
        var ok = RepoContextReuse.TryParseKnown(claim, out var path, out var hash);

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.False);
            Assert.That(path, Is.Empty);
            Assert.That(hash, Is.Empty);
        });
    }

    [Test]
    public void ContentHash_is_the_sha256_of_the_utf8_body_across_the_rent_boundary()
    {
        foreach (var length in new[] { 0, 1, 340, 341, 342, 1023, 1024, 1025, 4096 })
        {
            var body = new string('x', length);
            var expected = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(body)));

            Assert.That(
                RepoContextReuse.ContentHash(body),
                Is.EqualTo(expected),
                $"A body of {length} characters must hash identically.");
        }
    }

    [Test]
    public void ContentHash_is_the_sha256_of_a_multibyte_utf8_body()
    {
        var body = string.Concat(Enumerable.Repeat("caf\u00e9 \u4e2d\u6587 ", 500));
        var expected = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(body)));

        Assert.That(RepoContextReuse.ContentHash(body), Is.EqualTo(expected));
    }

    [Test]
    public void Receipt_is_the_sha256_of_the_nul_joined_parts()
    {
        const string repoId = "lattice";
        const string path = "src/A.cs";
        const string contentHash = "deadbeef";
        const string unitKey = "Acme.Widgets.Gadget.Run";
        var kind = RepoContextReuse.OutlineKind;
        var expected = Convert.ToHexStringLower(SHA256.HashData(
            Encoding.UTF8.GetBytes(string.Join('\u0000', repoId, path, contentHash, kind, unitKey))));

        Assert.That(
            RepoContextReuse.Receipt(repoId, path, contentHash, kind, unitKey),
            Is.EqualTo(expected));
    }

    [Test]
    public void Receipt_is_the_sha256_of_the_nul_joined_parts_above_the_stack_budget()
    {
        const string repoId = "lattice";
        var path = "src/" + new string('p', 2048) + ".cs";
        const string contentHash = "deadbeef";
        var unitKey = new string('u', 512);
        var kind = RepoContextReuse.OutlineKind;
        var expected = Convert.ToHexStringLower(SHA256.HashData(
            Encoding.UTF8.GetBytes(string.Join('\u0000', repoId, path, contentHash, kind, unitKey))));

        Assert.That(
            RepoContextReuse.Receipt(repoId, path, contentHash, kind, unitKey),
            Is.EqualTo(expected));
    }

    [Test]
    public void Receipt_is_the_sha256_of_the_nul_joined_parts_for_multibyte_parts()
    {
        const string repoId = "lattice";
        const string path = "src/caf\u00e9/\u4e2d\u6587.cs";
        const string contentHash = "deadbeef";
        const string unitKey = "Acme.Caf\u00e9.Run";
        var kind = RepoContextReuse.SpanKind;
        var expected = Convert.ToHexStringLower(SHA256.HashData(
            Encoding.UTF8.GetBytes(string.Join('\u0000', repoId, path, contentHash, kind, unitKey))));

        Assert.That(
            RepoContextReuse.Receipt(repoId, path, contentHash, kind, unitKey),
            Is.EqualTo(expected));
    }

    [Test]
    public void PossessionToken_is_the_path_and_hash_joined_by_nul()
        => Assert.That(
            RepoContextReuse.PossessionToken("src/A.cs", "deadbeef"),
            Is.EqualTo("src/A.cs\u0000deadbeef"));}
