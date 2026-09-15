namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextCoveragePage"/>, the pure paging arithmetic
/// and byte codec underneath the per-page coverage digest (issue #2486). They are
/// deliberately free of a cluster: the whole point of isolating this type is that
/// the part of the digest most likely to be silently wrong - a partial or
/// mis-versioned payload decoding to plausible-but-false coverage - can be proven
/// without one.
/// </summary>
[TestFixture]
public sealed class RepoContextCoveragePageTests
{
    [Test]
    public void PageCount_is_the_leading_byte_of_the_source_id()
        => Assert.That(
            RepoContextCoveragePage.PageCount,
            Is.EqualTo(256),
            "Pages are the leading byte of a SHA-256-derived source id, so there are "
            + "exactly 256 of them and they are evenly occupied by construction.");

    [Test]
    public void PageOf_maps_a_source_id_to_its_leading_byte()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextCoveragePage.PageOf("0000000000000000"), Is.EqualTo(0));
            Assert.That(RepoContextCoveragePage.PageOf("ff00000000000000"), Is.EqualTo(255));
            Assert.That(RepoContextCoveragePage.PageOf("7fffffffffffffff"), Is.EqualTo(0x7f));
        });

    [Test]
    public void PageOf_rejects_anything_that_is_not_a_source_id()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextCoveragePage.PageOf(null), Is.Null);
            Assert.That(RepoContextCoveragePage.PageOf(string.Empty), Is.Null);
            Assert.That(
                RepoContextCoveragePage.PageOf("nil-0000000000000000"),
                Is.Null,
                "A contentless marker is not a source id; the caller strips the prefix first.");
            Assert.That(
                RepoContextCoveragePage.PageOf("memkey-abc"),
                Is.Null,
                "Memory-key markers share the membership tree but are not sources.");
            Assert.That(RepoContextCoveragePage.PageOf("0x00000000000000"), Is.Null);
            Assert.That(RepoContextCoveragePage.PageOf("FF00000000000000"), Is.Null, "Upper case is not emitted.");
            Assert.That(RepoContextCoveragePage.PageOf(" 0000000000000000"), Is.Null);
            Assert.That(RepoContextCoveragePage.PageOf("000000000000000"), Is.Null, "Fifteen characters.");
            Assert.That(RepoContextCoveragePage.PageOf("00000000000000000"), Is.Null, "Seventeen characters.");
        });

    [Test]
    public void TryParse_round_trips_through_Format()
    {
        foreach (var value in new ulong[] { 0, 1, 0x0f, ulong.MaxValue, 0xdeadbeefcafef00d })
        {
            var formatted = RepoContextCoveragePage.Format(value);
            Assert.That(formatted, Has.Length.EqualTo(16));
            Assert.That(RepoContextCoveragePage.TryParse(formatted, out var parsed), Is.True);
            Assert.That(parsed, Is.EqualTo(value));
        }
    }

    [Test]
    public void Encode_and_TryDecode_round_trip_both_sets()
    {
        var embedded = new HashSet<ulong> { 1, 2, ulong.MaxValue };
        var contentless = new HashSet<ulong> { 7 };

        var payload = RepoContextCoveragePage.Encode(embedded, contentless);

        Assert.That(RepoContextCoveragePage.TryDecode(payload, out var decodedEmbedded, out var decodedContentless), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(decodedEmbedded, Is.EquivalentTo(embedded));
            Assert.That(
                decodedContentless,
                Is.EquivalentTo(contentless),
                "The embedded/contentless distinction must survive the round trip: the "
                + "ingestor drives contentless mark/unmark from it.");
        });
    }

    [Test]
    public void Encode_is_deterministic_regardless_of_enumeration_order()
    {
        var forward = RepoContextCoveragePage.Encode(new ulong[] { 1, 2, 3 }, new ulong[] { 9, 8 });
        var reverse = RepoContextCoveragePage.Encode(new ulong[] { 3, 2, 1 }, new ulong[] { 8, 9 });

        Assert.That(
            forward,
            Is.EqualTo(reverse),
            "A page is rewritten on every covered source, so a payload that varied with "
            + "set enumeration order would write a new value on every no-op and defeat the "
            + "store's no-op-write suppression.");
    }

    [Test]
    public void TryDecode_treats_a_malformed_payload_as_no_coverage()
        => Assert.Multiple(() =>
        {
            foreach (var payload in new byte[]?[]
            {
                null,
                [],
                [99, 0, 0, 0, 0, 0, 0, 0, 0],
                [1, 0, 0, 0],
                [1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3],
            })
            {
                Assert.That(
                    RepoContextCoveragePage.TryDecode(payload, out var embedded, out var contentless),
                    Is.False,
                    "A payload that cannot be trusted must not be partially believed.");
                Assert.That(embedded, Is.Empty);
                Assert.That(
                    contentless,
                    Is.Empty,
                    "Decoding to empty under-reports coverage, which costs a redundant "
                    + "idempotent embed. Decoding to anything else could mask a real gap.");
            }
        });

    [Test]
    public void An_unknown_schema_version_decodes_to_no_coverage()
    {
        var payload = RepoContextCoveragePage.Encode(new ulong[] { 1 }, []);
        payload[0] = (byte)(RepoContextCoveragePage.SchemaVersion + 1);

        Assert.That(RepoContextCoveragePage.TryDecode(payload, out var embedded, out var contentless), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.Empty);
            Assert.That(contentless, Is.Empty);
        });
    }
}
