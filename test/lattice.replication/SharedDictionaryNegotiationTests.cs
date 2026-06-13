namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of the pure <see cref="SharedDictionaryNegotiation.Negotiate"/>
/// branch logic: no configured dictionary, unknown peer capability, a matching
/// advertisement, a non-matching advertisement, and an empty advertisement.
/// </summary>
[TestFixture]
public class SharedDictionaryNegotiationTests
{
    [Test]
    public void Negotiate_returns_no_dictionary_matched_when_nothing_configured()
    {
        var result = SharedDictionaryNegotiation.Negotiate(0u, new uint[] { 1u, 2u });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.True);
            Assert.That(result.PeerCapabilityKnown, Is.False);
            Assert.That(result.FellBack, Is.False);
        });
    }

    [Test]
    public void Negotiate_falls_back_unknown_when_peer_capability_is_null()
    {
        var result = SharedDictionaryNegotiation.Negotiate(7u, peerAdvertisedIds: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.False);
            Assert.That(result.FellBack, Is.True);
        });
    }

    [Test]
    public void Negotiate_matches_when_peer_advertises_configured_id()
    {
        var result = SharedDictionaryNegotiation.Negotiate(7u, new uint[] { 3u, 7u, 9u });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(7u));
            Assert.That(result.Matched, Is.True);
            Assert.That(result.PeerCapabilityKnown, Is.True);
            Assert.That(result.FellBack, Is.False);
        });
    }

    [Test]
    public void Negotiate_falls_back_known_when_peer_advertises_other_ids()
    {
        var result = SharedDictionaryNegotiation.Negotiate(7u, new uint[] { 3u, 9u });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.True);
            Assert.That(result.FellBack, Is.True);
        });
    }

    [Test]
    public void Negotiate_falls_back_known_when_peer_advertises_empty_capability()
    {
        var result = SharedDictionaryNegotiation.Negotiate(7u, Array.Empty<uint>());

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.True);
            Assert.That(result.FellBack, Is.True);
        });
    }

    // Fingerprint-gated overload.

    [Test]
    public void Negotiate_fingerprint_returns_no_dictionary_matched_when_nothing_configured()
    {
        var result = SharedDictionaryNegotiation.Negotiate(
            0u, 1234UL, new[] { new AdvertisedCompressionDictionary(1u, 5UL) });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.True);
            Assert.That(result.PeerCapabilityKnown, Is.False);
            Assert.That(result.FellBack, Is.False);
            Assert.That(result.FingerprintMismatch, Is.False);
        });
    }

    [Test]
    public void Negotiate_fingerprint_falls_back_unknown_when_peer_capability_is_null()
    {
        var result = SharedDictionaryNegotiation.Negotiate(
            7u, 99UL, peerAdvertised: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.False);
            Assert.That(result.FellBack, Is.True);
            Assert.That(result.FingerprintMismatch, Is.False);
        });
    }

    [Test]
    public void Negotiate_fingerprint_matches_when_id_and_fingerprint_agree()
    {
        var result = SharedDictionaryNegotiation.Negotiate(
            7u,
            42UL,
            new[]
            {
                new AdvertisedCompressionDictionary(3u, 11UL),
                new AdvertisedCompressionDictionary(7u, 42UL),
            });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(7u));
            Assert.That(result.Matched, Is.True);
            Assert.That(result.PeerCapabilityKnown, Is.True);
            Assert.That(result.FellBack, Is.False);
            Assert.That(result.FingerprintMismatch, Is.False);
        });
    }

    [Test]
    public void Negotiate_fingerprint_falls_back_with_mismatch_when_same_id_different_fingerprint()
    {
        var result = SharedDictionaryNegotiation.Negotiate(
            7u,
            42UL,
            new[] { new AdvertisedCompressionDictionary(7u, 999UL) });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.True);
            Assert.That(result.FellBack, Is.True);
            Assert.That(result.FingerprintMismatch, Is.True);
        });
    }

    [Test]
    public void Negotiate_fingerprint_falls_back_without_mismatch_when_id_absent()
    {
        var result = SharedDictionaryNegotiation.Negotiate(
            7u,
            42UL,
            new[] { new AdvertisedCompressionDictionary(3u, 11UL) });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.True);
            Assert.That(result.FellBack, Is.True);
            Assert.That(result.FingerprintMismatch, Is.False);
        });
    }

    [Test]
    public void Negotiate_fingerprint_falls_back_without_mismatch_when_capability_empty()
    {
        var result = SharedDictionaryNegotiation.Negotiate(
            7u, 42UL, Array.Empty<AdvertisedCompressionDictionary>());

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(result.Matched, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.True);
            Assert.That(result.FellBack, Is.True);
            Assert.That(result.FingerprintMismatch, Is.False);
        });
    }

    [Test]
    public void Negotiate_fingerprint_matches_when_a_later_entry_carries_matching_fingerprint()
    {
        // Defensive duplicate-id guard: a mismatching entry must not mask a
        // later matching entry for the same id.
        var result = SharedDictionaryNegotiation.Negotiate(
            7u,
            42UL,
            new[]
            {
                new AdvertisedCompressionDictionary(7u, 999UL),
                new AdvertisedCompressionDictionary(7u, 42UL),
            });

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveDictionaryId, Is.EqualTo(7u));
            Assert.That(result.Matched, Is.True);
            Assert.That(result.FellBack, Is.False);
            Assert.That(result.FingerprintMismatch, Is.False);
        });
    }
}

/// <summary>
/// Unit coverage of <see cref="CompressionDictionaryFingerprint.Compute"/>:
/// determinism, empty-input behaviour, and sensitivity to a single changed
/// byte.
/// </summary>
[TestFixture]
public class CompressionDictionaryFingerprintTests
{
    [Test]
    public void Compute_is_deterministic_for_the_same_bytes()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };

        var a = CompressionDictionaryFingerprint.Compute(bytes);
        var b = CompressionDictionaryFingerprint.Compute(bytes);

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Compute_returns_offset_basis_for_empty_span()
    {
        var result = CompressionDictionaryFingerprint.Compute(ReadOnlySpan<byte>.Empty);

        Assert.That(result, Is.EqualTo(14695981039346656037UL));
    }

    [Test]
    public void Compute_differs_for_a_single_changed_byte()
    {
        var a = CompressionDictionaryFingerprint.Compute(new byte[] { 1, 2, 3 });
        var b = CompressionDictionaryFingerprint.Compute(new byte[] { 1, 2, 4 });

        Assert.That(a, Is.Not.EqualTo(b));
    }
}

/// <summary>
/// Unit coverage of <see cref="CompressionDictionaryAdvertisement.Build"/>:
/// null/no-catalog/empty short-circuits, and the ordered (id, fingerprint)
/// snapshot built from a populated provider.
/// </summary>
[TestFixture]
public class CompressionDictionaryAdvertisementTests
{
    [Test]
    public void Build_returns_null_for_null_provider()
    {
        Assert.That(CompressionDictionaryAdvertisement.Build(null), Is.Null);
    }

    [Test]
    public void Build_returns_null_when_provider_exposes_no_catalog()
    {
        var provider = new NonCatalogProvider();

        Assert.That(CompressionDictionaryAdvertisement.Build(provider), Is.Null);
    }

    [Test]
    public void Build_returns_null_for_empty_catalog()
    {
        Assert.That(
            CompressionDictionaryAdvertisement.Build(
                OperatorSuppliedCompressionDictionaryProvider.Empty),
            Is.Null);
    }

    [Test]
    public void Build_returns_ordered_id_fingerprint_pairs()
    {
        var d3 = new byte[] { 10, 20, 30 };
        var d1 = new byte[] { 7, 7, 7, 7 };
        var provider = new OperatorSuppliedCompressionDictionaryProvider(
            new Dictionary<uint, ReadOnlyMemory<byte>>
            {
                [3u] = d3,
                [1u] = d1,
            });

        var result = CompressionDictionaryAdvertisement.Build(provider);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Length, Is.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(result[0].Id, Is.EqualTo(1u));
            Assert.That(result[0].Fingerprint,
                Is.EqualTo(CompressionDictionaryFingerprint.Compute(d1)));
            Assert.That(result[1].Id, Is.EqualTo(3u));
            Assert.That(result[1].Fingerprint,
                Is.EqualTo(CompressionDictionaryFingerprint.Compute(d3)));
        });
    }

    private sealed class NonCatalogProvider : ILatticeCompressionDictionaryProvider
    {
        public bool TryGetDictionary(uint dictionaryId, out ReadOnlyMemory<byte> dictionary)
        {
            dictionary = ReadOnlyMemory<byte>.Empty;
            return false;
        }
    }
}
