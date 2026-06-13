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
}
