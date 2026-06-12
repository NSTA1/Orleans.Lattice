using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationAckTests
{
    [Test]
    public void Default_value_is_unaccepted_with_zero_hlc()
    {
        var ack = default(ReplicationAck);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.False);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void Init_assigns_every_property()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 7 };
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(hlc));
        });
    }

    [Test]
    public void Equality_uses_value_semantics()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 10, Counter = 1 };
        var a = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };
        var b = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };
        var c = new ReplicationAck { Accepted = false, HighestAppliedHlc = hlc };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(c));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void With_expression_produces_modified_copy()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };

        var modified = ack with { Accepted = false };

        Assert.Multiple(() =>
        {
            Assert.That(modified.Accepted, Is.False);
            Assert.That(modified.HighestAppliedHlc, Is.EqualTo(hlc));
            Assert.That(ack.Accepted, Is.True);
        });
    }

    [Test]
    public void Default_flow_control_hint_slots_are_null()
    {
        var ack = default(ReplicationAck);

        Assert.Multiple(() =>
        {
            Assert.That(ack.SuggestedBatchSize, Is.Null);
            Assert.That(ack.PauseForMs, Is.Null);
        });
    }

    [Test]
    public void Init_assigns_flow_control_hint_slots()
    {
        var ack = new ReplicationAck
        {
            Accepted = true,
            HighestAppliedHlc = HybridLogicalClock.Zero,
            SuggestedBatchSize = 64,
            PauseForMs = 250,
        };

        Assert.Multiple(() =>
        {
            Assert.That(ack.SuggestedBatchSize, Is.EqualTo(64));
            Assert.That(ack.PauseForMs, Is.EqualTo(250));
        });
    }

    [Test]
    public void Equality_uses_flow_control_hint_slots()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 10, Counter = 1 };
        var a = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc, SuggestedBatchSize = 32, PauseForMs = 100 };
        var b = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc, SuggestedBatchSize = 32, PauseForMs = 100 };
        var differentBatch = a with { SuggestedBatchSize = 64 };
        var differentPause = a with { PauseForMs = 200 };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(differentBatch));
            Assert.That(a, Is.Not.EqualTo(differentPause));
        });
    }

    [Test]
    public void Default_supported_wire_version_slot_is_null()
    {
        var ack = default(ReplicationAck);

        Assert.That(ack.SupportedWireVersion, Is.Null);
    }

    [Test]
    public void Init_assigns_supported_wire_version_slot()
    {
        var ack = new ReplicationAck
        {
            Accepted = true,
            HighestAppliedHlc = HybridLogicalClock.Zero,
            SupportedWireVersion = 4,
        };

        Assert.That(ack.SupportedWireVersion, Is.EqualTo(4));
    }

    [Test]
    public void Equality_uses_supported_wire_version_slot()
    {
        var a = new ReplicationAck { Accepted = true, SupportedWireVersion = 5 };
        var b = new ReplicationAck { Accepted = true, SupportedWireVersion = 5 };
        var different = a with { SupportedWireVersion = 3 };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(different));
        });
    }
}

[TestFixture]
public class WireVersionNegotiationTests
{
    [Test]
    public void Negotiate_uses_unknown_peer_floor_when_capability_is_null()
    {
        var result = WireVersionNegotiation.Negotiate(
            localCurrentVersion: 5,
            minimumSupportedVersion: 1,
            unknownPeerFloorVersion: 3,
            peerAdvertisedVersion: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveWireVersion, Is.EqualTo(3));
            Assert.That(result.PeerCapabilityKnown, Is.False);
            // Floor (3) is below local current (5), so a downgrade is in effect.
            Assert.That(result.DowngradeActive, Is.True);
        });
    }

    [Test]
    public void Negotiate_unknown_peer_floor_equal_to_current_is_not_a_downgrade()
    {
        var result = WireVersionNegotiation.Negotiate(
            localCurrentVersion: 5,
            minimumSupportedVersion: 1,
            unknownPeerFloorVersion: 5,
            peerAdvertisedVersion: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveWireVersion, Is.EqualTo(5));
            Assert.That(result.DowngradeActive, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.False);
        });
    }

    [Test]
    public void Negotiate_takes_min_of_local_and_peer_when_peer_is_older()
    {
        var result = WireVersionNegotiation.Negotiate(
            localCurrentVersion: 5,
            minimumSupportedVersion: 1,
            unknownPeerFloorVersion: 5,
            peerAdvertisedVersion: 3);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveWireVersion, Is.EqualTo(3));
            Assert.That(result.DowngradeActive, Is.True);
            Assert.That(result.PeerCapabilityKnown, Is.True);
        });
    }

    [Test]
    public void Negotiate_no_downgrade_when_peer_matches_local_current()
    {
        var result = WireVersionNegotiation.Negotiate(5, 1, 5, peerAdvertisedVersion: 5);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveWireVersion, Is.EqualTo(5));
            Assert.That(result.DowngradeActive, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.True);
        });
    }

    [Test]
    public void Negotiate_clamps_to_local_current_when_peer_is_newer()
    {
        // A newer receiver paired with an older sender: the sender can
        // only encode at its own current version.
        var result = WireVersionNegotiation.Negotiate(5, 1, 5, peerAdvertisedVersion: 9);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveWireVersion, Is.EqualTo(5));
            Assert.That(result.DowngradeActive, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.True);
        });
    }

    [Test]
    public void Negotiate_throws_when_peer_is_below_minimum_supported_floor()
    {
        Assert.That(
            () => WireVersionNegotiation.Negotiate(5, 3, 5, peerAdvertisedVersion: 2),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void Negotiate_accepts_peer_exactly_at_minimum_supported_floor()
    {
        var result = WireVersionNegotiation.Negotiate(5, 3, 5, peerAdvertisedVersion: 3);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveWireVersion, Is.EqualTo(3));
            Assert.That(result.DowngradeActive, Is.True);
            Assert.That(result.PeerCapabilityKnown, Is.True);
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Negotiate_throws_for_non_positive_local_current(int localCurrent)
    {
        Assert.That(
            () => WireVersionNegotiation.Negotiate(localCurrent, 1, 1, null),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(6)]
    public void Negotiate_throws_when_minimum_supported_is_out_of_range(int minimum)
    {
        Assert.That(
            () => WireVersionNegotiation.Negotiate(5, minimum, 5, null),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]  // below minimum
    [TestCase(6)]  // above local current
    public void Negotiate_throws_when_unknown_peer_floor_is_out_of_range(int floor)
    {
        Assert.That(
            () => WireVersionNegotiation.Negotiate(5, 2, floor, null),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}

[TestFixture]
public class WireVersionNegotiationResultTests
{
    [Test]
    public void Default_value_has_zeroed_fields()
    {
        var result = default(WireVersionNegotiationResult);

        Assert.Multiple(() =>
        {
            Assert.That(result.EffectiveWireVersion, Is.EqualTo(0));
            Assert.That(result.DowngradeActive, Is.False);
            Assert.That(result.PeerCapabilityKnown, Is.False);
        });
    }

    [Test]
    public void Equality_uses_value_semantics()
    {
        var a = new WireVersionNegotiationResult { EffectiveWireVersion = 3, DowngradeActive = true, PeerCapabilityKnown = true };
        var b = new WireVersionNegotiationResult { EffectiveWireVersion = 3, DowngradeActive = true, PeerCapabilityKnown = true };
        var c = a with { DowngradeActive = false };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(c));
        });
    }
}

[TestFixture]
public class WireVersionDownEncoderTests
{
    private static EncodedBatchHeader MakeHeader(
        int wireVersion = EncodedBatchHeader.CurrentWireVersion,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister,
        LatticeCompression compression = LatticeCompression.None)
        => new()
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = wireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId("site-a"),
            EntryCount = 1,
            BatchSequence = 1L,
            AtomicBatchSpanCount = 0,
            Mode = mode,
            Compression = compression,
        };

    [Test]
    public void MinimumDownEncodableWireVersion_is_one_below_current()
    {
        Assert.That(
            WireVersionDownEncoder.MinimumDownEncodableWireVersion,
            Is.EqualTo(EncodedBatchHeader.CurrentWireVersion - 1));
    }

    [Test]
    public void EnsureDownEncodable_same_version_is_a_noop_for_any_mode()
    {
        // A same-version target is never down-stamped, so even a CRDT
        // mode and a compressed tail are accepted (the verbatim hot path
        // ships the current-version frame unchanged).
        Assert.That(
            () => WireVersionDownEncoder.EnsureDownEncodable(
                EncodedBatchHeader.CurrentWireVersion,
                LatticeMergeMode.PnCounter,
                LatticeCompression.Zstd),
            Throws.Nothing);
    }

    [Test]
    public void EnsureDownEncodable_lww_uncompressed_down_stamp_is_allowed()
    {
        Assert.That(
            () => WireVersionDownEncoder.EnsureDownEncodable(
                WireVersionDownEncoder.MinimumDownEncodableWireVersion,
                LatticeMergeMode.LwwRegister,
                LatticeCompression.None),
            Throws.Nothing);
    }

    [Test]
    public void EnsureDownEncodable_crdt_mode_down_stamp_throws_not_supported()
    {
        Assert.That(
            () => WireVersionDownEncoder.EnsureDownEncodable(
                WireVersionDownEncoder.MinimumDownEncodableWireVersion,
                LatticeMergeMode.PnCounter,
                LatticeCompression.None),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void EnsureDownEncodable_compressed_down_stamp_throws_not_supported()
    {
        Assert.That(
            () => WireVersionDownEncoder.EnsureDownEncodable(
                WireVersionDownEncoder.MinimumDownEncodableWireVersion,
                LatticeMergeMode.LwwRegister,
                LatticeCompression.Zstd),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void EnsureDownEncodable_below_floor_throws_not_supported()
    {
        Assert.That(
            () => WireVersionDownEncoder.EnsureDownEncodable(
                WireVersionDownEncoder.MinimumDownEncodableWireVersion - 1,
                LatticeMergeMode.LwwRegister,
                LatticeCompression.None),
            Throws.InstanceOf<NotSupportedException>());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void EnsureDownEncodable_below_one_throws_argument_out_of_range(int version)
    {
        Assert.That(
            () => WireVersionDownEncoder.EnsureDownEncodable(
                version, LatticeMergeMode.LwwRegister, LatticeCompression.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void EnsureDownEncodable_above_current_throws_argument_out_of_range()
    {
        Assert.That(
            () => WireVersionDownEncoder.EnsureDownEncodable(
                EncodedBatchHeader.CurrentWireVersion + 1,
                LatticeMergeMode.LwwRegister,
                LatticeCompression.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void PrepareHeader_same_version_returns_header_unchanged()
    {
        var header = MakeHeader();

        var prepared = WireVersionDownEncoder.PrepareHeader(
            header, EncodedBatchHeader.CurrentWireVersion);

        Assert.That(prepared, Is.EqualTo(header));
    }

    [Test]
    public void PrepareHeader_down_stamp_lowers_wire_version_only()
    {
        var header = MakeHeader();
        var target = WireVersionDownEncoder.MinimumDownEncodableWireVersion;

        var prepared = WireVersionDownEncoder.PrepareHeader(header, target);

        Assert.Multiple(() =>
        {
            Assert.That(prepared.WireVersion, Is.EqualTo(target));
            // Every other field is preserved verbatim - only the version
            // slot changes, so the entry segments stay byte-identical.
            Assert.That(prepared, Is.EqualTo(header with { WireVersion = target }));
        });
    }

    [Test]
    public void PrepareHeader_crdt_mode_down_stamp_throws_not_supported()
    {
        var header = MakeHeader(mode: LatticeMergeMode.PnCounter);

        Assert.That(
            () => WireVersionDownEncoder.PrepareHeader(
                header, WireVersionDownEncoder.MinimumDownEncodableWireVersion),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void PrepareHeader_compressed_down_stamp_throws_not_supported()
    {
        var header = MakeHeader(compression: LatticeCompression.Zstd);

        Assert.That(
            () => WireVersionDownEncoder.PrepareHeader(
                header, WireVersionDownEncoder.MinimumDownEncodableWireVersion),
            Throws.InstanceOf<NotSupportedException>());
    }
}
