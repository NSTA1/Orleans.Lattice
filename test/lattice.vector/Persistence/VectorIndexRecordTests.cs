using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The record envelope is the single place that decides whether a persisted byte
/// sequence may be believed, so every way of damaging one is exercised here
/// rather than only where it is consumed.
/// </summary>
[TestFixture]
public sealed class VectorIndexRecordTests
{
    private static readonly byte[] Payload = [1, 2, 3, 4, 5, 6, 7];

    [Test]
    public void A_wrapped_payload_round_trips()
    {
        var record = VectorIndexRecord.Wrap(Payload);

        Assert.That(VectorIndexRecord.TryUnwrap(record, out var payload), Is.True);
        Assert.That(payload.ToArray(), Is.EqualTo(Payload));
    }

    [Test]
    public void An_empty_payload_round_trips()
    {
        var record = VectorIndexRecord.Wrap([]);

        Assert.That(VectorIndexRecord.TryUnwrap(record, out var payload), Is.True);
        Assert.That(payload.Length, Is.Zero);
    }

    [Test]
    public void Measure_reports_exactly_what_wrap_produces()
    {
        Assert.That(VectorIndexRecord.Wrap(Payload).Length, Is.EqualTo(VectorIndexRecord.Measure(Payload.Length)));
    }

    [Test]
    public void Wrapping_into_a_caller_buffer_reports_the_bytes_written()
    {
        var buffer = new byte[VectorIndexRecord.Measure(Payload.Length) + 8];

        var written = VectorIndexRecord.Wrap(Payload, buffer);

        Assert.That(written, Is.EqualTo(VectorIndexRecord.Measure(Payload.Length)));
        Assert.That(VectorIndexRecord.TryUnwrap(buffer.AsSpan(0, written), out var payload), Is.True);
        Assert.That(payload.ToArray(), Is.EqualTo(Payload));
    }

    [Test]
    public void Sealing_in_place_produces_the_same_record_as_wrapping()
    {
        var sealed_ = new byte[VectorIndexRecord.Measure(Payload.Length)];
        Payload.CopyTo(sealed_.AsSpan(VectorIndexPersistenceFormat.RecordHeaderSize));

        VectorIndexRecord.Seal(sealed_, Payload.Length);

        Assert.That(sealed_, Is.EqualTo(VectorIndexRecord.Wrap(Payload)));
    }

    [Test]
    public void A_short_buffer_is_refused_by_wrap()
    {
        Assert.That(() => VectorIndexRecord.Wrap(Payload, new byte[4]), Throws.ArgumentException);
    }

    [Test]
    public void A_wrongly_sized_buffer_is_refused_by_seal()
    {
        Assert.That(() => VectorIndexRecord.Seal(new byte[4], 7), Throws.ArgumentException);
    }

    [Test]
    public void A_negative_payload_length_is_refused_by_measure()
    {
        Assert.That(() => VectorIndexRecord.Measure(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void A_truncated_record_is_refused()
    {
        var record = VectorIndexRecord.Wrap(Payload);

        for (var length = 0; length < record.Length; length++)
        {
            Assert.That(VectorIndexRecord.TryUnwrap(record.AsSpan(0, length), out _), Is.False,
                $"A record truncated to {length} bytes must not be believed.");
        }
    }

    [Test]
    public void A_record_with_a_flipped_payload_byte_is_refused()
    {
        for (var offset = 0; offset < Payload.Length; offset++)
        {
            var record = VectorIndexRecord.Wrap(Payload);
            record[VectorIndexPersistenceFormat.RecordHeaderSize + offset] ^= 0xFF;

            Assert.That(VectorIndexRecord.TryUnwrap(record, out _), Is.False,
                $"A payload byte flipped at offset {offset} must fail the checksum.");
        }
    }

    [Test]
    public void A_record_with_a_wrong_marker_is_refused()
    {
        var record = VectorIndexRecord.Wrap(Payload);
        record[0] ^= 0xFF;

        Assert.That(VectorIndexRecord.TryUnwrap(record, out _), Is.False);
    }

    [Test]
    public void A_record_from_an_unsupported_layout_version_is_refused()
    {
        var record = VectorIndexRecord.Wrap(Payload);
        record[4] = 99;

        Assert.That(VectorIndexRecord.TryUnwrap(record, out _), Is.False);
    }

    [Test]
    public void A_record_whose_declared_length_disagrees_with_its_bytes_is_refused()
    {
        var record = VectorIndexRecord.Wrap(Payload);
        record[8] = (byte)(Payload.Length + 1);

        Assert.That(VectorIndexRecord.TryUnwrap(record, out _), Is.False);
    }

    [Test]
    public void A_record_declaring_a_negative_length_is_refused()
    {
        var record = VectorIndexRecord.Wrap(Payload);
        record[11] = 0x80;

        Assert.That(VectorIndexRecord.TryUnwrap(record, out _), Is.False);
    }

    [Test]
    public void The_layout_version_this_build_writes_is_the_one_it_reads()
    {
        Assert.Multiple(() =>
        {
            Assert.That(VectorIndexPersistenceFormat.IsSupported(VectorIndexPersistenceFormat.Version), Is.True);
            Assert.That(VectorIndexPersistenceFormat.IsSupported(VectorIndexPersistenceFormat.Version + 1), Is.False);
            Assert.That(VectorIndexPersistenceFormat.IsSupported(0), Is.False);
            Assert.That(VectorIndexPersistenceFormat.IsSupported(-1), Is.False);
        });
    }

    [Test]
    public void The_declared_payload_sizes_match_what_the_records_write()
    {
        Assert.Multiple(() =>
        {
            Assert.That(VectorIndexManifest.Size, Is.EqualTo(VectorIndexPersistenceFormat.ManifestPayloadSize));
            Assert.That(
                VectorIndexPartitionState.Size,
                Is.EqualTo(VectorIndexPersistenceFormat.PartitionStatePayloadSize));
            Assert.That(
                VectorIndexRecord.Measure(0),
                Is.EqualTo(VectorIndexPersistenceFormat.RecordHeaderSize));
        });
    }
}
