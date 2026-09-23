using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorIndexPartitionStateTests
{
    private static VectorIndexPartitionState State() => new(Epoch: 9, ChunkCount: 3, VectorCount: 200, IndexVersion: 55);

    [Test]
    public void A_partition_state_round_trips_through_a_record()
    {
        var state = State();

        Assert.That(VectorIndexPartitionState.TryReadRecord(state.ToRecord(), out var read), Is.True);
        Assert.That(read, Is.EqualTo(state));
    }

    [Test]
    public void Write_reports_exactly_the_declared_size()
    {
        Assert.That(State().Write(new byte[VectorIndexPartitionState.Size]),
            Is.EqualTo(VectorIndexPartitionState.Size));
    }

    [Test]
    public void A_short_buffer_is_refused()
    {
        Assert.That(() => State().Write(new byte[4]), Throws.ArgumentException);
    }

    [Test]
    public void A_corrupt_partition_state_is_refused()
    {
        var record = State().ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize] ^= 0xFF;

        Assert.That(VectorIndexPartitionState.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void A_payload_of_the_wrong_length_is_refused()
    {
        Assert.That(VectorIndexPartitionState.TryReadRecord(VectorIndexRecord.Wrap([1]), out _), Is.False);
    }

    [Test]
    public void A_negative_chunk_count_is_refused()
    {
        var record = State().ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 11] = 0x80;
        VectorIndexRecord.Seal(record, VectorIndexPartitionState.Size);

        Assert.That(VectorIndexPartitionState.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void An_empty_partition_round_trips()
    {
        var state = new VectorIndexPartitionState(0, 0, 0, 0);

        Assert.That(VectorIndexPartitionState.TryReadRecord(state.ToRecord(), out var read), Is.True);
        Assert.That(read, Is.EqualTo(state));
    }

    [Test]
    public void Chunks_sharing_the_record_epoch_are_written_in_the_compact_form()
    {
        var state = State();

        Assert.That(state.ToRecord([9, 9, 9]), Is.EqualTo(state.ToRecord()),
            "a uniform partition must stay byte-identical to what a build without per-chunk epochs reads");
    }

    [Test]
    public void The_compact_form_reads_as_every_chunk_at_the_record_epoch()
    {
        var state = State();

        Assert.That(VectorIndexPartitionState.TryReadRecord(state.ToRecord(), out var read, out var epochs), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(state));
            Assert.That(epochs, Is.EqualTo(new long[] { 9, 9, 9 }));
        });
    }

    [Test]
    public void Per_chunk_epochs_round_trip_through_a_record()
    {
        var state = State();
        long[] written = [4, 9, 7];

        Assert.That(VectorIndexPartitionState.TryReadRecord(state.ToRecord(written), out var read, out var epochs), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(state));
            Assert.That(epochs, Is.EqualTo(written));
        });
    }

    [Test]
    public void The_public_reader_refuses_the_extended_form_rather_than_misreading_it()
    {
        // Reading it as compact would claim every chunk lives under the newest
        // epoch, and the chunks that do not would read as missing.
        Assert.That(VectorIndexPartitionState.TryReadRecord(State().ToRecord([4, 9, 7]), out _), Is.False);
    }

    [Test]
    public void A_chunk_epoch_newer_than_the_record_epoch_is_refused()
    {
        var record = new VectorIndexPartitionState(9, 3, 200, 55).ToRecord([4, 9, 7]);
        var stamped = new VectorIndexPartitionState(8, 3, 200, 55);
        stamped.Write(record.AsSpan(VectorIndexPersistenceFormat.RecordHeaderSize));
        VectorIndexRecord.Seal(record, VectorIndexPartitionState.Size + (3 * sizeof(long)));

        Assert.That(VectorIndexPartitionState.TryReadRecord(record, out _, out _), Is.False);
    }

    [Test]
    public void An_extended_form_whose_length_disagrees_with_its_chunk_count_is_refused()
    {
        var payload = new byte[VectorIndexPartitionState.Size + (2 * sizeof(long))];
        State().Write(payload);

        Assert.That(VectorIndexPartitionState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _, out _), Is.False);
    }

    [Test]
    public void Chunk_epochs_that_do_not_match_the_chunk_count_are_refused_on_write()
    {
        Assert.That(() => State().ToRecord([9, 9]), Throws.ArgumentException);
    }
}
