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
}
