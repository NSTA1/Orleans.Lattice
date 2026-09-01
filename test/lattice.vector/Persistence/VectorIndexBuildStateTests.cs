using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorIndexBuildStateTests
{
    [Test]
    public void A_build_state_with_a_cursor_round_trips()
    {
        var state = new VectorIndexBuildState(2, VectorIndexBuildPhase.Ingesting, 10, 100, "doc-000042");

        Assert.That(VectorIndexBuildState.TryReadRecord(state.ToRecord(), out var read), Is.True);
        Assert.That(read, Is.EqualTo(state));
    }

    [Test]
    public void A_build_state_without_a_cursor_round_trips_as_null_not_empty()
    {
        var state = new VectorIndexBuildState(0, VectorIndexBuildPhase.NotStarted, 0, 0, null);

        Assert.That(VectorIndexBuildState.TryReadRecord(state.ToRecord(), out var read), Is.True);
        Assert.That(read.Cursor, Is.Null,
            "A null cursor means 'start at the beginning' and must not decode as an empty identifier.");
    }

    [Test]
    public void An_empty_cursor_round_trips_as_empty_not_null()
    {
        var state = new VectorIndexBuildState(0, VectorIndexBuildPhase.Ingesting, 0, 0, string.Empty);

        Assert.That(VectorIndexBuildState.TryReadRecord(state.ToRecord(), out var read), Is.True);
        Assert.That(read.Cursor, Is.Empty);
    }

    [Test]
    public void A_non_ascii_cursor_round_trips()
    {
        var state = new VectorIndexBuildState(1, VectorIndexBuildPhase.Ingesting, 1, 2, "doc-\u00e9\u4e2d\u6587");

        Assert.That(VectorIndexBuildState.TryReadRecord(state.ToRecord(), out var read), Is.True);
        Assert.That(read.Cursor, Is.EqualTo(state.Cursor));
    }

    [Test]
    public void A_corrupt_build_state_is_refused()
    {
        var record = new VectorIndexBuildState(1, VectorIndexBuildPhase.Training, 5, 5, "x").ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 1] ^= 0xFF;

        Assert.That(VectorIndexBuildState.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void A_build_state_naming_an_undefined_phase_is_refused()
    {
        var record = new VectorIndexBuildState(1, VectorIndexBuildPhase.Training, 5, 5, null).ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 8] = 42;
        VectorIndexRecord.Seal(record, record.Length - VectorIndexPersistenceFormat.RecordHeaderSize);

        Assert.That(VectorIndexBuildState.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void A_build_state_whose_cursor_length_disagrees_with_its_payload_is_refused()
    {
        var record = new VectorIndexBuildState(1, VectorIndexBuildPhase.Ingesting, 5, 5, "abc").ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 20] = 99;
        VectorIndexRecord.Seal(record, record.Length - VectorIndexPersistenceFormat.RecordHeaderSize);

        Assert.That(VectorIndexBuildState.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void A_payload_shorter_than_the_fixed_header_is_refused()
    {
        Assert.That(VectorIndexBuildState.TryReadRecord(VectorIndexRecord.Wrap([1, 2, 3]), out _), Is.False);
    }
}
