using System.Buffers.Binary;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The range checks the two durable record decoders apply to a payload that
/// already passed its envelope: a checksum proves the bytes are the ones that
/// were written, not that the values in them are ones this build could have
/// written.
/// </summary>
/// <remarks>
/// <para>
/// Both decoders open with a compound guard rejecting a negative field, and
/// neither had a test for any of its terms. The existing fixtures perturb a
/// record and reseal it, which reaches the <i>envelope</i> and the phase and
/// cursor-length checks, so the negative-field guards sat behind arms that were
/// already satisfied. Each term is driven separately here, because a compound
/// condition proved through one of its terms leaves every other term free to be
/// deleted with the suite still green.
/// </para>
/// <para>
/// The consequence of accepting one is not a bad error message. A negative
/// chunk count is a negative array length, and a negative ingested or expected
/// count is a build that reports progress past its own end.
/// </para>
/// </remarks>
[TestFixture]
public sealed class VectorIndexDurableRecordDecodeTests
{
    private const int BuildStateFixedSize = 24;
    private const int GenerationOffset = 0;
    private const int PhaseOffset = 8;
    private const int IngestedOffset = 12;
    private const int ExpectedOffset = 16;
    private const int CursorLengthOffset = 20;

    private const int EpochOffset = 0;
    private const int ChunkCountOffset = 8;
    private const int VectorCountOffset = 12;
    private const int IndexVersionOffset = 16;

    /// <summary>
    /// A well-formed build-state payload with a null cursor, which every test
    /// then perturbs in exactly one field.
    /// </summary>
    private static byte[] BuildStatePayload(int trailingBytes = 0)
    {
        var payload = new byte[BuildStateFixedSize + trailingBytes];
        var span = payload.AsSpan();
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(GenerationOffset, 8), 3);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(PhaseOffset, 4), (int)VectorIndexBuildPhase.Ingesting);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(IngestedOffset, 4), 5);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(ExpectedOffset, 4), 9);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(CursorLengthOffset, 4), -1);
        return payload;
    }

    private static byte[] PartitionStatePayload(int trailingBytes = 0)
    {
        var payload = new byte[VectorIndexPartitionState.Size + trailingBytes];
        var span = payload.AsSpan();
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(EpochOffset, 8), 4);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(ChunkCountOffset, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(VectorCountOffset, 4), 7);
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(IndexVersionOffset, 8), 11);
        return payload;
    }

    [Test]
    public void The_build_state_probe_payload_is_itself_decodable()
    {
        // Anti-vacuity. Every negative-field test below asserts a refusal, and a
        // refusal is also what a payload malformed for some unrelated reason
        // produces - so without this the whole fixture could pass while proving
        // nothing about the field it names.
        Assert.That(
            VectorIndexBuildState.TryReadRecord(VectorIndexRecord.Wrap(BuildStatePayload()), out var state),
            Is.True);

        Assert.That(state, Is.EqualTo(new VectorIndexBuildState(3, VectorIndexBuildPhase.Ingesting, 5, 9, null)));
    }

    [TestCase(GenerationOffset, "generation")]
    [TestCase(IngestedOffset, "ingested count")]
    [TestCase(ExpectedOffset, "expected count")]
    public void A_build_state_declaring_a_negative_count_is_refused(int offset, string field)
    {
        var payload = BuildStatePayload();
        if (offset == GenerationOffset)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, 8), -1);
        }
        else
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), -1);
        }

        Assert.That(
            VectorIndexBuildState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _),
            Is.False,
            $"A negative {field} names a checkpoint no build this code wrote could have produced.");
    }

    [Test]
    public void A_build_state_declaring_a_cursor_length_below_the_null_marker_is_refused()
    {
        // -1 is the legal "no cursor" marker, so the guard is not "negative" but
        // "below -1". The boundary is what separates a valid null cursor from a
        // length that would slice backwards out of the payload.
        var payload = BuildStatePayload();
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(CursorLengthOffset, 4), -2);

        Assert.That(VectorIndexBuildState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _), Is.False);
    }

    [Test]
    public void The_null_cursor_marker_itself_is_still_accepted()
    {
        // The other side of that boundary, so the guard cannot be tightened to
        // reject -1 without a test failing.
        var payload = BuildStatePayload();
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(CursorLengthOffset, 4), -1);

        Assert.That(VectorIndexBuildState.TryReadRecord(VectorIndexRecord.Wrap(payload), out var state), Is.True);
        Assert.That(state.Cursor, Is.Null);
    }

    [Test]
    public void A_null_cursor_build_state_carrying_trailing_bytes_is_refused()
    {
        // A record declaring no cursor must be exactly the fixed size. Trailing
        // bytes mean the writer and the reader disagree about the layout, which
        // is the same class of failure as a wrong version and gets the same
        // answer: discard and rebuild.
        var payload = BuildStatePayload(trailingBytes: 3);

        Assert.That(VectorIndexBuildState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _), Is.False);
    }

    [Test]
    public void A_build_state_is_range_checked_before_its_phase_is()
    {
        // Ordering matters here because the phase check is the one the existing
        // fixtures already reach. Pairing a negative field with an undefined
        // phase would pass whichever guard ran first; pairing it with a VALID
        // phase leaves only the range check able to refuse it.
        var payload = BuildStatePayload();
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(IngestedOffset, 4), int.MinValue);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(PhaseOffset, 4), (int)VectorIndexBuildPhase.Ready);

        Assert.That(VectorIndexBuildState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _), Is.False);
    }

    [Test]
    public void The_partition_state_probe_payload_is_itself_decodable()
    {
        Assert.That(
            VectorIndexPartitionState.TryReadRecord(
                VectorIndexRecord.Wrap(PartitionStatePayload()), out var state, out var epochs),
            Is.True);

        Assert.That(state, Is.EqualTo(new VectorIndexPartitionState(4, 2, 7, 11)));
        Assert.That(epochs, Is.EqualTo(new long[] { 4, 4 }));
    }

    [TestCase(EpochOffset, 8, "epoch")]
    [TestCase(ChunkCountOffset, 4, "chunk count")]
    [TestCase(VectorCountOffset, 4, "vector count")]
    [TestCase(IndexVersionOffset, 8, "index version")]
    public void A_partition_state_declaring_a_negative_field_is_refused(int offset, int width, string field)
    {
        var payload = PartitionStatePayload();
        if (width == 8)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, 8), -1);
        }
        else
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), -1);
        }

        Assert.That(
            VectorIndexPartitionState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _, out _),
            Is.False,
            $"A negative {field} names a commit record no flush this code wrote could have produced.");
    }

    [Test]
    public void A_partition_state_declaring_a_negative_chunk_count_is_refused_before_it_sizes_an_array()
    {
        // The chunk count is used directly as an array length immediately after
        // the guard, so this is the term whose removal fails loudest - and the
        // one whose refusal must therefore be pinned rather than inferred from a
        // sibling.
        var payload = PartitionStatePayload();
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(ChunkCountOffset, 4), int.MinValue);

        Assert.DoesNotThrow(
            () => VectorIndexPartitionState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _, out _));
        Assert.That(
            VectorIndexPartitionState.TryReadRecord(VectorIndexRecord.Wrap(payload), out _, out var epochs),
            Is.False);
        Assert.That(epochs, Is.Empty, "A refused record must not hand back a partly-built epoch array.");
    }

    [Test]
    public void A_zero_chunk_partition_state_is_still_accepted()
    {
        // The boundary of the chunk-count guard. A partition with no chunks is a
        // legal empty partition, so the check has to be "negative", not
        // "non-positive".
        var payload = PartitionStatePayload();
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(ChunkCountOffset, 4), 0);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(VectorCountOffset, 4), 0);

        Assert.That(
            VectorIndexPartitionState.TryReadRecord(VectorIndexRecord.Wrap(payload), out var state, out var epochs),
            Is.True);
        Assert.That(state.ChunkCount, Is.Zero);
        Assert.That(epochs, Is.Empty);
    }
}
