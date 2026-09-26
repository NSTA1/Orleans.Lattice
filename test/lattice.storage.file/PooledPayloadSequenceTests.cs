using System.Buffers;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Direct coverage for the chunked payload buffer that
/// <see cref="FileWalReadPressureTests"/> only ever reaches through a real WAL
/// read.
/// <para>
/// Driving it through a shard proves the happy path and nothing else: a shard
/// only ever fills the buffer with a payload it has already framed, so the
/// degenerate inputs are unreachable from there even though they are perfectly
/// reachable on the type. Three are exercised here - a sequence read before any
/// fill, a non-positive byte count, and a second
/// <see cref="IDisposable.Dispose"/> - because each of them governs whether the
/// buffer can hand out a view over chunks it has already returned to
/// <see cref="ArrayPool{T}"/>. That failure mode is silent rather than loud: the
/// pool neither detects nor refuses a stale view, so it surfaces as unrelated
/// corruption in whichever component next rents the same array.
/// </para>
/// </summary>
[TestFixture]
public sealed class PooledPayloadSequenceTests
{
    [Test]
    public void A_sequence_read_before_any_fill_is_empty_rather_than_undefined()
    {
        using var sequence = new PooledPayloadSequence();

        Assert.Multiple(() =>
        {
            Assert.That(sequence.Sequence.IsEmpty, Is.True);
            Assert.That(sequence.Sequence.Length, Is.EqualTo(0L));
        });
    }

    [Test]
    public void A_sequence_read_after_a_reset_is_empty_again()
    {
        // Reset is called between entries, so the emptied state is reached on
        // every page rather than only on a fresh instance.
        using var stream = new MemoryStream([1, 2, 3, 4]);
        using var sequence = new PooledPayloadSequence();

        sequence.Fill(stream, position: 0, byteCount: 4);
        Assert.That(sequence.Sequence.Length, Is.EqualTo(4L), "the fill must be observable before the reset");

        sequence.Reset();

        Assert.That(sequence.Sequence.IsEmpty, Is.True, "a reset sequence must not expose returned chunks");
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void A_non_positive_byte_count_yields_an_empty_sequence_without_touching_the_stream(int byteCount)
    {
        // The early return happens after Reset, so the previous entry's chunks
        // are still returned to the pool; what must not happen is a rent, a
        // seek, or a read for a payload of no bytes.
        using var stream = new ThrowingStream();
        using var sequence = new PooledPayloadSequence();

        sequence.Fill(stream, position: 0, byteCount);

        Assert.That(sequence.Sequence.IsEmpty, Is.True);
    }

    [Test]
    public void A_non_positive_byte_count_still_releases_the_previous_payload()
    {
        // The empty read is the common shape for a zero-length entry, so it sits
        // in the middle of a page rather than at its end. If it skipped the
        // reset, the previous entry's chunks would leak for the life of the read.
        using var stream = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8]);
        using var sequence = new PooledPayloadSequence();

        sequence.Fill(stream, position: 0, byteCount: 8);
        Assert.That(sequence.Sequence.Length, Is.EqualTo(8L));

        sequence.Fill(stream, position: 0, byteCount: 0);

        Assert.That(sequence.Sequence.IsEmpty, Is.True, "the superseded payload must not remain readable");
    }

    [Test]
    public void Disposing_twice_leaves_the_instance_disposed_rather_than_reusable()
    {
        // Dispose is reached twice whenever the buffer is held in a `using`
        // inside a method whose caller also disposes it, so the second call has
        // to be harmless. Note what this can and cannot witness: Reset empties
        // the rented list before Dispose sets its flag, so a second Reset would
        // iterate nothing and the guard is an early return rather than the only
        // thing standing between the pool and a double return. What is
        // observable, and what is asserted here, is that the second call neither
        // throws nor resurrects the instance.
        using var stream = new MemoryStream(new byte[1024]);
        var sequence = new PooledPayloadSequence();
        sequence.Fill(stream, position: 0, byteCount: 1024);

        sequence.Dispose();
        Assert.DoesNotThrow(sequence.Dispose);

        Assert.Multiple(() =>
        {
            Assert.That(sequence.Sequence.IsEmpty, Is.True, "a disposed buffer must expose no chunks");
            Assert.Throws<ObjectDisposedException>(
                () => sequence.Fill(stream, position: 0, byteCount: 4),
                "a second dispose must not clear the disposed flag");
        });
    }

    [Test]
    public void Filling_a_disposed_sequence_is_refused()
    {
        using var stream = new MemoryStream([1, 2, 3, 4]);
        var sequence = new PooledPayloadSequence();
        sequence.Dispose();

        Assert.Throws<ObjectDisposedException>(() => sequence.Fill(stream, position: 0, byteCount: 4));
    }

    [Test]
    public void A_payload_larger_than_one_chunk_is_exposed_as_a_multi_segment_sequence()
    {
        // The point of the type: a payload spanning chunks must never require a
        // contiguous buffer of its own size, so the sequence really is split
        // rather than compacted back into one block.
        var payload = new byte[(PooledPayloadSequence.ChunkBytes * 2) + 17];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i % 251);
        }

        using var stream = new MemoryStream(payload);
        using var sequence = new PooledPayloadSequence();
        sequence.Fill(stream, position: 0, payload.Length);

        var read = sequence.Sequence;

        Assert.Multiple(() =>
        {
            Assert.That(read.Length, Is.EqualTo(payload.Length));
            Assert.That(read.IsSingleSegment, Is.False, "a multi-chunk payload must stay chunked");
            Assert.That(read.ToArray(), Is.EqualTo(payload).AsCollection);
        });
    }

    [Test]
    public void A_fill_reads_from_the_requested_position_rather_than_the_stream_cursor()
    {
        using var stream = new MemoryStream([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
        using var sequence = new PooledPayloadSequence();

        sequence.Fill(stream, position: 4, byteCount: 3);

        Assert.That(sequence.Sequence.ToArray(), Is.EqualTo(new byte[] { 5, 4, 3 }).AsCollection);
    }

    /// <summary>
    /// A stream that fails any access at all, so a test asserting "the stream is
    /// not touched" fails at the offending call rather than on a later
    /// inference about its position.
    /// </summary>
    private sealed class ThrowingStream : Stream
    {
        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => throw new InvalidOperationException("The stream must not be measured.");

        public override long Position
        {
            get => throw new InvalidOperationException("The stream must not be read.");
            set => throw new InvalidOperationException("The stream must not be positioned.");
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new InvalidOperationException("The stream must not be read for an empty payload.");

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new InvalidOperationException("The stream must not be sought for an empty payload.");

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
