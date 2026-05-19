using System.Buffers;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="PooledByteBufferWriter"/>
/// used by the WAL grain to encode each captured mutation exactly
/// once. The writer is the seam through which the grain produces
/// detached <see cref="ArraySegment{T}"/> payloads that are then
/// handed to the storage provider; correctness of <c>Advance</c>
/// bounds, capacity growth, detach-and-reset, and disposal
/// idempotency matters because the grain returns the underlying
/// array to <see cref="ArrayPool{T}.Shared"/> on the flush settle
/// path. A bug here would either drop bytes on the floor or
/// double-return a pooled buffer.
/// </summary>
[TestFixture]
public sealed class PooledByteBufferWriterTests
{
    [Test]
    public void WrittenCount_starts_at_zero()
    {
        using var writer = new PooledByteBufferWriter();

        Assert.That(writer.WrittenCount, Is.Zero);
    }

    [Test]
    public void GetSpan_with_default_hint_rents_at_least_256_bytes()
    {
        using var writer = new PooledByteBufferWriter();

        var span = writer.GetSpan();

        Assert.That(span.Length, Is.GreaterThanOrEqualTo(256));
    }

    [Test]
    public void GetMemory_with_default_hint_rents_at_least_256_bytes()
    {
        using var writer = new PooledByteBufferWriter();

        var memory = writer.GetMemory();

        Assert.That(memory.Length, Is.GreaterThanOrEqualTo(256));
    }

    [Test]
    public void GetSpan_with_explicit_hint_rents_at_least_that_many_bytes()
    {
        using var writer = new PooledByteBufferWriter();

        var span = writer.GetSpan(4096);

        Assert.That(span.Length, Is.GreaterThanOrEqualTo(4096));
    }

    [Test]
    public void GetSpan_throws_on_negative_hint()
    {
        using var writer = new PooledByteBufferWriter();

        Assert.That(
            () => writer.GetSpan(-1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetMemory_throws_on_negative_hint()
    {
        using var writer = new PooledByteBufferWriter();

        Assert.That(
            () => writer.GetMemory(-1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Advance_throws_on_negative_count()
    {
        using var writer = new PooledByteBufferWriter();
        _ = writer.GetSpan(16);

        Assert.That(
            () => writer.Advance(-1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Advance_before_first_rent_throws_invalid_operation()
    {
        using var writer = new PooledByteBufferWriter();

        // No GetSpan/GetMemory call has been made, so no buffer is
        // rented; advancing zero or any positive count must fail
        // rather than silently no-op (the contract is "you must
        // first ask for capacity").
        Assert.That(
            () => writer.Advance(1),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Advance_past_rented_capacity_throws_invalid_operation()
    {
        using var writer = new PooledByteBufferWriter();
        var capacity = writer.GetSpan(16).Length;

        Assert.That(
            () => writer.Advance(capacity + 1),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Advance_updates_written_count_and_subsequent_span_offsets()
    {
        using var writer = new PooledByteBufferWriter();
        var first = writer.GetSpan(64);
        first[0] = 0xAA;
        first[1] = 0xBB;
        writer.Advance(2);

        var next = writer.GetSpan(8);
        next[0] = 0xCC;
        writer.Advance(1);

        Assert.That(writer.WrittenCount, Is.EqualTo(3));
    }

    [Test]
    public void Detach_returns_segment_with_written_bytes_in_order()
    {
        using var writer = new PooledByteBufferWriter();
        var span = writer.GetSpan(4);
        span[0] = 1; span[1] = 2; span[2] = 3; span[3] = 4;
        writer.Advance(4);

        var segment = writer.DetachWrittenSegment();

        try
        {
            Assert.That(segment.Count, Is.EqualTo(4));
            Assert.That(segment.AsSpan().ToArray(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
            Assert.That(segment.Array, Is.Not.Null);
        }
        finally
        {
            if (segment.Array is not null)
            {
                ArrayPool<byte>.Shared.Return(segment.Array);
            }
        }
    }

    [Test]
    public void Detach_when_no_buffer_rented_returns_empty_segment_with_non_null_array()
    {
        using var writer = new PooledByteBufferWriter();

        var segment = writer.DetachWrittenSegment();

        Assert.Multiple(() =>
        {
            Assert.That(segment.Count, Is.Zero);
            Assert.That(segment.Array, Is.Not.Null,
                "the grain's return-to-pool path is unconditional so the segment must carry a non-null backing array");
            Assert.That(segment.Array, Is.SameAs(Array.Empty<byte>()));
        });
    }

    [Test]
    public void Detach_resets_writer_so_next_get_span_rents_fresh_buffer()
    {
        using var writer = new PooledByteBufferWriter();
        var span = writer.GetSpan(8);
        span[0] = 0xFF;
        writer.Advance(1);

        var first = writer.DetachWrittenSegment();
        try
        {
            // After detach the writer's accounting is reset.
            Assert.That(writer.WrittenCount, Is.Zero);

            var fresh = writer.GetSpan(8);
            fresh[0] = 0x11;
            writer.Advance(1);

            Assert.That(writer.WrittenCount, Is.EqualTo(1));

            var second = writer.DetachWrittenSegment();
            try
            {
                Assert.That(second.AsSpan().ToArray(), Is.EqualTo(new byte[] { 0x11 }));
            }
            finally
            {
                if (second.Array is not null)
                {
                    ArrayPool<byte>.Shared.Return(second.Array);
                }
            }
        }
        finally
        {
            if (first.Array is not null && first.Array.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(first.Array);
            }
        }
    }

    [Test]
    public void Get_span_after_partial_advance_grows_buffer_and_preserves_bytes()
    {
        using var writer = new PooledByteBufferWriter();

        // Fill the initial rent, then ask for more than is available
        // to force the grow path. Bytes already written must be
        // preserved verbatim into the new (larger) rented buffer.
        var initial = writer.GetSpan(16);
        for (var i = 0; i < initial.Length; i++)
        {
            initial[i] = (byte)i;
        }
        writer.Advance(initial.Length);

        var more = writer.GetSpan(initial.Length * 8);
        Assert.That(more.Length, Is.GreaterThanOrEqualTo(initial.Length * 8));

        var segment = writer.DetachWrittenSegment();
        try
        {
            Assert.That(segment.Count, Is.EqualTo(initial.Length));
            for (var i = 0; i < initial.Length; i++)
            {
                Assert.That(segment.Array![segment.Offset + i], Is.EqualTo((byte)i));
            }
        }
        finally
        {
            if (segment.Array is not null && segment.Array.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(segment.Array);
            }
        }
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var writer = new PooledByteBufferWriter();
        _ = writer.GetSpan(64);
        writer.Advance(1);

        writer.Dispose();

        Assert.That(() => writer.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Dispose_on_unused_writer_does_not_throw()
    {
        var writer = new PooledByteBufferWriter();

        Assert.That(() => writer.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Dispose_after_detach_does_not_double_return_pooled_buffer()
    {
        // Detach transfers ownership; Dispose must not also return
        // the (now caller-owned) array, or the pool would receive
        // the same buffer twice and a future rent could hand the
        // same array to two concurrent writers.
        var writer = new PooledByteBufferWriter();
        var span = writer.GetSpan(16);
        span[0] = 0x42;
        writer.Advance(1);

        var segment = writer.DetachWrittenSegment();
        try
        {
            Assert.That(() => writer.Dispose(), Throws.Nothing);
        }
        finally
        {
            if (segment.Array is not null && segment.Array.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(segment.Array);
            }
        }
    }
}
