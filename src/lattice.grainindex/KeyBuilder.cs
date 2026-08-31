using System.Buffers;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// A minimal append-only character buffer used to build one index-entry key.
/// It starts on a caller-supplied stack span and grows into an
/// <see cref="ArrayPool{T}"/> array only when a key outgrows it, so the common
/// case - a short property name, a fixed-width numeric payload, and a short
/// grain key - builds a key with no heap traffic beyond the single result
/// string.
/// </summary>
/// <remarks>
/// This is a <c>ref struct</c>, so it can never escape to the heap and can
/// never be captured by an async state machine. A caller must
/// <see cref="Dispose"/> it (a <c>try</c>/<c>finally</c>) to return a rented
/// array; skipping that costs a pooled array, not correctness.
/// </remarks>
internal ref struct KeyBuilder
{
    private Span<char> _buffer;
    private char[]? _rented;
    private int _length;

    /// <summary>Initialises the builder over an initial, caller-owned span.</summary>
    /// <param name="initialBuffer">The initial buffer, typically <c>stackalloc</c>.</param>
    internal KeyBuilder(Span<char> initialBuffer)
    {
        _buffer = initialBuffer;
        _rented = null;
        _length = 0;
    }

    /// <summary>The characters written so far.</summary>
    internal readonly ReadOnlySpan<char> WrittenSpan => _buffer[.._length];

    /// <summary>Appends a single character.</summary>
    /// <param name="value">The character to append.</param>
    internal void Append(char value)
    {
        if (_length == _buffer.Length)
            Grow(1);

        _buffer[_length++] = value;
    }

    /// <summary>Appends a run of characters.</summary>
    /// <param name="value">The characters to append.</param>
    internal void Append(ReadOnlySpan<char> value)
    {
        if (value.IsEmpty)
            return;

        if (_length + value.Length > _buffer.Length)
            Grow(value.Length);

        value.CopyTo(_buffer[_length..]);
        _length += value.Length;
    }

    /// <summary>
    /// Reserves <paramref name="count"/> characters and returns the span to
    /// write them into. The caller must follow a successful write with
    /// <see cref="Advance(int)"/>.
    /// </summary>
    /// <param name="count">The number of characters to reserve.</param>
    /// <returns>A span of at least <paramref name="count"/> characters.</returns>
    internal Span<char> GetSpan(int count)
    {
        if (_length + count > _buffer.Length)
            Grow(count);

        return _buffer.Slice(_length, count);
    }

    /// <summary>Commits characters previously reserved with <see cref="GetSpan(int)"/>.</summary>
    /// <param name="count">The number of characters written.</param>
    internal void Advance(int count) => _length += count;

    /// <summary>Materialises the accumulated characters as a string.</summary>
    /// <returns>The built key.</returns>
    public override readonly string ToString() => new(WrittenSpan);

    /// <summary>Returns any rented array to the pool.</summary>
    internal void Dispose()
    {
        var rented = _rented;
        _rented = null;
        if (rented is not null)
            ArrayPool<char>.Shared.Return(rented);
    }

    private void Grow(int additional)
    {
        int required = _length + additional;
        int capacity = Math.Max(required, _buffer.Length * 2);
        var replacement = ArrayPool<char>.Shared.Rent(capacity);
        _buffer[.._length].CopyTo(replacement);

        var previous = _rented;
        _buffer = replacement;
        _rented = replacement;

        if (previous is not null)
            ArrayPool<char>.Shared.Return(previous);
    }
}
