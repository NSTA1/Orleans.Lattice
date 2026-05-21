using System.Collections.Concurrent;
using ZstdSharp;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="ILatticeCompressor"/> implementation for
/// <see cref="LatticeCompression.Zstd"/>. Backed by the pure-managed
/// <c>ZstdSharp.Port</c> port of the reference Zstandard library
/// (RFC 8478); no native dependencies, no per-RID payload, identical
/// behaviour on every platform Orleans runs on.
/// <para>
/// Lives in the core <c>Orleans.Lattice</c> package so both the
/// replication framing layer and the WAL storage layer can register
/// the same compressor instance via DI. Replication's
/// <c>AddLatticeReplication</c> wires it in with the framing-tail
/// compression level by default; storage providers (e.g. Azure Table
/// WAL) can register their own instance with a different level when
/// the workload profile differs.
/// </para>
/// <para>
/// <c>ZstdSharp.Compressor</c> and <c>ZstdSharp.Decompressor</c> wrap
/// native contexts that are not safe for concurrent use, so this type
/// holds activation-scoped <see cref="ConcurrentBag{T}"/> pools and
/// rents/returns instances per call. The pool is unbounded but in
/// steady state holds at most one entry per concurrent encoder/decoder
/// caller (the gRPC marshaller holds the call inside a single grain
/// turn / RPC handler), so the working set stays small.
/// </para>
/// </summary>
public sealed class ZstdLatticeCompressor : ILatticeCompressor, IDisposable
{
    private readonly int _compressionLevel;
    private readonly ConcurrentBag<Compressor> _compressors = new();
    private readonly ConcurrentBag<Decompressor> _decompressors = new();
    private bool _disposed;

    /// <summary>
    /// Initialises a new compressor. <paramref name="compressionLevel"/>
    /// is the Zstandard compression level (1-22); the canonical
    /// production value is 3.
    /// </summary>
    public ZstdLatticeCompressor(int compressionLevel)
    {
        // The canonical replication surface restricts the level to the
        // documented [1, 22] range that ZSTD calls out as its standard
        // compression-level interval, even though the underlying
        // ZstdSharp.Compressor accepts ultra-fast negative levels and
        // a level-0 alias for "use default". Mirroring the validator
        // here gives a single shared contract across DI and direct
        // instantiation; out-of-range levels (including 0 and any
        // negative ultra-fast level) fail fast at construction.
        if (compressionLevel < 1 || compressionLevel > 22)
        {
            throw new ArgumentOutOfRangeException(
                nameof(compressionLevel),
                compressionLevel,
                "Zstandard compression level must be in [1, 22].");
        }
        _compressionLevel = compressionLevel;
    }

    /// <inheritdoc />
    public LatticeCompression Algorithm => LatticeCompression.Zstd;

    /// <inheritdoc />
    public int GetMaxCompressedLength(int uncompressedLength)
        => Compressor.GetCompressBound(uncompressedLength);

    /// <inheritdoc />
    public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_compressors.TryTake(out var compressor))
        {
            compressor = new Compressor(_compressionLevel);
        }
        try
        {
            return compressor.Wrap(source, destination);
        }
        finally
        {
            _compressors.Add(compressor);
        }
    }

    /// <inheritdoc />
    public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (destination.Length != uncompressedLength)
        {
            throw new ArgumentException(
                $"Destination span length ({destination.Length}) must match uncompressedLength ({uncompressedLength}).",
                nameof(destination));
        }

        if (!_decompressors.TryTake(out var decompressor))
        {
            decompressor = new Decompressor();
        }
        int produced;
        try
        {
            produced = decompressor.Unwrap(source, destination);
        }
        catch (Exception inner) when (inner is not ArgumentException)
        {
            throw new ArgumentException(
                "Zstandard payload could not be decompressed; the bytes are not a valid Zstandard frame.",
                nameof(source),
                inner);
        }
        finally
        {
            _decompressors.Add(decompressor);
        }
        if (produced != uncompressedLength)
        {
            throw new ArgumentException(
                $"Zstandard decompressed length ({produced}) does not match the framing-declared uncompressed length ({uncompressedLength}); the payload is corrupt.",
                nameof(source));
        }
    }

    /// <summary>
    /// Disposes the pooled compressor / decompressor instances. The
    /// shipper holds the singleton compressor for the silo's lifetime,
    /// so disposal is invoked at silo shutdown via the DI container.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        while (_compressors.TryTake(out var c))
        {
            c.Dispose();
        }
        while (_decompressors.TryTake(out var d))
        {
            d.Dispose();
        }
    }
}
