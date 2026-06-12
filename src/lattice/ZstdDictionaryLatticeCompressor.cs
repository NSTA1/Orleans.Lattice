using System.Collections.Concurrent;
using ZstdSharp;

namespace Orleans.Lattice;

/// <summary>
/// Shared-dictionary <see cref="ILatticeCompressor"/> implementation for
/// <see cref="LatticeCompression.ZstdDictionary"/>. Backed by the
/// pure-managed <c>ZstdSharp.Port</c> port of the reference Zstandard
/// library (RFC 8478); compresses and inflates a payload against a
/// dictionary selected by a stable id resolved through an injected
/// <see cref="ILatticeCompressionDictionaryProvider"/>.
/// <para>
/// A shared dictionary captures the cross-batch redundancy (repeated key
/// prefixes, identical value schemas, recurring CRDT delta shapes) that a
/// per-batch dictionary-less compressor cannot see, recovering a saving on
/// small, self-similar batches. The reserved dictionary id <c>0</c> means
/// "no dictionary" and routes through a plain dictionary-less Zstandard
/// path, so the inherited <see cref="ILatticeCompressor"/> members behave
/// exactly like <see cref="ZstdLatticeCompressor"/>.
/// </para>
/// <para>
/// <c>ZstdSharp.Compressor</c> and <c>ZstdSharp.Decompressor</c> wrap
/// native contexts that are not safe for concurrent use, and a loaded
/// dictionary is sticky on the instance, so this type holds a pool of
/// instances <b>per dictionary id</b> and rents/returns per call. Each
/// pooled instance loads its dictionary once on first construction.
/// </para>
/// </summary>
public sealed class ZstdDictionaryLatticeCompressor : ILatticeDictionaryCompressor, IDisposable
{
    private readonly int _compressionLevel;
    private readonly ILatticeCompressionDictionaryProvider _dictionaryProvider;
    private readonly ConcurrentDictionary<uint, ConcurrentBag<Compressor>> _compressors = new();
    private readonly ConcurrentDictionary<uint, ConcurrentBag<Decompressor>> _decompressors = new();
    private bool _disposed;

    /// <summary>
    /// Initialises a new dictionary-aware compressor.
    /// <paramref name="compressionLevel"/> is the Zstandard compression
    /// level (1-22); the canonical production value is 3. Dictionary bytes
    /// are resolved on demand through
    /// <paramref name="dictionaryProvider"/>.
    /// </summary>
    /// <param name="compressionLevel">The Zstandard compression level (1-22).</param>
    /// <param name="dictionaryProvider">The shared-dictionary provider.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="compressionLevel"/> is outside [1, 22].
    /// </exception>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="dictionaryProvider"/> is <see langword="null"/>.
    /// </exception>
    public ZstdDictionaryLatticeCompressor(int compressionLevel, ILatticeCompressionDictionaryProvider dictionaryProvider)
    {
        if (compressionLevel < 1 || compressionLevel > 22)
        {
            throw new ArgumentOutOfRangeException(
                nameof(compressionLevel),
                compressionLevel,
                "Zstandard compression level must be in [1, 22].");
        }
        ArgumentNullException.ThrowIfNull(dictionaryProvider);
        _compressionLevel = compressionLevel;
        _dictionaryProvider = dictionaryProvider;
    }

    /// <inheritdoc />
    public LatticeCompression Algorithm => LatticeCompression.ZstdDictionary;

    /// <inheritdoc />
    public bool HasDictionary(uint dictionaryId)
        => dictionaryId != 0 && _dictionaryProvider.TryGetDictionary(dictionaryId, out _);

    /// <inheritdoc />
    public int GetMaxCompressedLength(int uncompressedLength)
        => Compressor.GetCompressBound(uncompressedLength);

    /// <inheritdoc />
    public int GetMaxCompressedLength(int uncompressedLength, uint dictionaryId)
        => Compressor.GetCompressBound(uncompressedLength);

    /// <inheritdoc />
    public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        => Compress(source, destination, 0u);

    /// <inheritdoc />
    public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
        => Decompress(source, destination, uncompressedLength, 0u);

    /// <inheritdoc />
    public int Compress(ReadOnlySpan<byte> source, Span<byte> destination, uint dictionaryId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var pool = _compressors.GetOrAdd(dictionaryId, static _ => new ConcurrentBag<Compressor>());
        if (!pool.TryTake(out var compressor))
        {
            compressor = new Compressor(_compressionLevel);
            if (dictionaryId != 0)
            {
                if (!_dictionaryProvider.TryGetDictionary(dictionaryId, out var dictionary))
                {
                    compressor.Dispose();
                    throw new ArgumentException(
                        $"Compression dictionary id {dictionaryId} is not registered with the dictionary provider.",
                        nameof(dictionaryId));
                }
                compressor.LoadDictionary(dictionary.ToArray());
            }
        }
        try
        {
            return compressor.Wrap(source, destination);
        }
        finally
        {
            pool.Add(compressor);
        }
    }

    /// <inheritdoc />
    public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength, uint dictionaryId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (destination.Length != uncompressedLength)
        {
            throw new ArgumentException(
                $"Destination span length ({destination.Length}) must match uncompressedLength ({uncompressedLength}).",
                nameof(destination));
        }

        var pool = _decompressors.GetOrAdd(dictionaryId, static _ => new ConcurrentBag<Decompressor>());
        if (!pool.TryTake(out var decompressor))
        {
            decompressor = new Decompressor();
            if (dictionaryId != 0)
            {
                if (!_dictionaryProvider.TryGetDictionary(dictionaryId, out var dictionary))
                {
                    decompressor.Dispose();
                    throw new ArgumentException(
                        $"Compression dictionary id {dictionaryId} is not registered with the dictionary provider.",
                        nameof(dictionaryId));
                }
                decompressor.LoadDictionary(dictionary.ToArray());
            }
        }
        int produced;
        try
        {
            produced = decompressor.Unwrap(source, destination);
        }
        catch (Exception inner) when (inner is not ArgumentException)
        {
            throw new ArgumentException(
                "Zstandard dictionary payload could not be decompressed; the bytes are not a valid Zstandard frame for the selected dictionary.",
                nameof(source),
                inner);
        }
        finally
        {
            pool.Add(decompressor);
        }
        if (produced != uncompressedLength)
        {
            throw new ArgumentException(
                $"Zstandard decompressed length ({produced}) does not match the framing-declared uncompressed length ({uncompressedLength}); the payload is corrupt.",
                nameof(source));
        }
    }

    /// <summary>
    /// Disposes every pooled compressor / decompressor instance across
    /// all dictionary ids. The singleton is held for the silo's lifetime,
    /// so disposal is invoked at silo shutdown via the DI container.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        foreach (var pool in _compressors.Values)
        {
            while (pool.TryTake(out var c))
            {
                c.Dispose();
            }
        }
        foreach (var pool in _decompressors.Values)
        {
            while (pool.TryTake(out var d))
            {
                d.Dispose();
            }
        }
    }
}
