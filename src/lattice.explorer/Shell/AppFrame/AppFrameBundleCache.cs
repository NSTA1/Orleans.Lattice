using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// A process-wide cache of verified UI bundle bytes, keyed by
/// <c>(sourceKey, slug, version, bundleDigest, path)</c>.
/// </summary>
/// <remarks>
/// <para>
/// It holds bytes only, never a credential, a circuit or any scoped service, so it is
/// safe as a singleton. It is consulted only by <see cref="AppFrameBundleLoader.LoadAsync"/>,
/// and only for a launch the per-launch workspace gate has already authorised for the
/// current user: a hit saves the fetch, never the gate.
/// </para>
/// <para>
/// Only bytes that were verified against the manifest's digest are stored, and they are
/// copied on the way in, so a caller cannot mutate a cached asset. The cache is bounded by
/// <see cref="MaxTotalBytes"/>; once full it stops admitting rather than evicting, which is
/// correct (a miss just refetches) and keeps the admission path simple.
/// </para>
/// </remarks>
internal sealed class AppFrameBundleCache
{
    /// <summary>The default bound on cached bytes (64 MiB).</summary>
    public const long DefaultMaxTotalBytes = 64L * 1024 * 1024;

    private readonly ConcurrentDictionary<Key, byte[]> _entries = new();
    private long _totalBytes;

    /// <summary>Creates a cache with the default bound.</summary>
    public AppFrameBundleCache()
        : this(DefaultMaxTotalBytes)
    {
    }

    /// <summary>Creates a cache with an explicit bound.</summary>
    /// <param name="maxTotalBytes">The most bytes the cache holds; zero disables caching.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxTotalBytes"/> is negative.</exception>
    public AppFrameBundleCache(long maxTotalBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxTotalBytes);
        MaxTotalBytes = maxTotalBytes;
    }

    /// <summary>The most bytes the cache holds.</summary>
    public long MaxTotalBytes { get; }

    /// <summary>The bytes the cache now holds.</summary>
    public long TotalBytes => Interlocked.Read(ref _totalBytes);

    /// <summary>The number of cached assets.</summary>
    public int Count => _entries.Count;

    /// <summary>Looks up verified bytes.</summary>
    /// <param name="launch">The authorised launch.</param>
    /// <param name="path">The asset path.</param>
    /// <param name="bytes">The cached bytes on a hit.</param>
    /// <returns>Whether the asset was cached.</returns>
    public bool TryGet(AppFrameLaunch launch, string path, out ReadOnlyMemory<byte> bytes)
    {
        if (_entries.TryGetValue(KeyOf(launch, path), out var cached))
        {
            bytes = cached;
            return true;
        }

        bytes = default;
        return false;
    }

    /// <summary>Stores verified bytes, copying them, unless that would exceed <see cref="MaxTotalBytes"/>.</summary>
    /// <param name="launch">The authorised launch.</param>
    /// <param name="path">The asset path.</param>
    /// <param name="verified">Bytes already verified against the asset's pinned digest.</param>
    /// <returns>Whether the bytes were admitted.</returns>
    public bool TryAdd(AppFrameLaunch launch, string path, ReadOnlyMemory<byte> verified)
    {
        var length = verified.Length;
        if (Interlocked.Add(ref _totalBytes, length) > MaxTotalBytes)
        {
            Interlocked.Add(ref _totalBytes, -length);
            return false;
        }

        if (!_entries.TryAdd(KeyOf(launch, path), verified.ToArray()))
        {
            Interlocked.Add(ref _totalBytes, -length);
            return false;
        }

        return true;
    }

    private static Key KeyOf(AppFrameLaunch launch, string path) =>
        new(launch.SourceKey, launch.Slug, launch.Version, launch.Ui.BundleDigest, path);

    private readonly record struct Key(string SourceKey, string Slug, string Version, string BundleDigest, string Path);
}
