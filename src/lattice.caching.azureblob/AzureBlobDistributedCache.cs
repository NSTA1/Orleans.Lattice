using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob;

/// <summary>
/// An <see cref="IDistributedCache"/> backed by an Azure Storage account: each
/// entry is a single block blob whose content is the cached value and whose
/// expiry (absolute cap, sliding window, and current effective instant) lives in
/// blob metadata. Keys are hashed to a fixed, storage-legal blob name by
/// <see cref="BlobCacheKeyMap"/>, so any caller-supplied key is safe.
/// <para>
/// Expiry is enforced lazily on read: an expired entry is treated as a miss and
/// best-effort deleted when next fetched or refreshed, and a sliding entry has
/// its effective expiry advanced (capped at the absolute expiration) on each
/// read. There is no background sweeper, so an entry that is written and never
/// read again lingers until it is overwritten or removed - acceptable for the
/// low-churn, per-subject workloads (for example a Microsoft.Identity.Web token
/// cache) this backend targets. Values are held whole in memory during a
/// read/write, so this cache is intended for small entries, not large blobs.
/// </para>
/// </summary>
internal sealed class AzureBlobDistributedCache : IDistributedCache
{
    private readonly BlobContainerClient _container;
    private readonly string _keyPrefix;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _initGate = new(1, 1);
    private bool _containerReady;

    /// <summary>Initializes a new <see cref="AzureBlobDistributedCache"/>.</summary>
    /// <param name="container">The blob container that backs the cache. Must not be <see langword="null"/>.</param>
    /// <param name="keyPrefix">The blob-name prefix prepended to every entry (may be empty). Must not be <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock used for expiry maths. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    public AzureBlobDistributedCache(BlobContainerClient container, string keyPrefix, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(keyPrefix);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _container = container;
        _keyPrefix = keyPrefix;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public byte[]? Get(string key) => GetAsync(key).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        token.ThrowIfCancellationRequested();
        await EnsureContainerAsync(token).ConfigureAwait(false);

        var blob = _container.GetBlobClient(BlobName(key));

        BlobDownloadResult download;
        try
        {
            download = (await blob.DownloadContentAsync(token).ConfigureAwait(false)).Value;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }

        var expiration = BlobCacheEntryExpiration.FromMetadata(download.Details.Metadata);
        var now = _timeProvider.GetUtcNow();
        if (BlobCacheEntryExpiration.IsExpired(expiration, now))
        {
            await TryDeleteAsync(blob, token).ConfigureAwait(false);
            return null;
        }

        await TrySlideAsync(blob, expiration, now, token).ConfigureAwait(false);
        return download.Content.ToArray();
    }

    /// <inheritdoc />
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
        => SetAsync(key, value, options).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task SetAsync(
        string key,
        byte[] value,
        DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        token.ThrowIfCancellationRequested();
        await EnsureContainerAsync(token).ConfigureAwait(false);

        var now = _timeProvider.GetUtcNow();
        var expiration = BlobCacheEntryExpiration.Compute(options, now);
        var uploadOptions = new BlobUploadOptions { Metadata = BlobCacheEntryExpiration.ToMetadata(expiration) };

        var blob = _container.GetBlobClient(BlobName(key));
        using var stream = new MemoryStream(value, writable: false);
        await blob.UploadAsync(stream, uploadOptions, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Refresh(string key) => RefreshAsync(key).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task RefreshAsync(string key, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        token.ThrowIfCancellationRequested();
        await EnsureContainerAsync(token).ConfigureAwait(false);

        var blob = _container.GetBlobClient(BlobName(key));

        BlobProperties properties;
        try
        {
            properties = (await blob.GetPropertiesAsync(cancellationToken: token).ConfigureAwait(false)).Value;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return;
        }

        var expiration = BlobCacheEntryExpiration.FromMetadata(properties.Metadata);
        var now = _timeProvider.GetUtcNow();
        if (BlobCacheEntryExpiration.IsExpired(expiration, now))
        {
            await TryDeleteAsync(blob, token).ConfigureAwait(false);
            return;
        }

        await TrySlideAsync(blob, expiration, now, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Remove(string key) => RemoveAsync(key).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        token.ThrowIfCancellationRequested();
        await EnsureContainerAsync(token).ConfigureAwait(false);

        var blob = _container.GetBlobClient(BlobName(key));
        await blob.DeleteIfExistsAsync(cancellationToken: token).ConfigureAwait(false);
    }

    private string BlobName(string key) => BlobCacheKeyMap.ToBlobName(_keyPrefix, key);

    private async Task TrySlideAsync(
        BlobClient blob,
        BlobCacheEntryExpiration.Values expiration,
        DateTimeOffset now,
        CancellationToken token)
    {
        var slid = BlobCacheEntryExpiration.Slide(expiration, now);
        if (slid is null)
        {
            return;
        }

        var updated = BlobCacheEntryExpiration.ToMetadata(expiration with { Effective = slid });
        try
        {
            await blob.SetMetadataAsync(updated, cancellationToken: token).ConfigureAwait(false);
        }
        catch (RequestFailedException)
        {
            // Best-effort sliding renewal: the entry may have been concurrently
            // removed or rewritten. A lost slide only shortens the window, never
            // corrupts the entry, so the read still returns the current value.
        }
    }

    private static async Task TryDeleteAsync(BlobClient blob, CancellationToken token)
    {
        try
        {
            await blob.DeleteIfExistsAsync(cancellationToken: token).ConfigureAwait(false);
        }
        catch (RequestFailedException)
        {
            // Best-effort eviction of an expired entry; a concurrent delete or a
            // transient failure is harmless because the entry already read as a
            // miss.
        }
    }

    private async Task EnsureContainerAsync(CancellationToken cancellationToken)
    {
        if (_containerReady)
        {
            return;
        }

        await _initGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_containerReady)
            {
                return;
            }

            await _container.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            _containerReady = true;
        }
        finally
        {
            _initGate.Release();
        }
    }
}
