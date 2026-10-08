using System.Diagnostics.CodeAnalysis;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using NSubstitute;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// An in-memory stand-in for a blob container, exposed as a real
/// <see cref="BlobContainerClient"/> whose virtual members are substituted onto a
/// dictionary of blobs. Each entry carries the three things the cache actually
/// depends on - its content, its metadata, and an <see cref="ETag"/> that changes
/// on every write - and the conditional semantics the cache relies on are modelled
/// faithfully: a read of a missing blob raises 404, and a conditional write whose
/// <c>BlobRequestConditions.IfMatch</c> no longer matches raises 412.
/// <para>
/// This exists because every behavioural fixture for
/// <see cref="AzureBlobDistributedCache"/> is <c>[Category("AzureStorageEmulator")]</c>
/// and is therefore excluded from the default test run, leaving the cache's own
/// orchestration - lazy container creation, expiry-on-read eviction, sliding
/// renewal, and the 404 miss arms - unexecuted unless a developer happens to have
/// Azurite running. Modelling the container in memory moves all of it into the
/// default suite while keeping the assertions behavioural (a value round-trips, an
/// expired entry reads as a miss and is evicted) rather than merely structural.
/// </para>
/// <para>
/// It deliberately does not emulate the blob REST API at the transport layer. The
/// cache consumes the SDK's typed surface, so substituting that surface tests the
/// code under test without also re-implementing storage's wire protocol - the
/// emulator-gated fixtures remain the end-to-end proof.
/// </para>
/// </summary>
internal sealed class FakeBlobStore
{
    private readonly Dictionary<string, Entry> _blobs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, BlobClient> _clients = new(StringComparer.Ordinal);
    private int _etagSeed;

    /// <summary>One stored blob: its bytes, its metadata, and its current tag.</summary>
    private sealed record Entry(byte[] Content, IDictionary<string, string> Metadata, ETag ETag);

    /// <summary>How many times the container's create-if-not-exists call was made.</summary>
    public int ContainerCreateCalls { get; private set; }

    /// <summary>How many times a conditional metadata write was attempted.</summary>
    public int SetMetadataCalls { get; private set; }

    /// <summary>How many times a delete was attempted, conditional or not.</summary>
    public int DeleteCalls { get; private set; }

    /// <summary>How many times a blob's content was downloaded.</summary>
    public int DownloadCalls { get; private set; }

    /// <summary>
    /// When set, called before each metadata write; returning an exception fails
    /// that write instead of applying it. Used to drive the best-effort slide's
    /// swallowed-failure arm.
    /// </summary>
    public Func<string, RequestFailedException?>? OnSetMetadata { get; set; }

    /// <summary>
    /// When set, called before each delete; returning an exception fails that
    /// delete. Used to drive the best-effort eviction's swallowed-failure arm.
    /// </summary>
    public Func<string, RequestFailedException?>? OnDelete { get; set; }

    /// <summary>The blob names currently present, in insertion order.</summary>
    public IReadOnlyCollection<string> Names => _blobs.Keys;

    /// <summary>True when a blob with <paramref name="name"/> is present.</summary>
    public bool Exists(string name) => _blobs.ContainsKey(name);

    /// <summary>The metadata currently stored against <paramref name="name"/>.</summary>
    public IDictionary<string, string> MetadataOf(string name) => _blobs[name].Metadata;

    /// <summary>Seeds a blob directly, bypassing the cache's own write path.</summary>
    public void Seed(string name, byte[] content, IDictionary<string, string> metadata) =>
        _blobs[name] = new Entry(content, new Dictionary<string, string>(metadata, StringComparer.Ordinal), NextETag());

    private ETag NextETag() => new($"\"etag-{++_etagSeed}\"");

    private static RequestFailedException NotFound() =>
        new(404, "The specified blob does not exist.", BlobErrorCode.BlobNotFound.ToString(), innerException: null);

    private static RequestFailedException PreconditionFailed() =>
        new(412, "The condition specified using HTTP conditional header(s) is not met.", "ConditionNotMet", innerException: null);

    private static Response<T> Ok<T>(T value) => Response.FromValue(value, Substitute.For<Response>());

    /// <summary>
    /// Builds the container client the cache is constructed over. Every blob name
    /// resolves to a stable client instance, so a test can assert against the same
    /// substitute the cache used.
    /// </summary>
    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Substitutes are owned by the test fixture and have no unmanaged state.")]
    public BlobContainerClient CreateContainerClient()
    {
        var container = Substitute.For<BlobContainerClient>();

        container.CreateIfNotExistsAsync(
                Arg.Any<PublicAccessType>(),
                Arg.Any<IDictionary<string, string>>(),
                Arg.Any<BlobContainerEncryptionScopeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                ContainerCreateCalls++;
                return Task.FromResult(Ok(BlobsModelFactory.BlobContainerInfo(new ETag("\"container\""), DateTimeOffset.UnixEpoch)));
            });

        container.GetBlobClient(Arg.Any<string>()).Returns(call => GetOrCreateClient(call.Arg<string>()));

        return container;
    }

    private BlobClient GetOrCreateClient(string name)
    {
        if (_clients.TryGetValue(name, out var existing))
        {
            return existing;
        }

        var client = BuildClient(name);
        _clients[name] = client;
        return client;
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Substitutes are owned by the test fixture and have no unmanaged state.")]
    private BlobClient BuildClient(string name)
    {
        var blob = Substitute.For<BlobClient>();

        blob.DownloadContentAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            DownloadCalls++;
            if (!_blobs.TryGetValue(name, out var entry))
            {
                return Task.FromException<Response<BlobDownloadResult>>(NotFound());
            }

            var details = BlobsModelFactory.BlobDownloadDetails(
                metadata: new Dictionary<string, string>(entry.Metadata, StringComparer.Ordinal),
                eTag: entry.ETag);
            return Task.FromResult(Ok(BlobsModelFactory.BlobDownloadResult(BinaryData.FromBytes(entry.Content), details)));
        });

        blob.GetPropertiesAsync(Arg.Any<BlobRequestConditions>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            if (!_blobs.TryGetValue(name, out var entry))
            {
                return Task.FromException<Response<BlobProperties>>(NotFound());
            }

            var properties = BlobsModelFactory.BlobProperties(
                metadata: new Dictionary<string, string>(entry.Metadata, StringComparer.Ordinal),
                eTag: entry.ETag,
                smartAccessTier: null);
            return Task.FromResult(Ok(properties));
        });

        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            var stream = call.Arg<Stream>();
            var options = call.Arg<BlobUploadOptions>();
            using var buffer = new MemoryStream();
            stream.CopyTo(buffer);

            var metadata = options.Metadata is null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(options.Metadata, StringComparer.Ordinal);
            _blobs[name] = new Entry(buffer.ToArray(), metadata, NextETag());

            return Task.FromResult(Ok(BlobsModelFactory.BlobContentInfo(_blobs[name].ETag, DateTimeOffset.UnixEpoch, [], null, null, null, 0)));
        });

        blob.SetMetadataAsync(
                Arg.Any<IDictionary<string, string>>(),
                Arg.Any<BlobRequestConditions>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                SetMetadataCalls++;

                var injected = OnSetMetadata?.Invoke(name);
                if (injected is not null)
                {
                    return Task.FromException<Response<BlobInfo>>(injected);
                }

                if (!_blobs.TryGetValue(name, out var entry))
                {
                    return Task.FromException<Response<BlobInfo>>(NotFound());
                }

                var conditions = call.Arg<BlobRequestConditions>();
                if (conditions?.IfMatch is { } required && required != entry.ETag)
                {
                    return Task.FromException<Response<BlobInfo>>(PreconditionFailed());
                }

                var updated = new Dictionary<string, string>(call.Arg<IDictionary<string, string>>(), StringComparer.Ordinal);
                var tag = NextETag();
                _blobs[name] = entry with { Metadata = updated, ETag = tag };
                return Task.FromResult(Ok(BlobsModelFactory.BlobInfo(tag, DateTimeOffset.UnixEpoch)));
            });

        blob.DeleteIfExistsAsync(
                Arg.Any<DeleteSnapshotsOption>(),
                Arg.Any<BlobRequestConditions>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                DeleteCalls++;

                var injected = OnDelete?.Invoke(name);
                if (injected is not null)
                {
                    return Task.FromException<Response<bool>>(injected);
                }

                if (!_blobs.TryGetValue(name, out var entry))
                {
                    return Task.FromResult(Ok(false));
                }

                var conditions = call.Arg<BlobRequestConditions>();
                if (conditions?.IfMatch is { } required && required != entry.ETag)
                {
                    return Task.FromException<Response<bool>>(PreconditionFailed());
                }

                _blobs.Remove(name);
                return Task.FromResult(Ok(true));
            });

        return blob;
    }
}
