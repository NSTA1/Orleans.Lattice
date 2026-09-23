using System.Buffers;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.ContractProbes;

/// <summary>
/// Adapts any <see cref="IWalStorageProvider"/> to
/// <see cref="IWalStorageProviderContractProbe"/>, so every provider runs the
/// shared <see cref="WalStorageProviderContractTestsBase"/> suite through one
/// translation layer rather than three hand-written ones that could drift.
/// <para>
/// Compiled into the core test assembly and linked into each storage provider's
/// test assembly (the shared testing library cannot reference Orleans.Lattice).
/// </para>
/// </summary>
internal sealed class WalStorageProviderContractProbe : IWalStorageProviderContractProbe
{
    private static readonly HybridLogicalClock Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

    private readonly Func<IWalStorageProvider> _create;
    private readonly Func<IWalStorageProvider, CancellationToken, Task>? _durabilityBarrier;
    private readonly bool _durable;
    private readonly IWalRecordEncoder _encoder;
    private IWalStorageProvider _provider;

    /// <param name="create">Builds a provider over the fixture's storage; called again on every reopen of a durable provider.</param>
    /// <param name="serializer">Serializer backing the encoder used by the encoded paths.</param>
    /// <param name="durable">
    /// <see langword="true"/> when the provider persists state, so a reopen
    /// disposes it and rebuilds it over the same storage; <see langword="false"/>
    /// for a volatile provider, whose reopen is a no-op because the instance
    /// outliving its grain is exactly what a reactivation looks like.
    /// </param>
    /// <param name="durabilityBarrier">
    /// The barrier the provider's real caller crosses before treating an append
    /// as durable, if it has one (the Azure Table provider's phase-2 flush).
    /// </param>
    public WalStorageProviderContractProbe(
        Func<IWalStorageProvider> create,
        Serializer<WalRecord> serializer,
        bool durable,
        Func<IWalStorageProvider, CancellationToken, Task>? durabilityBarrier = null)
    {
        ArgumentNullException.ThrowIfNull(create);
        ArgumentNullException.ThrowIfNull(serializer);
        _create = create;
        _durable = durable;
        _durabilityBarrier = durabilityBarrier;
        _encoder = new OrleansBinaryWalRecordEncoder(serializer);
        _provider = create();
    }

    public async Task AppendAsync(string treeId, int shardIndex, IReadOnlyList<WalContractEntry> entries, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var walEntries = entries.Select(e => ToWalEntry(treeId, e)).ToArray();
        await _provider.AppendBatchAsync(treeId, shardIndex, walEntries, cancellationToken).ConfigureAwait(false);
        await CrossBarrierAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task AppendEncodedAsync(string treeId, int shardIndex, IReadOnlyList<WalContractEntry> entries, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var segments = entries.Select(e => Encode(treeId, e)).ToArray();
        var offsets = entries.Select(static e => e.Offset).ToArray();
        await _provider.AppendEncodedBatchAsync(treeId, shardIndex, segments, offsets, _encoder, cancellationToken).ConfigureAwait(false);
        await CrossBarrierAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task AppendEncodedWithMismatchedOffsetsAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
    {
        var segments = new[] { Encode(treeId, new WalContractEntry(0L, "k0", [0x01])) };
        var offsets = new[] { 0L, 1L };
        await _provider.AppendEncodedBatchAsync(treeId, shardIndex, segments, offsets, _encoder, cancellationToken).ConfigureAwait(false);
        await CrossBarrierAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<WalContractEntry>> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
    {
        var read = new List<WalContractEntry>();
        await foreach (var entry in _provider
            .ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken)
            .ConfigureAwait(false))
        {
            read.Add(new WalContractEntry(entry.Offset, entry.Mutation.Key, entry.Mutation.Value ?? []));
        }

        return read;
    }

    public async Task<WalContractEncodedPage> ReadEncodedAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
    {
        var page = await _provider
            .ReadEncodedAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, _encoder, cancellationToken)
            .ConfigureAwait(false);

        // The page is transient and provider-owned, so decode it before returning.
        var segments = page.EncodedEntries.Span;
        var offsets = page.Offsets.Span;
        if (segments.Length != offsets.Length)
        {
            throw new InvalidOperationException(
                $"Encoded page is not parallel: {segments.Length} segments against {offsets.Length} offsets.");
        }

        var entries = new WalContractEntry[segments.Length];
        for (var i = 0; i < segments.Length; i++)
        {
            var record = _encoder.Decode(segments[i].AsSpan(), treeId);
            entries[i] = new WalContractEntry(offsets[i], record.Key, record.Value ?? []);
        }

        return new WalContractEncodedPage(entries, page.HighestOffsetInclusive);
    }

    public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        _provider.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

    public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        _provider.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

    public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken) =>
        _provider.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);

    public Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        _provider.EvaluateCompactionAsync(treeId, shardIndex, cancellationToken);

    public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        _provider.ReconcileAsync(treeId, shardIndex, cancellationToken);

    public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        _provider.GetRetainedByteSizeAsync(treeId, shardIndex, cancellationToken);

    public Task<long> GetPhysicalByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
        _provider.GetPhysicalByteSizeAsync(treeId, shardIndex, cancellationToken);

    public async Task ReopenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_durable)
        {
            return;
        }

        await DisposeProviderAsync(_provider).ConfigureAwait(false);
        _provider = _create();
    }

    public ValueTask DisposeAsync() => DisposeProviderAsync(_provider);

    private Task CrossBarrierAsync(CancellationToken cancellationToken) =>
        _durabilityBarrier is null ? Task.CompletedTask : _durabilityBarrier(_provider, cancellationToken);

    private static async ValueTask DisposeProviderAsync(IWalStorageProvider provider)
    {
        switch (provider)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    // A null tree id is passed through to the provider untouched, but the payload
    // is stamped with a placeholder so building it cannot throw first and mask
    // whether the provider itself validates.
    private static WalEntry ToWalEntry(string treeId, WalContractEntry entry) => new()
    {
        Offset = entry.Offset,
        Mutation = new LatticeMutation
        {
            TreeId = treeId ?? string.Empty,
            Kind = MutationKind.Set,
            Key = entry.Key,
            Value = entry.Value,
            Timestamp = Timestamp,
            OriginClusterId = "site-a",
        },
    };

    private ArraySegment<byte> Encode(string treeId, WalContractEntry entry)
    {
        var record = new WalRecord
        {
            TreeId = treeId ?? string.Empty,
            Op = MutationKind.Set,
            Key = entry.Key,
            Value = entry.Value,
            Timestamp = Timestamp,
            OriginClusterId = "site-a",
        };

        var writer = new ArrayBufferWriter<byte>();
        _encoder.Encode(in record, writer);
        return new ArraySegment<byte>(writer.WrittenSpan.ToArray());
    }
}
