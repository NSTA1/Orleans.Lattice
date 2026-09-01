using System.Runtime.CompilerServices;
using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// The default <see cref="IGrainIndexEnrollmentStore"/>, backed by the same
/// internal registry <see cref="ILattice"/> tree that holds the persisted index
/// definitions.
/// </summary>
/// <remarks>
/// <para>
/// One tree, three key kinds. The seen markers live under
/// <see cref="GrainIndexRegistryKeys.Seen(string, string)"/> and the outbox
/// under <see cref="GrainIndexRegistryKeys.Pending(string, string)"/>, so a
/// backfill listing an index's enrolled grains and a drain listing outstanding
/// writes are each one contiguous range read that cannot see the other kind.
/// </para>
/// <para>
/// The tree reference is resolved once, in the constructor: this store sits on
/// the mutation path, where a per-call <c>GetGrain</c> would be pure overhead.
/// The system-origin scope is entered only when one is not already active, so a
/// caller that hoists a single scope over a whole enrolment - which the enroller
/// does - pays for one scope object rather than one per tree call.
/// </para>
/// </remarks>
internal sealed class GrainIndexEnrollmentStore : IGrainIndexEnrollmentStore
{
    private readonly ILattice _registry;
    private readonly ILatticeSerializer<GrainIndexEnrollmentRecord> _enrollmentSerializer;
    private readonly ILatticeSerializer<GrainIndexPendingProjection> _pendingSerializer;

    /// <summary>Initialises a new store.</summary>
    /// <param name="grainFactory">Opens the registry tree. Must not be <c>null</c>.</param>
    /// <param name="enrollmentSerializer">Encodes and decodes seen markers. Must not be <c>null</c>.</param>
    /// <param name="pendingSerializer">Encodes and decodes outbox entries. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexEnrollmentStore(
        IGrainFactory grainFactory,
        OrleansGrainIndexSerializer<GrainIndexEnrollmentRecord> enrollmentSerializer,
        OrleansGrainIndexSerializer<GrainIndexPendingProjection> pendingSerializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(enrollmentSerializer);
        ArgumentNullException.ThrowIfNull(pendingSerializer);

        _registry = grainFactory.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);
        _enrollmentSerializer = enrollmentSerializer;
        _pendingSerializer = pendingSerializer;
    }

    /// <inheritdoc />
    public async Task<GrainIndexEnrollmentRecord?> ReadEnrollmentAsync(
        string indexName,
        string grainKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(grainKey);

        using var scope = EnterSystemOrigin();
        var bytes = await _registry
            .GetAsync(GrainIndexRegistryKeys.Seen(indexName, grainKey), cancellationToken)
            .ConfigureAwait(true);

        return bytes is null ? null : _enrollmentSerializer.Deserialize(bytes);
    }

    /// <inheritdoc />
    public async Task WritePendingAsync(
        GrainIndexPendingProjection pending,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(pending);

        using var scope = EnterSystemOrigin();
        await _registry
            .SetAsync(
                GrainIndexRegistryKeys.Pending(pending.IndexName, pending.GrainKey),
                _pendingSerializer.Serialize(pending),
                cancellationToken)
            .ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task CompleteAsync(
        string indexName,
        string grainKey,
        GrainIndexProjection projection,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(grainKey);
        ArgumentNullException.ThrowIfNull(projection);

        var marker = new List<KeyValuePair<string, byte[]>>(1)
        {
            new(
                GrainIndexRegistryKeys.Seen(indexName, grainKey),
                _enrollmentSerializer.Serialize(new GrainIndexEnrollmentRecord(projection))),
        };

        using var scope = EnterSystemOrigin();
        await _registry
            .SetManyAtomicAsync(
                marker,
                new[] { GrainIndexRegistryKeys.Pending(indexName, grainKey) },
                NewOperationId(),
                cancellationToken)
            .ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task WithdrawAsync(string indexName, string grainKey, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(grainKey);

        using var scope = EnterSystemOrigin();
        await _registry
            .SetManyAtomicAsync(
                [],
                new[]
                {
                    GrainIndexRegistryKeys.Seen(indexName, grainKey),
                    GrainIndexRegistryKeys.Pending(indexName, grainKey),
                },
                NewOperationId(),
                cancellationToken)
            .ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ScanSeenKeysAsync(
        string indexName,
        string firstKeyInclusive,
        string lastKeyInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(firstKeyInclusive);
        ArgumentNullException.ThrowIfNull(lastKeyInclusive);

        var prefix = GrainIndexRegistryKeys.SeenPrefix(indexName);

        // Appending U+0000 to the inclusive upper bound yields the smallest
        // string ordinally greater than it, which turns the caller's inclusive
        // range into the half-open one the tree scans.
        var startInclusive = string.Concat(prefix, firstKeyInclusive);
        var endExclusive = string.Concat(prefix, lastKeyInclusive, "\u0000");

        // The scope has to span the whole enumeration rather than only its
        // first page, for the same reason the outbox scan's does: each
        // continuation is a fresh grain call.
        using var scope = EnterSystemOrigin();

        var keys = _registry.KeysAsync(
            startInclusive,
            endExclusive,
            cancellationToken: cancellationToken);

        await foreach (var key in keys.WithCancellation(cancellationToken).ConfigureAwait(true))
            yield return key[prefix.Length..];
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<GrainIndexPendingProjection> ScanPendingAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // The scope has to span the whole enumeration, not just its first page:
        // the tree is walked lazily, so each continuation is a fresh grain call
        // that the access gate would otherwise see without the system-origin
        // marker.
        using var scope = EnterSystemOrigin();

        var entries = _registry.EntriesAsync(
            GrainIndexRegistryKeys.PendingPrefix(),
            GrainIndexRegistryKeys.PendingPrefixEnd(),
            cancellationToken: cancellationToken);

        await foreach (var entry in entries.WithCancellation(cancellationToken).ConfigureAwait(true))
            yield return _pendingSerializer.Deserialize(entry.Value);
    }

    /// <summary>
    /// A fresh idempotency key for a registry batch. Registry batches are
    /// bookkeeping rather than user data and are always safe to re-run, so
    /// unlike an index batch they gain nothing from a stable key.
    /// </summary>
    private static string NewOperationId() => Guid.NewGuid().ToString("N");

    /// <summary>
    /// Enters a system-origin scope unless the caller already established one.
    /// The registry is index infrastructure: it must be readable and writable
    /// whatever caller identity, if any, the host's access gate would otherwise
    /// demand.
    /// </summary>
    private static IDisposable? EnterSystemOrigin() =>
        LatticeSystemOrigin.IsActive ? null : LatticeSystemOrigin.Enter();
}
