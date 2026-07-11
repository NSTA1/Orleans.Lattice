using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-versioning <see cref="ILatticeWriteInterceptor"/>. Consulted at the
/// <c>LatticeGrain</c> write choke point after authorization and before WAL append,
/// it stamps the per-value schema-version envelope onto values written to an
/// opted-in tree, and (in strict mode) dead-letters an ingested item whose version
/// cannot be upcast to the tree's target.
/// </summary>
/// <remarks>
/// <para>
/// <b>Zero overhead when off.</b> A tree with no version config resolves to a cached
/// <c>null</c> and returns the singleton <see cref="LatticeWriteDecision.Accept()"/>
/// with no allocation, so an unversioned tree keeps its exact byte shape. Only an
/// opted-in tree pays the stamping allocation (one header-prefixed array per write),
/// which is intrinsic to making the value self-describing.
/// </para>
/// <para>
/// <b>Local writes.</b> A local <see cref="LatticeOperation.Write"/> to an opted-in
/// tree is stamped at the tree's current target version. An already-enveloped value
/// (for example a re-write of stored bytes) is accepted verbatim so it is never
/// double-stamped.
/// </para>
/// <para>
/// <b>Ingest trust model.</b> System-origin ingest (replication apply / restore) is
/// trusted by default and bypasses this interceptor, so an ingested item is stored
/// with whatever tag it carries. Strict mode (<see cref="InterceptsSystemOrigin"/>)
/// re-validates ingest: an enveloped item whose version is newer than the target,
/// or whose version cannot be upcast to the target, is dead-lettered rather than
/// applied, so ingest never blocks.
/// </para>
/// <para>
/// <b>CRDT scope (v1).</b> A <see cref="LatticeOperation.CrdtApply"/> delta is not
/// stamped: envelope versioning applies to whole-value writes, while CRDT
/// merge-input upcasting depends on the merge seam supplying per-record versioned
/// bytes (a documented follow-up on <see cref="LatticeMergeContext"/>). CRDT deltas
/// are therefore accepted verbatim here.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaVersionWriteInterceptor : ILatticeWriteInterceptor
{
    private readonly ILatticeSchemaVersionProvider _provider;
    private readonly ILatticeSchemaRegistry _registry;
    private readonly ILatticeSchemaDeadLetterStore _deadLetters;
    private readonly TimeProvider _timeProvider;
    private readonly int _previewMaxBytes;

    /// <summary>Initializes a new <see cref="LatticeSchemaVersionWriteInterceptor"/>.</summary>
    /// <param name="provider">Resolves the cached per-tree version config.</param>
    /// <param name="registry">The schema registry used to check upcastability in strict mode.</param>
    /// <param name="deadLetters">The dead-letter store strict-ingest diversions are appended to.</param>
    /// <param name="options">The versioning options carrying the preview byte bound.</param>
    /// <param name="timeProvider">The clock used to stamp dead-letter entries.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeSchemaVersionWriteInterceptor(
        ILatticeSchemaVersionProvider provider,
        ILatticeSchemaRegistry registry,
        ILatticeSchemaDeadLetterStore deadLetters,
        IOptions<LatticeSchemaVersioningOptions> options,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(deadLetters);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _provider = provider;
        _registry = registry;
        _deadLetters = deadLetters;
        _timeProvider = timeProvider;
        _previewMaxBytes = Math.Max(1, options.Value.DeadLetterPreviewMaxBytes);
    }

    /// <inheritdoc />
    public bool InterceptsSystemOrigin => _provider.StrictIngestEnabled;

    /// <inheritdoc />
    public ValueTask<LatticeWriteDecision> OnWriteAsync(
        in LatticeWriteRequest request,
        CancellationToken cancellationToken = default) =>
        OnWriteCoreAsync(request.TreeId, request.Key, request.Value, request.Operation, cancellationToken);

    private async ValueTask<LatticeWriteDecision> OnWriteCoreAsync(
        string treeId,
        string key,
        byte[] value,
        LatticeOperation operation,
        CancellationToken cancellationToken)
    {
        var config = await _provider.GetConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (config is not { } version)
        {
            // Unversioned tree: accept with no per-write allocation.
            return LatticeWriteDecision.Accept();
        }

        // An already-stamped value is never re-stamped (idempotent re-writes and any
        // value that arrived carrying a tag are honoured as-is).
        if (LatticeSchemaEnvelope.IsEnveloped(value))
        {
            return await OnEnvelopedAsync(treeId, key, value, version, cancellationToken).ConfigureAwait(false);
        }

        // CRDT deltas are not envelope-versioned in v1 (see the type remarks).
        if (operation == LatticeOperation.CrdtApply)
        {
            return LatticeWriteDecision.Accept();
        }

        // A local whole-value write to an opted-in tree is stamped at the target.
        // The header-prefixed array is the intrinsic cost of a self-describing value.
        return LatticeWriteDecision.AcceptTransformed(
            LatticeSchemaEnvelope.Encode(version.SchemaId, version.TargetVersion, value));
    }

    private async ValueTask<LatticeWriteDecision> OnEnvelopedAsync(
        string treeId,
        string key,
        byte[] value,
        LatticeSchemaVersionConfig version,
        CancellationToken cancellationToken)
    {
        // Only strict-mode ingest re-validates a tagged item; a local re-write of
        // already-enveloped bytes is always accepted verbatim.
        if (!LatticeAccessGateContext.IsSystemOrigin || !version.StrictIngest)
        {
            return LatticeWriteDecision.Accept();
        }

        _ = LatticeSchemaEnvelope.TryReadHeader(value, out var schemaId, out var storedVersion);

        // Trusted, upcastable ingest is stored with its own tag; read-time upcasting
        // brings it to the target when it is later read. Only an item newer than the
        // target, or one whose version cannot be upcast, is dead-lettered.
        if (schemaId == version.SchemaId
            && storedVersion <= version.TargetVersion
            && _registry.CanUpcast(version.SchemaId, storedVersion, version.TargetVersion))
        {
            return LatticeWriteDecision.Accept();
        }

        var reason =
            $"Strict-ingest: value at schema {schemaId} v{storedVersion} cannot be upcast to " +
            $"schema {version.SchemaId} v{version.TargetVersion}.";
        var entry = BuildDeadLetterEntry(key, value, reason);
        await _deadLetters.AppendAsync(treeId, entry, cancellationToken).ConfigureAwait(false);
        return LatticeWriteDecision.DeadLetter(reason);
    }

    private LatticeSchemaDeadLetterEntry BuildDeadLetterEntry(string key, byte[] value, string reason)
    {
        var previewLength = Math.Min(value.Length, _previewMaxBytes);
        var preview = previewLength == 0 ? Array.Empty<byte>() : value.AsSpan(0, previewLength).ToArray();
        return new LatticeSchemaDeadLetterEntry(
            key, preview, value.Length, reason, LatticeSchemaDeadLetterSource.Replication, _timeProvider.GetUtcNow());
    }
}
