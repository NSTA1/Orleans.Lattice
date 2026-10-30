using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-enforcement <see cref="ILatticeWriteInterceptor"/>. Consulted at the
/// <c>LatticeGrain</c> write choke point after authorization and before WAL
/// append, it validates the incoming value against the tree's cached
/// <see cref="CompiledSchemaPolicy"/> and:
/// <list type="bullet">
/// <item><description>accepts when the tree has no policy (zero-overhead) or the value is valid;</description></item>
/// <item><description>throws <see cref="LatticeSchemaViolationException"/> for an invalid <b>local</b> write (fail-closed);</description></item>
/// <item><description>dead-letters an invalid <b>strict-ingest</b> item from an intercepted system-origin write so that write never blocks.</description></item>
/// </list>
/// </summary>
/// <remarks>
/// <para>
/// <b>Zero overhead when off.</b> A tree with no policy resolves to a cached
/// <c>null</c> and returns the singleton <see cref="LatticeWriteDecision.Accept()"/>
/// with no allocation. System-origin traffic that reaches the write choke point
/// is inspected only when strict mode is globally enabled via
/// <see cref="InterceptsSystemOrigin"/>.
/// </para>
/// <para>
/// <b>CRDT deltas.</b> A <see cref="LatticeOperation.CrdtApply"/> delta is
/// validated only when it is shape-checkable (parses as JSON); an opaque delta is
/// accepted here and any merge-result violation is left to
/// <see cref="LatticeSchemaMergeObserver"/>, so convergence is never blocked.
/// </para>
/// <para>
/// <b>Already-enveloped values.</b> A policy judges the body, never the
/// schema-version envelope. A value that arrives already enveloped (a replicated
/// entry or CRDT delta, or a re-write of stored bytes) is judged by what the
/// registered <see cref="ILatticeValueDecoder"/> serves a reader: its body, upcast
/// to the tree's target. One that cannot be decoded to the target shape is a
/// violation. The value is admitted with its envelope unchanged. With no active
/// decoder no read strips an envelope, so the whole value is judged.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaWriteInterceptor : ILatticeWriteInterceptor
{
    private readonly ILatticeSchemaPolicyProvider _provider;
    private readonly ILatticeSchemaDeadLetterStore _deadLetters;
    private readonly TimeProvider _timeProvider;
    private readonly int _previewMaxBytes;
    private readonly ILatticeValueDecoder? _valueDecoder;

    /// <summary>Initializes a new <see cref="LatticeSchemaWriteInterceptor"/>.</summary>
    /// <param name="provider">Resolves the cached per-tree policy.</param>
    /// <param name="deadLetters">The dead-letter store strict-ingest diversions are appended to.</param>
    /// <param name="options">The enforcement options carrying the preview byte bound.</param>
    /// <param name="timeProvider">The clock used to stamp dead-letter entries.</param>
    /// <param name="valueDecoder">
    /// The silo's read-path value decoder, used to recover the reader-visible body of
    /// an already-enveloped value, or <c>null</c> when none is registered (the value
    /// is then judged whole).
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="provider"/>, <paramref name="deadLetters"/>, <paramref name="options"/>,
    /// or <paramref name="timeProvider"/> is <c>null</c>.
    /// </exception>
    public LatticeSchemaWriteInterceptor(
        ILatticeSchemaPolicyProvider provider,
        ILatticeSchemaDeadLetterStore deadLetters,
        IOptions<LatticeSchemaEnforcementOptions> options,
        TimeProvider timeProvider,
        ILatticeValueDecoder? valueDecoder = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(deadLetters);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _provider = provider;
        _deadLetters = deadLetters;
        _timeProvider = timeProvider;
        _previewMaxBytes = Math.Max(1, options.Value.DeadLetterPreviewMaxBytes);
        _valueDecoder = valueDecoder;
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
        var compiled = await _provider.GetCompiledPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (compiled is null)
        {
            // No policy governs this tree: accept with no per-write allocation.
            return LatticeWriteDecision.Accept();
        }

        string? reason;
        var judged = value;

        // An already-enveloped value (a replicated entry or delta, or a re-write of
        // stored bytes) is judged by the bytes a reader is served: the body, upcast
        // to the tree's target. Only the read decoder decides that, so with no active
        // decoder (versioning not registered) nothing strips the header on read and
        // the whole value is judged, which keeps an envelope-shaped prefix from
        // bypassing the policy. A plain value is judged as-is with no extra work.
        if (LatticeSchemaEnvelope.IsEnveloped(value)
            && _valueDecoder is { } decoder
            && decoder.IsActive(treeId))
        {
            try
            {
                judged = await decoder.DecodeAsync(treeId, value, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException)
            {
                // No reader can be served this value at the target shape (stamped
                // newer than the target, no upcast path, or an upcast that cannot run
                // over the body), so its body cannot be judged: fail closed.
                reason = $"The value's version envelope cannot be decoded to the tree's target shape. {ex.Message}";
                return await ViolationAsync(compiled, treeId, key, value, operation, reason, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        // A CRDT delta is only validated when it is shape-checkable (JSON). An
        // opaque delta is accepted here; a merge-result violation is surfaced by
        // the merge observer instead of blocking convergence.
        if (operation == LatticeOperation.CrdtApply && !SchemaValueChecks.IsWellFormedJson(judged))
        {
            return LatticeWriteDecision.Accept();
        }

        reason = compiled.Validate(judged);
        if (reason is null)
        {
            return LatticeWriteDecision.Accept();
        }

        return await ViolationAsync(compiled, treeId, key, value, operation, reason, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<LatticeWriteDecision> ViolationAsync(
        CompiledSchemaPolicy compiled,
        string treeId,
        string key,
        byte[] value,
        LatticeOperation operation,
        string reason,
        CancellationToken cancellationToken)
    {
        if (!LatticeAccessGateContext.IsSystemOrigin)
        {
            // Local write: fail closed. Nothing is durable before this throw.
            throw new LatticeSchemaViolationException(treeId, key, reason);
        }

        // Intercepted system-origin write (strict globally on). Honour the per-tree
        // strict flag: dead-letter when strict, otherwise trust the item as-is.
        if (!compiled.StrictIngest)
        {
            return LatticeWriteDecision.Accept();
        }

        var entry = BuildDeadLetterEntry(key, value, reason, SourceFor(operation));
        await _deadLetters.AppendAsync(treeId, entry, cancellationToken).ConfigureAwait(false);
        return LatticeWriteDecision.DeadLetter(reason);
    }

    private LatticeSchemaDeadLetterEntry BuildDeadLetterEntry(
        string key, byte[] value, string reason, LatticeSchemaDeadLetterSource source)
    {
        var previewLength = Math.Min(value.Length, _previewMaxBytes);
        var preview = previewLength == 0 ? Array.Empty<byte>() : value.AsSpan(0, previewLength).ToArray();
        return new LatticeSchemaDeadLetterEntry(
            key, preview, value.Length, reason, source, _timeProvider.GetUtcNow());
    }

    private static LatticeSchemaDeadLetterSource SourceFor(LatticeOperation operation) =>
        (operation & (LatticeOperation.Restore | LatticeOperation.BulkLoad)) != 0
            ? LatticeSchemaDeadLetterSource.Restore
            : LatticeSchemaDeadLetterSource.Replication;
}
