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
/// <item><description>dead-letters an invalid <b>strict-ingest</b> item (replication apply / restore) so ingest never blocks.</description></item>
/// </list>
/// </summary>
/// <remarks>
/// <para>
/// <b>Zero overhead when off.</b> A tree with no policy resolves to a cached
/// <c>null</c> and returns the singleton <see cref="LatticeWriteDecision.Accept()"/>
/// with no allocation. System-origin (ingest) traffic is inspected only when
/// strict mode is globally enabled via <see cref="InterceptsSystemOrigin"/>.
/// </para>
/// <para>
/// <b>CRDT deltas.</b> A <see cref="LatticeOperation.CrdtApply"/> delta is
/// validated only when it is shape-checkable (parses as JSON); an opaque delta is
/// accepted here and any merge-result violation is left to
/// <see cref="LatticeSchemaMergeObserver"/>, so convergence is never blocked.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaWriteInterceptor : ILatticeWriteInterceptor
{
    private readonly ILatticeSchemaPolicyProvider _provider;
    private readonly ILatticeSchemaDeadLetterStore _deadLetters;
    private readonly TimeProvider _timeProvider;
    private readonly int _previewMaxBytes;

    /// <summary>Initializes a new <see cref="LatticeSchemaWriteInterceptor"/>.</summary>
    /// <param name="provider">Resolves the cached per-tree policy.</param>
    /// <param name="deadLetters">The dead-letter store strict-ingest diversions are appended to.</param>
    /// <param name="options">The enforcement options carrying the preview byte bound.</param>
    /// <param name="timeProvider">The clock used to stamp dead-letter entries.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeSchemaWriteInterceptor(
        ILatticeSchemaPolicyProvider provider,
        ILatticeSchemaDeadLetterStore deadLetters,
        IOptions<LatticeSchemaEnforcementOptions> options,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(deadLetters);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _provider = provider;
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
        var compiled = await _provider.GetCompiledPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (compiled is null)
        {
            // No policy governs this tree: accept with no per-write allocation.
            return LatticeWriteDecision.Accept();
        }

        // A CRDT delta is only validated when it is shape-checkable (JSON). An
        // opaque delta is accepted here; a merge-result violation is surfaced by
        // the merge observer instead of blocking convergence.
        if (operation == LatticeOperation.CrdtApply && !SchemaValueChecks.IsWellFormedJson(value))
        {
            return LatticeWriteDecision.Accept();
        }

        var reason = compiled.Validate(value);
        if (reason is null)
        {
            return LatticeWriteDecision.Accept();
        }

        if (!LatticeAccessGateContext.IsSystemOrigin)
        {
            // Local write: fail closed. Nothing is durable before this throw.
            throw new LatticeSchemaViolationException(treeId, key, reason);
        }

        // System-origin ingest (strict globally on). Honour the per-tree strict
        // flag: dead-letter when strict, otherwise trust the item as-is.
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
