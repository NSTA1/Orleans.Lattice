namespace Orleans.Lattice;

/// <summary>
/// The shared enforcement primitive over an <see cref="ILatticeWriteInterceptor"/>.
/// It short-circuits the default no-op interceptor and the system-origin bypass,
/// constructs a <see cref="LatticeWriteRequest"/>, consults the interceptor, and
/// translates the returned <see cref="LatticeWriteDecision"/> into the effect the
/// <c>LatticeGrain</c> choke point applies: proceed with the original value,
/// proceed with a transformed value, drop (dead-letter), or throw
/// <see cref="LatticeWriteRejectedException"/> to the caller.
/// </summary>
/// <remarks>
/// <para>
/// <b>Zero-cost default.</b> <see cref="Skips"/> is <c>true</c> when the
/// registered interceptor is the default <see cref="NullLatticeWriteInterceptor"/>,
/// or when the turn is system-origin and the interceptor has not opted into
/// system-origin ingest via <see cref="ILatticeWriteInterceptor.InterceptsSystemOrigin"/>.
/// Call sites additionally gate on the null type by reference before invoking
/// any method here, so the default host never constructs a request, never calls
/// the interceptor, and never allocates on the write path.
/// </para>
/// <para>
/// <b>Atomic-batch semantics.</b> On the batch surface a
/// <see cref="LatticeWriteDecisionKind.Reject"/> always aborts, and a
/// <see cref="LatticeWriteDecisionKind.DeadLetter"/> aborts too when
/// <c>atomic</c> is <c>true</c> (an all-or-nothing commit cannot silently drop
/// one leg). A dead-letter on a non-atomic batch drops just that entry.
/// </para>
/// </remarks>
internal static class LatticeWriteInterceptorEnforcement
{
    /// <summary>
    /// <c>true</c> when interception can be skipped entirely for the current turn:
    /// the interceptor is the default no-op, or the turn is a system-origin call
    /// the interceptor has not opted into.
    /// </summary>
    public static bool Skips(ILatticeWriteInterceptor interceptor) =>
        interceptor is NullLatticeWriteInterceptor
        || (LatticeAccessGateContext.IsGateBypassed && !interceptor.InterceptsSystemOrigin);

    /// <summary>
    /// Applies the interceptor to a single-key write and returns the effect the
    /// choke point applies. Throws <see cref="LatticeWriteRejectedException"/> on
    /// a reject; returns a drop outcome on a dead-letter; returns a proceed
    /// outcome (original or transformed bytes) otherwise.
    /// </summary>
    public static async ValueTask<LatticeWriteInterceptionOutcome> InterceptPointAsync(
        ILatticeWriteInterceptor interceptor,
        string treeId,
        LatticeOperation operation,
        string key,
        byte[] value,
        TimeSpan? ttl,
        CancellationToken cancellationToken)
    {
        if (Skips(interceptor))
        {
            return LatticeWriteInterceptionOutcome.Write(value);
        }

        var request = new LatticeWriteRequest(treeId, key, value, operation, ttl);
        var decision = await interceptor.OnWriteAsync(in request, cancellationToken);
        return decision.Kind switch
        {
            LatticeWriteDecisionKind.Accept => LatticeWriteInterceptionOutcome.Write(value),
            LatticeWriteDecisionKind.AcceptTransformed =>
                LatticeWriteInterceptionOutcome.Write(TransformedOrThrow(in decision, treeId, operation, key)),
            LatticeWriteDecisionKind.DeadLetter => LatticeWriteInterceptionOutcome.Drop(),
            _ => throw Rejected(treeId, operation, key, decision.Reason),
        };
    }

    /// <summary>
    /// Applies the interceptor to every entry of a batch write, resolving the
    /// effective entry list once. Returns the same list reference when nothing
    /// changed (no allocation), otherwise a new list with transformed values
    /// substituted and, on a non-atomic batch, dead-lettered entries dropped.
    /// Throws <see cref="LatticeWriteRejectedException"/> on the first rejected
    /// entry (and on the first dead-letter when <paramref name="atomic"/> is
    /// <c>true</c>), aborting the whole batch before any write is applied.
    /// </summary>
    public static async ValueTask<List<KeyValuePair<string, byte[]>>> InterceptEntriesAsync(
        ILatticeWriteInterceptor interceptor,
        string treeId,
        LatticeOperation operation,
        List<KeyValuePair<string, byte[]>> entries,
        bool atomic,
        CancellationToken cancellationToken)
    {
        if (Skips(interceptor) || entries.Count == 0)
        {
            return entries;
        }

        List<KeyValuePair<string, byte[]>>? rewritten = null;
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var key = entry.Key;
            var value = entry.Value;
            var request = new LatticeWriteRequest(treeId, key, value, operation, ttl: null);
            var decision = await interceptor.OnWriteAsync(in request, cancellationToken);

            switch (decision.Kind)
            {
                case LatticeWriteDecisionKind.Accept:
                    rewritten?.Add(entry);
                    break;

                case LatticeWriteDecisionKind.AcceptTransformed:
                    rewritten ??= CopyPrefix(entries, i);
                    rewritten.Add(new KeyValuePair<string, byte[]>(
                        key, TransformedOrThrow(in decision, treeId, operation, key)));
                    break;

                case LatticeWriteDecisionKind.DeadLetter:
                    if (atomic)
                    {
                        throw Rejected(treeId, operation, key, decision.Reason);
                    }

                    // Non-atomic: drop this entry. Materialize the prefix we kept
                    // so far so the dropped index is excluded.
                    rewritten ??= CopyPrefix(entries, i);
                    break;

                default:
                    throw Rejected(treeId, operation, key, decision.Reason);
            }
        }

        return rewritten ?? entries;
    }

    private static List<KeyValuePair<string, byte[]>> CopyPrefix(
        List<KeyValuePair<string, byte[]>> entries, int count)
    {
        var copy = new List<KeyValuePair<string, byte[]>>(entries.Count);
        for (var i = 0; i < count; i++)
        {
            copy.Add(entries[i]);
        }

        return copy;
    }

    private static byte[] TransformedOrThrow(
        in LatticeWriteDecision decision, string treeId, LatticeOperation operation, string key)
    {
        // A transformed accept must carry replacement bytes. A null payload is a
        // misbehaving interceptor; fail closed rather than persist a null value.
        return decision.TransformedValue
            ?? throw new LatticeWriteRejectedException(
                treeId,
                operation,
                key,
                "The interceptor returned AcceptTransformed with no replacement value.");
    }

    private static LatticeWriteRejectedException Rejected(
        string treeId, LatticeOperation operation, string key, string? reason) =>
        new(treeId, operation, key, reason ?? "Rejected by the write interceptor.");
}
