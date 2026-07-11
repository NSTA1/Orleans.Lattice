namespace Orleans.Lattice.Schema;

/// <summary>
/// The pure merge-result decision over a compiled policy: it validates a merged
/// value and, on failure, surfaces a non-mutating
/// <see cref="LatticeMergeOutcome.AcceptWithEvent"/> annotation. It never rejects
/// or transforms, so CRDT convergence is never blocked or perturbed. Factored out
/// of <see cref="LatticeSchemaMergeObserver"/> so the decision is unit-testable
/// without a leaf grain.
/// </summary>
internal static class LatticeSchemaMergeValidation
{
    /// <summary>
    /// Evaluates <paramref name="policy"/> against the merged value in
    /// <paramref name="ctx"/>. Returns <see cref="LatticeMergeOutcome.Accept()"/>
    /// when the merged value is valid; otherwise
    /// <see cref="LatticeMergeOutcome.AcceptWithEvent"/> carrying the failure
    /// reason.
    /// </summary>
    /// <param name="policy">The compiled policy governing the merged key's tree.</param>
    /// <param name="ctx">The completed merge's context.</param>
    /// <returns>The non-blocking merge outcome.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    public static LatticeMergeOutcome Evaluate(CompiledSchemaPolicy policy, in LatticeMergeContext ctx)
    {
        ArgumentNullException.ThrowIfNull(policy);
        return policy.Validate(ctx.MergedValue) is { } reason
            ? LatticeMergeOutcome.AcceptWithEvent(reason)
            : LatticeMergeOutcome.Accept();
    }
}
