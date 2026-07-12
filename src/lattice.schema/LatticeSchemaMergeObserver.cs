namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-enforcement <see cref="ILatticeMergeObserver"/>. After a per-key
/// CRDT / LWW merge completes, it validates the merged value against the tree's
/// policy and, on a violation, surfaces a non-mutating
/// <see cref="LatticeMergeOutcome.AcceptWithEvent"/> annotation. It never rejects
/// or transforms, so convergence is never blocked or perturbed - the observer is
/// the safety net for violations that a pre-merge delta check cannot catch (for
/// example two individually-valid deltas that merge into an invalid combined
/// value).
/// </summary>
/// <remarks>
/// <para>
/// <b>Opt-in.</b> Registered only when
/// <see cref="LatticeSchemaEnforcementOptions.ValidateCrdtMergeResults"/> is set,
/// so the merge path keeps its zero-overhead default.
/// </para>
/// <para>
/// <b>Tree resolution.</b> The core merge seam stamps the tree id onto
/// <see cref="LatticeMergeContext.TreeId"/>, so the observer resolves the per-tree
/// policy directly from the context in production. It still falls back to the
/// ambient <see cref="LatticeSchemaMergeTree"/> scope when the context carries no
/// tree id (tests that drive the observer directly). Absent both, it accepts every
/// merge, never blocking convergence.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaMergeObserver(ILatticeSchemaPolicyProvider provider) : ILatticeMergeObserver
{
    /// <inheritdoc />
    public ValueTask<LatticeMergeOutcome> OnMergedAsync(in LatticeMergeContext ctx, CancellationToken ct)
    {
        // Prefer the tree id the core merge seam now stamps onto the context; fall
        // back to the ambient scope for tests that drive the observer directly.
        // Absent both, there is nothing to validate against, so accept (never block
        // convergence).
        var treeId = ctx.TreeId ?? LatticeSchemaMergeTree.Current;
        if (treeId is null)
        {
            return new ValueTask<LatticeMergeOutcome>(LatticeMergeOutcome.Accept());
        }

        return OnMergedCoreAsync(treeId, ctx.Key, ctx.Mode, ctx.LocalValue, ctx.IncomingValue, ctx.MergedValue, ct);
    }

    private async ValueTask<LatticeMergeOutcome> OnMergedCoreAsync(
        string treeId,
        string key,
        LatticeMergeMode mode,
        byte[]? localValue,
        byte[]? incomingValue,
        byte[] mergedValue,
        CancellationToken ct)
    {
        var compiled = await provider.GetCompiledPolicyAsync(treeId, ct).ConfigureAwait(false);
        if (compiled is null)
        {
            return LatticeMergeOutcome.Accept();
        }

        var rebuilt = new LatticeMergeContext(key, mode, localValue, incomingValue, mergedValue, treeId);
        return LatticeSchemaMergeValidation.Evaluate(compiled, in rebuilt);
    }
}
