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
/// <b>Wiring limitation.</b> The core merge seam (#1198) does not carry the tree
/// id in <see cref="LatticeMergeContext"/>, so the observer resolves the tree
/// through the ambient <see cref="LatticeSchemaMergeTree"/> scope. Until a core
/// hook stamps that scope around leaf merges, the observer accepts every merge in
/// production; the decision logic is nonetheless exercised end-to-end in tests
/// that enter the scope. Adding the tree id to the merge context is the follow-up
/// that completes this path.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaMergeObserver(ILatticeSchemaPolicyProvider provider) : ILatticeMergeObserver
{
    /// <inheritdoc />
    public ValueTask<LatticeMergeOutcome> OnMergedAsync(in LatticeMergeContext ctx, CancellationToken ct)
    {
        // The merge context carries the key but not the tree; resolve the tree
        // from the ambient scope. Absent it, there is nothing to validate against,
        // so accept (never block convergence).
        if (LatticeSchemaMergeTree.Current is not { } treeId)
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

        var rebuilt = new LatticeMergeContext(key, mode, localValue, incomingValue, mergedValue);
        return LatticeSchemaMergeValidation.Evaluate(compiled, in rebuilt);
    }
}
