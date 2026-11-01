namespace Orleans.Lattice.Api.Data;

/// <summary>
/// The data-plane capability probe: whether the caller may write or delete in a
/// tree, or at one key of it, answered from the same access gate the gated
/// <see cref="ILattice"/> write path consults, with no side effects.
/// </summary>
internal sealed partial class LatticeDataApi
{
    /// <inheritdoc />
    public async Task<LatticeDataCapabilities> ProbeCapabilitiesAsync(
        string treeId,
        string? key = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        string effectiveTreeId;
        try
        {
            // Composed first, so the probe answers for the tree a write would actually land on.
            effectiveTreeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeTenantAccessDeniedException)
        {
            // Fail closed: a caller refused its tenant may write nothing anywhere.
            return new LatticeDataCapabilities { TreeId = treeId, Key = key };
        }

        var canWrite = await IsAllowedAsync(effectiveTreeId, LatticeOperation.Write, key, cancellationToken).ConfigureAwait(false);
        var canDelete = await IsAllowedAsync(effectiveTreeId, LatticeOperation.Delete, key, cancellationToken).ConfigureAwait(false);
        return new LatticeDataCapabilities { TreeId = treeId, Key = key, CanWrite = canWrite, CanDelete = canDelete };
    }

    /// <summary>
    /// Mirrors the point enforcement the gated write path applies: a system-origin
    /// turn or the no-op gate allows, otherwise the gate's decision decides, with any
    /// per-key filter applied to the key. Without a key, a subject holding any allow
    /// grant for the operation (whole-tree, prefix or exact-key) is allowed.
    /// </summary>
    private async ValueTask<bool> IsAllowedAsync(
        string treeId,
        LatticeOperation operation,
        string? key,
        CancellationToken cancellationToken)
    {
        var gate = _accessGate;
        if (LatticeAccessGateContext.IsGateBypassed || gate is null or NullLatticeAccessGate)
        {
            return true;
        }

        var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(_membership, cancellationToken).ConfigureAwait(false);
        if (key is null)
        {
            if (subject.IsAnonymous)
            {
                return false;
            }

            if (_grantProbe is { } probe)
            {
                return await probe.HasAnyGrantAsync(treeId, subject, operation, cancellationToken).ConfigureAwait(false);
            }

            var tree = new LatticeAccessRequest(treeId, operation, subject);
            return (await gate.AuthorizeAsync(in tree, cancellationToken).ConfigureAwait(false)).Allowed;
        }

        var request = new LatticeAccessRequest(treeId, operation, subject, key);
        var decision = await gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        return decision.Allowed && (decision.KeyFilter is null || decision.KeyFilter(key));
    }
}
