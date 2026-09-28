using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps;

/// <summary>Applies replication intent to the same tenant-composed tree ids as app provisioning.</summary>
internal sealed class AppReplicationEnrolment(ILatticeReplicationConfigAuthority? authority)
{
    internal async Task<(AppActivationFailure Failure, AppManifestError? Diagnostic, IReadOnlyDictionary<string, bool>? Trees)> ApplyAsync(
        TenantId tenant,
        AppManifest? manifest,
        AppManifest? previous,
        IReadOnlyDictionary<string, bool> trackedTrees,
        Func<IReadOnlyDictionary<string, bool>, Task> recordIntent,
        CancellationToken cancellationToken)
    {
        if (authority is null)
        {
            return default;
        }

        var desired = Resolve(tenant, manifest);
        var prior = Resolve(tenant, previous);
        var pending = new Dictionary<string, bool>(trackedTrees, StringComparer.Ordinal);
        foreach (var priorTree in prior.Keys)
        {
            // A manifest without provenance is not evidence that this app authored an enable.
            pending.TryAdd(priorTree, false);
        }
        var treeId = string.Empty;
        try
        {
            // Check the entire set before authoring any enrolment: a later tree's mode
            // rejection must not leave earlier trees enabled or dropped trees disabled.
            foreach (var pair in desired)
            {
                treeId = pair.Key;
                var status = await authority.GetTreeStatusAsync(treeId, cancellationToken).ConfigureAwait(false);
                if ((prior.TryGetValue(treeId, out var oldMode) && oldMode != pair.Value)
                    || status is { Enabled: true } current
                        && (current.Ambiguous || current.Mode is { } mode && mode != pair.Value))
                {
                    return Failure(AppActivationFailure.ReplicationModeChangeRejected, "replication-mode-change",
                        treeId, "The declared merge mode differs from the previously enabled mode or the current mode is ambiguous.");
                }

                pending[treeId] = trackedTrees.GetValueOrDefault(treeId) || status is not { Enabled: true };
            }

            // Persist the union before the first side effect so an interrupted upgrade can
            // still unenrol every attempted tree, even if its manifest never becomes applied.
            if (pending.Count > 0)
            {
                await recordIntent(new Dictionary<string, bool>(pending, StringComparer.Ordinal)).ConfigureAwait(false);
            }

            foreach (var pair in desired)
            {
                treeId = pair.Key;
                var enabled = await authority.EnableReplicationAsync(treeId, pair.Value, cancellationToken: cancellationToken).ConfigureAwait(false);
                var authored = trackedTrees.GetValueOrDefault(treeId) || !enabled.AlreadyEnabled;
                if (pending[treeId] != authored)
                {
                    pending[treeId] = authored;
                    await recordIntent(new Dictionary<string, bool>(pending, StringComparer.Ordinal)).ConfigureAwait(false);
                }
            }

            foreach (var entry in pending)
            {
                treeId = entry.Key;
                if (entry.Value && !desired.ContainsKey(treeId))
                {
                    await authority.DisableReplicationAsync(treeId, cancellationToken).ConfigureAwait(false);
                }
            }

            var retained = new Dictionary<string, bool>(desired.Count, StringComparer.Ordinal);
            foreach (var desiredTree in desired.Keys)
            {
                retained.Add(desiredTree, pending[desiredTree]);
            }

            return (AppActivationFailure.None, null, retained);
        }
        catch (LatticeReplicationModeChangeRejectedException ex)
        {
            return Failure(AppActivationFailure.ReplicationModeChangeRejected, "replication-mode-change", treeId, ex.Message);
        }
        catch (LatticeReplicationPreconditionFailedException ex)
        {
            return Failure(AppActivationFailure.ReplicationPreconditionFailed, "replication-precondition", treeId, ex.Message);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return Failure(AppActivationFailure.ReplicationEnrolmentFailed, "replication-enrolment", treeId,
                $"{ex.GetType().Name}: {ex.Message}");
        }
    }

    private static (AppActivationFailure, AppManifestError, IReadOnlyDictionary<string, bool>?) Failure(
        AppActivationFailure failure, string code, string treeId, string message) =>
        (failure, new AppManifestError(code, "$.replication", $"Replication for tree '{treeId}' failed: {message}"), null);

    private static Dictionary<string, LatticeMergeMode> Resolve(TenantId tenant, AppManifest? manifest)
    {
        var result = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
        if (manifest?.Replication is not { } declarations)
        {
            return result;
        }

        foreach (var declaration in declarations)
        {
            foreach (var tree in manifest.Trees)
            {
                if (string.Equals(tree.Name, declaration.Tree, StringComparison.Ordinal))
                {
                    var local = tree.AdoptedTreeId
                        ?? AppActivationTreeNames.LocalStructuralTree(manifest.Identity.Slug, tree.Name);
                    result.Add(LatticeTenantResolution.ComposeEffectiveTreeId(tenant, local), declaration.MergeMode);
                    break;
                }
            }
        }

        return result;
    }
}
