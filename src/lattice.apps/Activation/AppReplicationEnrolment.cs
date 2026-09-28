using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps;

/// <summary>Applies replication intent to the same tenant-composed tree ids as app provisioning.</summary>
internal sealed class AppReplicationEnrolment(ILatticeReplicationConfigAuthority? authority)
{
    internal async Task<(AppActivationFailure Failure, AppManifestError? Diagnostic, IReadOnlyList<string>? Trees)> ApplyAsync(
        TenantId tenant,
        AppManifest? manifest,
        AppManifest? previous,
        IReadOnlyList<string> trackedTrees,
        Func<IReadOnlyList<string>, Task> recordIntent,
        CancellationToken cancellationToken)
    {
        if (authority is null)
        {
            return default;
        }

        var desired = Resolve(tenant, manifest);
        var prior = Resolve(tenant, previous);
        var retired = new HashSet<string>(trackedTrees, StringComparer.Ordinal);
        retired.UnionWith(prior.Keys);
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
            }

            // Persist the union before the first side effect so an interrupted upgrade can
            // still unenrol every attempted tree, even if its manifest never becomes applied.
            var pending = new HashSet<string>(retired, StringComparer.Ordinal);
            pending.UnionWith(desired.Keys);
            if (pending.Count > 0)
            {
                await recordIntent(pending.ToArray()).ConfigureAwait(false);
            }

            foreach (var pair in desired)
            {
                treeId = pair.Key;
                await authority.EnableReplicationAsync(treeId, pair.Value, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            foreach (var retiredTree in retired)
            {
                treeId = retiredTree;
                if (!desired.ContainsKey(treeId))
                {
                    await authority.DisableReplicationAsync(treeId, cancellationToken).ConfigureAwait(false);
                }
            }

            return (AppActivationFailure.None, null, desired.Keys.ToArray());
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

    private static (AppActivationFailure, AppManifestError, IReadOnlyList<string>?) Failure(
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
