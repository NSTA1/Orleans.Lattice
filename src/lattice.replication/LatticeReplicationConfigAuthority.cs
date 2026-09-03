using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The default <see cref="ILatticeReplicationConfigAuthority"/>. Performs the
/// read-modify-write authoring against the config OR-Map through
/// <see cref="ILatticeReplicationConfigStore"/>: it reads the current converged
/// <see cref="LatticeReplicationConfigEntry"/> for the target tree, mutates a
/// clone (fixing the merge mode and toggling the enablement flag), and writes
/// the fully-merged entry back so every enrolled peer converges by recursing
/// into <see cref="LatticeReplicationConfigEntry.MergeFrom"/>.
/// <para>
/// Enablement dots are stamped with the local replica id from
/// <see cref="ILatticeReplicationContext.LocalReplicaId"/> and a replica-local
/// monotonic counter minted from the current flag state (see
/// <see cref="NextFlagCounter"/>); the merge-mode register mints its own dot
/// internally. The authority is <b>not</b> the authorization boundary - the API
/// facade authorizes the operator before calling here - so it runs its config
/// reads and writes as replication infrastructure under the system origin (via
/// the store) and assumes an already-authorized caller.
/// </para>
/// <para>
/// <b>Reads reconcile both enrollment sources.</b> Authoring only ever touches
/// the config OR-Map, but a host's <i>effective</i> replicated-tree set is the
/// OR-Map unioned with the static
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> deployment map,
/// because <see cref="SnapshotLatticeMergeModeResolver"/> falls back to that map
/// on the commit path. <see cref="GetTreeStatusAsync"/> and
/// <see cref="GetAllTreeStatusesAsync"/> therefore project the union under the
/// same precedence the resolver applies, so an operator report can never say
/// "no trees" about an estate that is demonstrably replicating.
/// </para>
/// </summary>
internal sealed class LatticeReplicationConfigAuthority(
    ILatticeReplicationConfigStore store,
    ILatticeReplicationPreconditionValidator preconditions,
    ILatticeReplicationContext replicationContext,
    ILatticeReplicationAdmin admin,
    ILatticeTreeContentProbe treeContentProbe,
    IOptionsMonitor<LatticeReplicationOptions> options) : ILatticeReplicationConfigAuthority
{
    private readonly ILatticeReplicationConfigStore _store =
        store ?? throw new ArgumentNullException(nameof(store));

    private readonly ILatticeReplicationPreconditionValidator _preconditions =
        preconditions ?? throw new ArgumentNullException(nameof(preconditions));

    private readonly ILatticeReplicationContext _replicationContext =
        replicationContext ?? throw new ArgumentNullException(nameof(replicationContext));

    private readonly ILatticeReplicationAdmin _admin =
        admin ?? throw new ArgumentNullException(nameof(admin));

    private readonly ILatticeTreeContentProbe _treeContentProbe =
        treeContentProbe ?? throw new ArgumentNullException(nameof(treeContentProbe));

    private readonly IOptionsMonitor<LatticeReplicationOptions> _options =
        options ?? throw new ArgumentNullException(nameof(options));

    /// <inheritdoc />
    public async Task<LatticeReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // Precondition validation FIRST: reject flag-mode without a local
        // replica id cleanly, before any config read or write.
        var precondition = _preconditions.Validate(treeId, mode);
        if (!precondition.IsSatisfied)
        {
            throw new LatticeReplicationPreconditionFailedException(
                precondition.FailureReason ?? $"Replication cannot be enabled for tree '{treeId}'.",
                treeId,
                mode);
        }

        // The config entry's own enablement flag is an RwFlag whose dots are
        // stamped with the local replica id, so any mode needs a non-empty one.
        var replicaId = _replicationContext.LocalReplicaId;
        if (string.IsNullOrEmpty(replicaId))
        {
            throw new LatticeReplicationPreconditionFailedException(
                $"Replication cannot be enabled for tree '{treeId}': the replication configuration "
                + "entry's enablement flag is authored with the local replica id, but none is "
                + $"configured. Set {nameof(LatticeReplicationOptions)}."
                + $"{nameof(LatticeReplicationOptions.ClusterId)} to a non-empty, globally-unique "
                + "cluster identifier.",
                treeId,
                mode);
        }

        var current = await _store.ReadEntryAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (current is { IsEnabled: true })
        {
            // The tree is already replicating. Its merge mode is fixed and can
            // only change via disable-then-re-enable.
            if (current.HasAmbiguousMode)
            {
                throw new LatticeReplicationModeChangeRejectedException(
                    $"Tree '{treeId}' is enabled with an ambiguous merge mode (concurrent clusters "
                    + "assigned divergent modes). Disable the tree and re-enable it under the "
                    + $"required mode '{mode}' to clear the ambiguity and re-bootstrap peers.",
                    treeId,
                    mode,
                    default,
                    currentModeAmbiguous: true);
            }

            if (current.TryGetMode(out var existingMode))
            {
                if (existingMode == mode)
                {
                    // Idempotent no-op: already enabled under the requested mode.
                    return new LatticeReplicationEnableResult(
                        treeId,
                        existingMode,
                        AlreadyEnabled: true,
                        BootstrapRequested: false);
                }

                throw new LatticeReplicationModeChangeRejectedException(
                    $"Tree '{treeId}' is already enabled under merge mode '{existingMode}', which "
                    + $"cannot be changed to '{mode}' in place. Disable the tree and re-enable it "
                    + "under the new mode to change it (this re-bootstraps peers).",
                    treeId,
                    mode,
                    existingMode,
                    currentModeAmbiguous: false);
            }

            // Enabled but no unambiguous mode value present (an unusual merged
            // state): fall through to fix the mode and re-author the flag.
        }

        var working = current?.Clone() ?? new LatticeReplicationConfigEntry();
        working.SetMode(replicaId, mode);
        working.Enable(replicaId, NextFlagCounter(working.Enabled, replicaId));
        await _store.WriteEntryAsync(treeId, replicaId, working, cancellationToken).ConfigureAwait(false);

        // Compose with the snapshot bootstrap when enabling a tree that already
        // holds data: the change feed only carries new mutations, so peers need
        // a one-off snapshot of the pre-existing rows. The bootstrap seam is
        // receiver-driven, so bootstrapSourceClusterId names the cluster holding
        // the authoritative data to pull from.
        var bootstrapRequested = false;
        if (!string.IsNullOrEmpty(bootstrapSourceClusterId))
        {
            if (await _treeContentProbe.HasContentAsync(treeId, cancellationToken).ConfigureAwait(false))
            {
                await _admin.RequestSnapshotAsync(treeId, bootstrapSourceClusterId, cancellationToken)
                    .ConfigureAwait(false);
                bootstrapRequested = true;
            }
        }

        return new LatticeReplicationEnableResult(
            treeId,
            mode,
            AlreadyEnabled: false,
            BootstrapRequested: bootstrapRequested);
    }

    /// <inheritdoc />
    public async Task<LatticeReplicationDisableResult> DisableReplicationAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var current = await _store.ReadEntryAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (current is null || !current.IsEnabled)
        {
            // Absent or already disabled: idempotent no-op, no dot authored and
            // no local replica id required.
            return new LatticeReplicationDisableResult(treeId, AlreadyDisabled: true);
        }

        var replicaId = _replicationContext.LocalReplicaId;
        if (string.IsNullOrEmpty(replicaId))
        {
            throw new LatticeReplicationPreconditionFailedException(
                $"Replication cannot be disabled for tree '{treeId}': the disable flag dot is "
                + "authored with the local replica id, but none is configured. Set "
                + $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ClusterId)} "
                + "to a non-empty, globally-unique cluster identifier.",
                treeId,
                default);
        }

        // Disable-wins: author a disable dot. The entry (and its fixed mode) is
        // kept in the OR-Map, and already-replicated peer data is never purged;
        // disable only pauses shipping of new mutations.
        var working = current.Clone();
        working.Disable(replicaId, NextFlagCounter(working.Enabled, replicaId));
        await _store.WriteEntryAsync(treeId, replicaId, working, cancellationToken).ConfigureAwait(false);

        return new LatticeReplicationDisableResult(treeId, AlreadyDisabled: false);
    }

    /// <inheritdoc />
    public async Task<LatticeReplicationTreeStatus?> GetTreeStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var entry = await _store.ReadEntryAsync(treeId, cancellationToken).ConfigureAwait(false);
        var declared = StaticDeclarations();
        LatticeMergeMode? staticMode =
            declared is not null && declared.TryGetValue(treeId, out var m) ? m : null;

        return entry is null && staticMode is null ? null : Reconcile(treeId, entry, staticMode);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>> GetAllTreeStatusesAsync(
        CancellationToken cancellationToken = default)
    {
        var entries = await _store.ReadEntriesAsync(cancellationToken).ConfigureAwait(false);
        var declared = StaticDeclarations();
        var declaredCount = declared?.Count ?? 0;

        var result = new Dictionary<string, LatticeReplicationTreeStatus>(
            entries.Count + declaredCount,
            StringComparer.Ordinal);

        foreach (var pair in entries)
        {
            LatticeMergeMode? staticMode =
                declared is not null && declared.TryGetValue(pair.Key, out var m) ? m : null;
            result[pair.Key] = Reconcile(pair.Key, pair.Value, staticMode);
        }

        if (declared is null)
        {
            return result;
        }

        // Statically declared trees the runtime config tree has never been asked
        // about are still shipped by the resolver's fallback, so they belong in
        // the report. Reporting only the OR-Map made a purely statically
        // configured estate look unreplicated.
        foreach (var pair in declared)
        {
            if (!result.ContainsKey(pair.Key))
            {
                result[pair.Key] = Reconcile(pair.Key, entry: null, pair.Value);
            }
        }

        return result;
    }

    /// <summary>
    /// The static deployment-time replicated-tree map currently in effect, or
    /// <see langword="null"/> when the host declared none. Read from
    /// <see cref="IOptionsMonitor{TOptions}.CurrentValue"/> on every call, so a
    /// reloaded configuration is reflected without a restart - matching how
    /// <see cref="SnapshotReplicatedTreeMembership"/> reads the same map.
    /// </summary>
    private IReadOnlyDictionary<string, LatticeMergeMode>? StaticDeclarations()
        => _options.CurrentValue.ReplicatedTrees;

    /// <summary>
    /// Projects a tree's two enrollment sources - its stored
    /// <see cref="LatticeReplicationConfigEntry"/> (when the runtime config tree
    /// carries one) and its static
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> declaration (when
    /// the deployment declared one) - into the effective
    /// <see cref="LatticeReplicationTreeStatus"/> an operator surface reports.
    /// <para>
    /// The precedence deliberately mirrors
    /// <see cref="SnapshotLatticeMergeModeResolver.Resolve"/> exactly, so the
    /// report describes what the commit path actually does: ambiguity fails
    /// closed and is never resolved by the static declaration; otherwise an
    /// enabled, unambiguous runtime mode wins; otherwise the static declaration
    /// is the floor that keeps the tree shipping.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree the status describes.</param>
    /// <param name="entry">The runtime config entry, or <see langword="null"/> when the OR-Map has none.</param>
    /// <param name="staticMode">The statically declared merge mode, or <see langword="null"/> when undeclared.</param>
    /// <returns>The reconciled per-tree status.</returns>
    private static LatticeReplicationTreeStatus Reconcile(
        string treeId,
        LatticeReplicationConfigEntry? entry,
        LatticeMergeMode? staticMode)
    {
        if (entry is null)
        {
            // Static-only: the resolver returns the declared mode unconditionally,
            // so the tree is effectively enrolled even though no operator ever
            // called enable.
            return new LatticeReplicationTreeStatus(treeId, Enabled: true, staticMode, Ambiguous: false)
            {
                Source = LatticeReplicationEnrollmentSource.Static,
            };
        }

        var bothDeclare = staticMode is not null;

        // Fail closed: a divergent runtime mode pauses shipping for this tree and
        // must never fall through to the static declaration, which would silently
        // pick a mode.
        if (entry.HasAmbiguousMode)
        {
            return new LatticeReplicationTreeStatus(treeId, entry.IsEnabled, Mode: null, Ambiguous: true)
            {
                Source = bothDeclare
                    ? LatticeReplicationEnrollmentSource.RuntimeAndStatic
                    : LatticeReplicationEnrollmentSource.Runtime,
            };
        }

        var hasRuntimeMode = entry.TryGetMode(out var runtimeMode);
        if (entry.IsEnabled && hasRuntimeMode)
        {
            return new LatticeReplicationTreeStatus(treeId, Enabled: true, runtimeMode, Ambiguous: false)
            {
                Source = bothDeclare
                    ? LatticeReplicationEnrollmentSource.RuntimeAndStatic
                    : LatticeReplicationEnrollmentSource.Runtime,
            };
        }

        if (bothDeclare)
        {
            // The runtime entry yields no enabled, unambiguous mode, so the
            // resolver falls back to the static declaration and the tree keeps
            // shipping under it. A runtime disable does not override the
            // deployment-time floor, and saying otherwise here would reproduce
            // the very under-reporting this projection exists to prevent.
            return new LatticeReplicationTreeStatus(treeId, Enabled: true, staticMode, Ambiguous: false)
            {
                Source = LatticeReplicationEnrollmentSource.Static,
            };
        }

        // Runtime-only and not currently shipping: report the flag state and the
        // mode the entry retains, which a later re-enable reuses.
        return new LatticeReplicationTreeStatus(
            treeId,
            entry.IsEnabled,
            hasRuntimeMode ? runtimeMode : null,
            Ambiguous: false)
        {
            Source = LatticeReplicationEnrollmentSource.Runtime,
        };
    }

    /// <summary>
    /// Mints the next replica-local monotonic counter for a fresh enable/disable
    /// dot on <paramref name="flag"/>: one past the maximum counter this replica
    /// has already stamped on any enable, disable, or tombstone dot. Because the
    /// authority reads the fully-merged entry before authoring, this counter is
    /// strictly greater than every dot this replica has contributed to the
    /// converged state, so the new dot is causally fresh.
    /// </summary>
    private static long NextFlagCounter(RwFlag flag, string replicaId)
    {
        var max = 0L;
        var seen = false;
        UpdateMax(flag.Enables, replicaId, ref max, ref seen);
        UpdateMax(flag.Disables, replicaId, ref max, ref seen);
        UpdateMax(flag.Tombstones, replicaId, ref max, ref seen);
        return seen ? max + 1 : 1;
    }

    private static void UpdateMax(List<OrSetDot> dots, string replicaId, ref long max, ref bool seen)
    {
        foreach (var dot in dots)
        {
            if (string.Equals(dot.ReplicaId, replicaId, StringComparison.Ordinal) && (!seen || dot.Counter > max))
            {
                max = dot.Counter;
                seen = true;
            }
        }
    }
}
