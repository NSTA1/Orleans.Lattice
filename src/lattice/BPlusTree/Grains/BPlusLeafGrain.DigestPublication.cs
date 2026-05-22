namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-side digest <i>publication</i> wiring (distinct from the
/// digest <i>computation</i> in <c>BPlusLeafGrain.Digest.cs</c>):
/// records this leaf's parent internal node id via
/// <see cref="SetParentAsync"/> and publishes a fresh
/// <see cref="ChildDigestSnapshot"/> to that parent whenever the
/// leaf's projection digest changes. Together with the matching
/// internal-node aggregator (<c>BPlusInternalGrain.Digest.cs</c>),
/// this maintains an O(1)-per-shard root-side fold so a whole-tree
/// digest poll no longer fans out into per-leaf grain calls.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation flag that is flipped <see langword="true"/> by
    /// <c>StoreEntry</c> / <c>RemoveEntry</c> whenever the running
    /// projection hash changes, and consulted by
    /// <see cref="PublishDigestUpwardAsync"/> so a mutation funnel that
    /// makes no observable change (e.g. a re-application of an
    /// already-applied LWW value at or below the persisted Timestamp)
    /// pays zero cross-grain publication cost. Reset on every
    /// successful publish.
    /// </summary>
    private bool _digestDirty;

    /// <summary>
    /// Per-activation guard preventing repeated one-way latch stamping
    /// on every trimmed-path mutation. Set on the first observed
    /// disabled-digest mutation after activation; the latch itself is
    /// idempotent on the registry side, but skipping the cross-grain
    /// hop after the first stamp avoids unnecessary registry churn for
    /// every subsequent write on the same activation.
    /// </summary>
    private bool _latchAttempted;

    /// <summary>
    /// Marks the running projection hash as dirty. Called from the
    /// digest funnels (<c>StoreEntry</c>, <c>RemoveEntry</c>) so the
    /// per-mutation publication path can elide no-op publishes.
    /// </summary>
    internal void MarkDigestDirty() => _digestDirty = true;

    /// <inheritdoc />
    public async Task SetParentAsync(GrainId? parentId)
    {
        if (state.State.ParentId == parentId)
        {
            // Idempotent re-call with the same parent: no persist, no
            // callback. The internal-node seeding path consults the
            // child via GetChildDigestSnapshotAsync directly after a
            // SetParentAsync call so we never callback into a parent
            // that may still be inside its own AcceptSplitAsync /
            // InitializeAsync mutation frame (which would deadlock the
            // non-reentrant internal grain).
            return;
        }

        var prev = state.State.ParentId;
        state.State.ParentId = parentId;
        try
        {
            await PersistAsync();
        }
        catch
        {
            state.State.ParentId = prev;
            throw;
        }

        // Mark the digest dirty so the next mutation triggers an upward
        // publish to the new parent. The parent itself is expected to
        // pull our current snapshot via GetChildDigestSnapshotAsync
        // immediately after this call (driven by the internal-grain
        // seeding path), which keeps the parent's ChildDigests table
        // consistent without a reentrant callback.
        if (parentId is not null) _digestDirty = true;
    }

    /// <inheritdoc />
    public Task<ChildDigestSnapshot> GetChildDigestSnapshotAsync()
    {
        EnsureProjectionHashInitialized();
        return Task.FromResult(new ChildDigestSnapshot
        {
            // Clone so an in-place XOR on state.State.ProjectionHash
            // cannot retroactively mutate the bytes the caller has
            // captured.
            Hash = (byte[])state.State.ProjectionHash!.Clone(),
            EntryCount = Cache.Count,
            CheckpointOffset = state.State.ProjectionCheckpointOffset,
        });
    }

    /// <summary>
    /// Publishes this leaf's current <see cref="ChildDigestSnapshot"/>
    /// to its parent internal node, if a parent is registered and the
    /// running projection hash has been marked dirty since the previous
    /// publish. Called from every mutation funnel after the digest
    /// funnels (<c>StoreEntry</c> / <c>RemoveEntry</c>) have flipped
    /// <see cref="_digestDirty"/>. A <see langword="null"/> parent
    /// (flat-tree case where the root is itself a leaf) makes this a
    /// no-op; the shard-root digest read path falls back to reading
    /// the leaf directly in that shape.
    /// </summary>
    private async Task PublishDigestUpwardAsync()
    {
        if (!_maintainProjectionDigest)
        {
            // Disabled-mode mutation: we have just landed a write while
            // the resolved opt-out is false, which means the persisted
            // projection-hash aggregate is now permanently stale (the
            // running XOR fold has skipped contributions). Stamp the
            // one-way registry latch so any later configuration flip
            // back to MaintainProjectionDigest = true cannot silently
            // re-expose the digest API on top of a hash that no longer
            // describes the leaf contents. The stamp is best-effort
            // (a registry failure during the trimmed path must not fail
            // the user-visible mutation), idempotent (LatchProjection...
            // is a no-op once already true), and gated by an activation
            // guard so we pay at most one registry hop per activation.
            if (!_latchAttempted)
            {
                _latchAttempted = true;
                await TryStampDigestLatchAsync();
            }
            return;
        }
        if (!_digestDirty) return;
        if (state.State.ParentId is not { } parentId)
        {
            // Clear the dirty flag even without a parent so we do not
            // accumulate dirt across mutations on a flat-tree leaf. A
            // future re-parent re-publishes via SetParentAsync.
            _digestDirty = false;
            return;
        }
        await PublishCurrentDigestAsync(parentId);
        _digestDirty = false;
    }

    /// <summary>
    /// Best-effort stamp of the one-way registry latch that records
    /// "this tree has accepted writes while digest maintenance was
    /// disabled". Skipped for system trees because they bypass the
    /// registry entirely (and never participate in digest maintenance
    /// by design); failures are swallowed because the mutation has
    /// already succeeded and re-attempting the stamp on the next
    /// mutation is acceptable.
    /// </summary>
    private async Task TryStampDigestLatchAsync()
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId)) return;
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            // System trees bypass the registry; the resolver already
            // forces MaintainProjectionDigest = false for them and the
            // latch is meaningless because there is no per-tree row to
            // stamp.
            return;
        }
        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.LatchProjectionDigestPermanentlyDisabledAsync(treeId);
        }
        catch
        {
            // Latch stamping is best-effort. A registry failure must
            // not propagate into the user-visible mutation. The guard
            // flag is reset so a subsequent mutation gets another
            // chance to stamp.
            _latchAttempted = false;
        }
    }

    private async Task PublishCurrentDigestAsync(GrainId parentId)
    {
        EnsureProjectionHashInitialized();
        var snapshot = new ChildDigestSnapshot
        {
            // Clone so a subsequent in-place XOR on
            // state.State.ProjectionHash does not retroactively mutate
            // the bytes the parent's table has captured.
            Hash = (byte[])state.State.ProjectionHash!.Clone(),
            EntryCount = Cache.Count,
            CheckpointOffset = state.State.ProjectionCheckpointOffset,
        };
        var parent = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
        await parent.OnChildDigestPublishedAsync(context.GrainId, snapshot);
    }
}
