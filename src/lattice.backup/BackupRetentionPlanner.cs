namespace Orleans.Lattice.Backup;

/// <summary>
/// The pure decisions of backup retention pruning, isolated from storage so that
/// <see cref="BackupSchedulerGrain"/>, <see cref="LatticeBackupCaptureService"/>
/// and the retention model (<c>spec/backup/BackupRetention.tla</c>) share one
/// rule set: what a policy keeps of a listing, the base closure of what it
/// keeps, which artifacts the kept manifests still reference, which doomed
/// manifests an increment in the sink still builds on, and whether an
/// increment may publish over its base.
/// </summary>
/// <remarks>
/// A retention pass and an incremental capture are fenced against each other
/// (issue #5357). The pass removes each doomed manifest's catalog row first, then
/// scans the sink once with <see cref="DependedOn"/>, restores the row of every
/// doomed manifest an increment outside the pass builds on, and deletes the rest
/// <see cref="TipFirst"/>, each sink manifest before its artifacts. The increment writes its sink manifest
/// first, then re-reads its base's catalog row and publishes only if
/// <see cref="MayPublishIncrement"/> allows it. Whichever side moves second sees
/// the other: either the pass finds the increment's sink manifest, or the
/// increment finds its base's row gone and falls back to a full capture.
/// </remarks>
internal static class BackupRetentionPlanner
{
    /// <summary>
    /// The ids of the manifests in <paramref name="listing"/> the policy retains
    /// at <paramref name="now"/>, including the base closure.
    /// </summary>
    /// <param name="listing">The scope's manifests, as listed from the catalog.</param>
    /// <param name="keepLast">Keep the newest this many manifests; <see langword="null"/> when unset.</param>
    /// <param name="maxAge">Keep every manifest created within this window; <see langword="null"/> when unset.</param>
    /// <param name="now">The current time.</param>
    /// <returns>
    /// Every listed manifest when neither knob is set (the unbounded no-op).
    /// Otherwise the newest <paramref name="keepLast"/> (by <see cref="RankTime"/>,
    /// then ordinal id, descending), every manifest last captured at or after
    /// <c>now - maxAge</c>, and then repeatedly the base of a kept increment while
    /// that base is itself in the listing.
    /// </returns>
    public static HashSet<string> ComputeRetained(
        IReadOnlyList<BackupManifest> listing, int? keepLast, TimeSpan? maxAge, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(listing);
        var keep = ComputeKeepSet(listing, keepLast, maxAge, now);
        ExpandBaseClosure(listing, keep);
        return keep;
    }

    /// <summary>
    /// The manifests of <paramref name="listing"/> outside <paramref name="retained"/>,
    /// in listing order.
    /// </summary>
    /// <param name="listing">The scope's manifests, as listed from the catalog.</param>
    /// <param name="retained">The retained ids from <see cref="ComputeRetained"/>.</param>
    /// <returns>The manifests the pass is to prune.</returns>
    public static List<BackupManifest> Doomed(IReadOnlyList<BackupManifest> listing, IReadOnlySet<string> retained)
    {
        ArgumentNullException.ThrowIfNull(listing);
        ArgumentNullException.ThrowIfNull(retained);
        var doomed = new List<BackupManifest>();
        foreach (var manifest in listing)
        {
            if (!retained.Contains(manifest.Id))
            {
                doomed.Add(manifest);
            }
        }

        return doomed;
    }

    /// <summary>
    /// The artifact ids every retained manifest of <paramref name="listing"/>
    /// references, which a pass never deletes even when a pruned manifest names
    /// the same id.
    /// </summary>
    /// <param name="listing">The scope's manifests, as listed from the catalog.</param>
    /// <param name="retained">The retained ids from <see cref="ComputeRetained"/>.</param>
    /// <returns>The artifact ids to keep.</returns>
    public static HashSet<string> RetainedArtifacts(IReadOnlyList<BackupManifest> listing, IReadOnlySet<string> retained)
    {
        ArgumentNullException.ThrowIfNull(listing);
        ArgumentNullException.ThrowIfNull(retained);
        var artifacts = new HashSet<string>(StringComparer.Ordinal);
        foreach (var manifest in listing)
        {
            if (!retained.Contains(manifest.Id))
            {
                continue;
            }

            foreach (var descriptor in manifest.ContentDescriptors)
            {
                artifacts.Add(descriptor.ArtifactId);
            }
        }

        return artifacts;
    }

    /// <summary>
    /// The ids of the <paramref name="doomed"/> manifests the pass must keep
    /// because something it is not pruning builds on them: every doomed manifest
    /// an increment in <paramref name="sinkManifests"/> outside the pass names as
    /// its base, and, transitively, the base of every manifest so kept. An
    /// increment the pass is itself pruning does not keep its base. A pass that
    /// has removed a kept manifest's catalog row restores it instead of deleting it.
    /// </summary>
    /// <param name="doomed">The manifests the pass is pruning.</param>
    /// <param name="sinkManifests">Every manifest in the sink, read after the doomed rows were removed.</param>
    /// <returns>The doomed ids to keep.</returns>
    public static HashSet<string> DependedOn(IReadOnlyCollection<BackupManifest> doomed, IEnumerable<BackupManifest> sinkManifests)
    {
        ArgumentNullException.ThrowIfNull(doomed);
        ArgumentNullException.ThrowIfNull(sinkManifests);
        var doomedById = new Dictionary<string, BackupManifest>(StringComparer.Ordinal);
        foreach (var manifest in doomed)
        {
            doomedById[manifest.Id] = manifest;
        }

        var blocked = new HashSet<string>(StringComparer.Ordinal);
        var frontier = new Stack<string>();
        foreach (var manifest in sinkManifests)
        {
            if (!doomedById.ContainsKey(manifest.Id)
                && manifest.BaseBackupId is { } baseId
                && doomedById.ContainsKey(baseId)
                && blocked.Add(baseId))
            {
                frontier.Push(baseId);
            }
        }

        while (frontier.Count > 0)
        {
            if (doomedById[frontier.Pop()].BaseBackupId is { } baseId
                && doomedById.ContainsKey(baseId)
                && blocked.Add(baseId))
            {
                frontier.Push(baseId);
            }
        }

        return blocked;
    }

    /// <summary>
    /// Orders the manifests a pass deletes tip first: a manifest comes after every
    /// one of them that builds on it, so an interrupted pass never leaves an
    /// increment in the sink whose base it has already deleted.
    /// </summary>
    /// <param name="deleting">The manifests the pass deletes.</param>
    /// <returns>The same manifests, every increment before its base.</returns>
    public static List<BackupManifest> TipFirst(IReadOnlyList<BackupManifest> deleting)
    {
        ArgumentNullException.ThrowIfNull(deleting);
        var remaining = new List<BackupManifest>(deleting);
        var ordered = new List<BackupManifest>(deleting.Count);
        while (remaining.Count > 0)
        {
            var bases = new HashSet<string>(StringComparer.Ordinal);
            foreach (var manifest in remaining)
            {
                if (manifest.BaseBackupId is { } baseId)
                {
                    bases.Add(baseId);
                }
            }

            // A malformed cycle has no tip; emit the rest in listing order rather than loop.
            var tips = remaining.Where(m => !bases.Contains(m.Id)).ToList();
            if (tips.Count == 0)
            {
                ordered.AddRange(remaining);
                break;
            }

            ordered.AddRange(tips);
            remaining.RemoveAll(m => !bases.Contains(m.Id));
        }

        return ordered;
    }

    /// <summary>
    /// Whether an increment whose sink manifest has been written may register
    /// its catalog row: only while its base's catalog row is still present.
    /// </summary>
    /// <param name="increment">The increment about to be published.</param>
    /// <param name="baseRow">Its base's catalog row, read after the increment's sink manifest was written; <see langword="null"/> when absent.</param>
    /// <returns>
    /// <see langword="true"/> for a full backup, or for an increment whose base row
    /// is present; <see langword="false"/> when a retention pass has begun to
    /// prune the base, in which case the capture abandons the increment.
    /// </returns>
    public static bool MayPublishIncrement(BackupManifest increment, BackupManifest? baseRow)
    {
        ArgumentNullException.ThrowIfNull(increment);
        if (increment.BaseBackupId is not { } baseId)
        {
            return true;
        }

        return baseRow is not null && string.Equals(baseRow.Id, baseId, StringComparison.Ordinal);
    }

    /// <summary>
    /// The time retention ranks a backup by: when it was last captured. A backup
    /// id is a content address, so a capture of identical bytes republishes an
    /// existing backup, keeping its first <see cref="BackupManifest.CreatedAtUtc"/>
    /// and recording itself in <see cref="BackupManifest.LastCapturedAtUtc"/>; the
    /// later of the two is the rank. A manifest that predates the field ranks by
    /// <see cref="BackupManifest.CreatedAtUtc"/>.
    /// </summary>
    /// <param name="manifest">The manifest to rank.</param>
    /// <returns>The later of its capture time and its last capture time.</returns>
    public static DateTimeOffset RankTime(BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        if (RedProof.NoRankRefresh) { return manifest.CreatedAtUtc; } // RED-PROOF
        return manifest.LastCapturedAtUtc is { } last && last > manifest.CreatedAtUtc
            ? last
            : manifest.CreatedAtUtc;
    }

    private static HashSet<string> ComputeKeepSet(
        IReadOnlyList<BackupManifest> listing, int? keepLast, TimeSpan? maxAge, DateTimeOffset now)
    {
        var keep = new HashSet<string>(StringComparer.Ordinal);

        // Retention enabled but unbounded (neither knob set) retains everything -
        // a safe no-op that never prunes.
        if (keepLast is null && maxAge is null)
        {
            foreach (var manifest in listing)
            {
                keep.Add(manifest.Id);
            }

            return keep;
        }

        if (keepLast is { } count)
        {
            var recent = listing
                .OrderByDescending(RankTime)
                .ThenByDescending(m => m.Id, StringComparer.Ordinal)
                .Take(count);
            foreach (var manifest in recent)
            {
                keep.Add(manifest.Id);
            }
        }

        if (maxAge is { } window)
        {
            // Guard the subtraction: a very large (but validator-permitted, since the
            // validator only rejects a non-positive window) RetentionMaxAge would push
            // the cutoff below DateTimeOffset.MinValue and throw. A "retain for
            // practically forever" window means keep every manifest, so saturate the
            // cutoff at MinValue rather than fault the retention pass.
            var cutoff = window >= now - DateTimeOffset.MinValue
                ? DateTimeOffset.MinValue
                : now - window;
            foreach (var manifest in listing)
            {
                if (RankTime(manifest) >= cutoff)
                {
                    keep.Add(manifest.Id);
                }
            }
        }

        return keep;
    }

    private static void ExpandBaseClosure(IReadOnlyList<BackupManifest> listing, HashSet<string> keep)
    {
        var byId = new Dictionary<string, BackupManifest>(StringComparer.Ordinal);
        foreach (var manifest in listing)
        {
            byId[manifest.Id] = manifest;
        }

        var changed = true;
        while (changed)
        {
            changed = false;
            foreach (var id in keep.ToList())
            {
                if (byId.TryGetValue(id, out var manifest)
                    && manifest.BaseBackupId is { } baseId
                    && byId.ContainsKey(baseId)
                    && keep.Add(baseId))
                {
                    changed = true;
                }
            }
        }
    }
}
