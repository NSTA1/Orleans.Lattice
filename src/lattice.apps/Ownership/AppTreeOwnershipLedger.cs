namespace Orleans.Lattice.Apps;

/// <summary>
/// The tree ownership ledger: the claim, verify, release and describe rules that make every tree an
/// app owns (its structural <c>a/{slug}/{tree}</c> trees and its adopted pre-app trees) belong to
/// exactly one install for the tree's whole lifetime, including its soft-delete window.
/// </summary>
/// <remarks>
/// <para>
/// <b>Storage.</b> One <see cref="AppTreeClaim"/> per tree in the reserved <c>sys-app-trees</c>
/// tree, keyed by the tree's effective (tenant-composed) id, so the same app-local names in two
/// tenants never conflict and no tenant id reaches a manifest. Every write is a compare-and-set, so
/// two concurrent claimants cannot both win.
/// </para>
/// <para>
/// <b>Standing of an existing claim.</b> A claim owned by the claimant is its own (idempotent). A
/// claim owned by another identity (tenant, slug and publisher) is held while that owner is installed
/// (its registry record exists, is not uninstalled and records the same publisher); a structural claim
/// is additionally held while its tree is still registered, which covers the whole soft-delete window
/// after uninstall. Any other claim - released, or stale because its owner is gone and (for a
/// structural claim) its tree was purged - is free, so no purge hook is needed.
/// </para>
/// <para>
/// <b>A fresh claim</b> refuses, by generic registry facts only: a structural tree that already exists
/// (created outside any app lifecycle); a tree core derived from another tree (not a logical tree);
/// and a tree whose physical backing is also another logical tree's alias target, or which is aliased
/// to a tree not derived from it (owning it would give the data a second name).
/// </para>
/// </remarks>
internal sealed class AppTreeOwnershipLedger
{
    /// <summary>The bounded compare-and-set retry budget for one ledger entry.</summary>
    internal const int MaxAttempts = 8;

    private readonly IAppTreeLedgerStore _ledger;
    private readonly IAppTreeFacts _facts;
    private readonly IAppRegistryStore _registry;
    private readonly TimeProvider _time;

    /// <summary>Initializes a new <see cref="AppTreeOwnershipLedger"/>.</summary>
    /// <param name="ledger">The claim store.</param>
    /// <param name="facts">The core tree-registry facts.</param>
    /// <param name="registry">The app registry store, consulted for owner liveness.</param>
    /// <param name="timeProvider">The clock stamping claims; defaults to <see cref="TimeProvider.System"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    public AppTreeOwnershipLedger(
        IAppTreeLedgerStore ledger,
        IAppTreeFacts facts,
        IAppRegistryStore registry,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(ledger);
        ArgumentNullException.ThrowIfNull(facts);
        ArgumentNullException.ThrowIfNull(registry);
        _ledger = ledger;
        _facts = facts;
        _registry = registry;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// The trees an install of <paramref name="manifest"/> in <paramref name="tenant"/> owns, in
    /// ascending ledger-key order so concurrent claimants contend on the same first key.
    /// </summary>
    /// <param name="manifest">A validated manifest.</param>
    /// <param name="tenant">The install's tenant.</param>
    /// <returns>The claims to take.</returns>
    internal static AppTreeClaimPlan[] Plan(AppManifest manifest, TenantId tenant)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        var slug = manifest.Identity.Slug;
        var trees = manifest.Trees ?? [];
        var plan = new List<AppTreeClaimPlan>(trees.Length);
        foreach (var tree in trees)
        {
            if (tree is null)
                continue;
            if (tree.AdoptedTreeId is { } adopted)
            {
                // A non-adoptable id can never be granted either; it is refused by validation and
                // the compilers, and is never recorded as owned.
                if (AppTreeIds.IsAdoptable(adopted))
                    plan.Add(new(LatticeTenantResolution.ComposeEffectiveTreeId(tenant, adopted), tree.Name, AppTreeClaimKind.Adopted));
            }
            else
            {
                plan.Add(new(AppActivationTreeNames.StructuralTree(tenant, slug, tree.Name), tree.Name, AppTreeClaimKind.Structural));
            }
        }

        plan.Sort(static (x, y) => string.CompareOrdinal(x.Key, y.Key));
        return plan.ToArray();
    }

    /// <summary>
    /// Takes (or confirms) every claim in <paramref name="plan"/> for <paramref name="owner"/>, stopping
    /// at the first conflict. Claims already held by the owner are idempotent.
    /// </summary>
    /// <param name="owner">The claiming install.</param>
    /// <param name="revision">The install's registry revision, recorded on new claims.</param>
    /// <param name="plan">The claims, from <see cref="Plan"/>.</param>
    /// <param name="acquired">Receives the keys of claims newly written by this call; may be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The first conflict, or <c>null</c> when every claim is held by the owner.</returns>
    internal async Task<AppTreeOwnershipConflict?> ClaimAsync(
        AppTreeOwner owner,
        long revision,
        IReadOnlyList<AppTreeClaimPlan> plan,
        List<string>? acquired,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);
        foreach (var item in plan)
        {
            if (await ClaimOneAsync(owner, revision, item, acquired, cancellationToken).ConfigureAwait(false) is { } conflict)
                return conflict;
        }

        return null;
    }

    /// <summary>Reports, without writing, every tree in <paramref name="plan"/> the owner could not claim.</summary>
    /// <param name="owner">The prospective owner.</param>
    /// <param name="plan">The claims, from <see cref="Plan"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The conflicts, in plan order; empty when every claim would succeed.</returns>
    internal async Task<IReadOnlyList<AppTreeOwnershipConflict>> DescribeAsync(
        AppTreeOwner owner,
        IReadOnlyList<AppTreeClaimPlan> plan,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);
        List<AppTreeOwnershipConflict>? conflicts = null;
        foreach (var item in plan)
        {
            var read = await _ledger.GetAsync(item.Key, cancellationToken).ConfigureAwait(false);
            var (standing, holder) = await StandingAsync(read.Claim, owner, item.Key, cancellationToken).ConfigureAwait(false);
            var conflict = standing switch
            {
                Standing.Own => null,
                Standing.Held => Held(item, holder!, owner),
                _ => await CheckFreshAsync(item).ConfigureAwait(false),
            };
            if (conflict is not null)
                (conflicts ??= []).Add(conflict);
        }

        return conflicts is null ? Array.Empty<AppTreeOwnershipConflict>() : conflicts;
    }

    /// <summary>Releases the owner's live claims on <paramref name="keys"/>; claims held by anyone else are untouched.</summary>
    /// <param name="owner">The owner whose claims to release.</param>
    /// <param name="keys">The ledger keys.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>A task that completes once every matching claim is released.</returns>
    internal async Task ReleaseAsync(AppTreeOwner owner, IEnumerable<string> keys, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(keys);
        foreach (var key in keys)
        {
            for (var attempt = 1; attempt <= MaxAttempts; attempt++)
            {
                var read = await _ledger.GetAsync(key, cancellationToken).ConfigureAwait(false);
                if (read.Claim is not { Released: false } claim || claim.Owner != owner)
                    break;
                if (await _ledger.TrySetAsync(key, claim with { Released = true }, read.Version, cancellationToken).ConfigureAwait(false))
                    break;
            }
        }
    }

    /// <summary>
    /// Releases every live adopted claim of <paramref name="owner"/> whose key is not in
    /// <paramref name="keep"/>: on uninstall (<paramref name="keep"/> <c>null</c>) or on an upgrade that
    /// stops adopting a tree. Structural claims are never released here; they are held until purge.
    /// </summary>
    /// <param name="owner">The owner.</param>
    /// <param name="keep">The adopted keys still owned, or <c>null</c> to release all.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>A task that completes once the claims are released.</returns>
    internal async Task ReleaseAdoptedAsync(AppTreeOwner owner, IReadOnlySet<string>? keep, CancellationToken cancellationToken)
    {
        List<string>? release = null;
        await foreach (var (key, claim) in _ledger.ScanAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!claim.Released
                && claim.Kind == AppTreeClaimKind.Adopted
                && claim.Owner == owner
                && (keep is null || !keep.Contains(key)))
            {
                (release ??= []).Add(key);
            }
        }

        if (release is not null)
            await ReleaseAsync(owner, release, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves which installed app owns each tree <paramref name="manifest"/> reaches through a
    /// cross-app role scope or subscription, for the pure compilers.
    /// </summary>
    /// <param name="manifest">The manifest being activated.</param>
    /// <param name="tenant">The install's tenant.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The owner snapshot; <see cref="AppTreeOwnerSnapshot.None"/> when the manifest reaches no other app.</returns>
    internal async Task<AppTreeOwnerSnapshot> ResolveCrossAppOwnersAsync(
        AppManifest manifest,
        TenantId tenant,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        var slug = manifest.Identity.Slug;
        HashSet<string>? seen = null;
        List<KeyValuePair<string, AppSlug>>? owners = null;

        foreach (var role in manifest.Roles ?? [])
        {
            foreach (var template in role?.Scopes ?? [])
            {
                if (template?.App is { } app && app != slug)
                    await ConsiderAsync(app, template.Tree).ConfigureAwait(false);
            }
        }

        foreach (var subscription in manifest.Subscriptions ?? [])
        {
            if (subscription?.App is { } app && app != slug)
                await ConsiderAsync(app, subscription.Tree).ConfigureAwait(false);
        }

        return owners is null ? AppTreeOwnerSnapshot.None : AppTreeOwnerSnapshot.Create(owners);

        async Task ConsiderAsync(AppSlug app, string tree)
        {
            var key = AppActivationTreeNames.StructuralTree(tenant, app, tree);
            if (!(seen ??= new(StringComparer.Ordinal)).Add(key))
                return;

            var read = await _ledger.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (read.Claim is { Released: false } claim
                && claim.Slug == app
                && claim.Tenant == tenant
                && await IsInstalledAsync(claim.Owner, cancellationToken).ConfigureAwait(false))
            {
                (owners ??= []).Add(new(key, app));
            }
        }
    }

    /// <summary>
    /// Decides a core alias from logical <paramref name="logicalTreeId"/> to physical
    /// <paramref name="physicalTreeId"/>: allowed iff the logical tree's owner equals the owner of the
    /// physical tree's derivation source (or of the physical tree itself when it is independent),
    /// where either owner may be "no app".
    /// </summary>
    /// <param name="logicalTreeId">The logical tree being aliased.</param>
    /// <param name="physicalTreeId">The physical target.</param>
    /// <param name="derivedFrom">The target's recorded derivation, or <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns><c>null</c> to allow, otherwise the denial reason.</returns>
    /// <remarks>
    /// Runs inside the core registry's mutation turn, so it performs only point reads: the ledger tree
    /// is first confirmed registered through an interleaving registry read, because reading an
    /// unregistered tree would lazily register it through a non-interleaving registry call.
    /// </remarks>
    internal async Task<string?> EvaluateAliasAsync(
        string logicalTreeId,
        string physicalTreeId,
        string? derivedFrom,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(logicalTreeId);
        ArgumentException.ThrowIfNullOrEmpty(physicalTreeId);
        if (!await _facts.ExistsAsync(AppRegistryTreeNames.TreeLedgerTree).ConfigureAwait(false))
            return null;

        var target = derivedFrom ?? physicalTreeId;
        var logicalOwner = await GetOwnerAsync(logicalTreeId, cancellationToken).ConfigureAwait(false);
        var targetOwner = await GetOwnerAsync(target, cancellationToken).ConfigureAwait(false);
        if (logicalOwner == targetOwner)
            return null;

        // The reason reaches API callers, so it names only the ids the caller supplied, never an owner.
        return $"Aliasing tree '{logicalTreeId}' to '{physicalTreeId}' would cross an app ownership boundary: "
            + "the tree and the target are not owned by the same app install.";
    }

    /// <summary>
    /// The effective owner of <paramref name="treeId"/> for alias decisions: the holder of a live
    /// structural claim, or of an adopted claim whose owner is still installed.
    /// </summary>
    /// <param name="treeId">The effective tree id.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The owner, or <c>null</c> when no app owns the tree.</returns>
    internal async Task<AppTreeOwner?> GetOwnerAsync(string treeId, CancellationToken cancellationToken)
    {
        var read = await _ledger.GetAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (read.Claim is not { Released: false } claim)
            return null;

        // A structural claim is held for the tree's whole lifetime; counting it without the
        // liveness read keeps the decision conservative (deny rather than allow).
        if (claim.Kind == AppTreeClaimKind.Structural)
            return claim.Owner;

        return await IsInstalledAsync(claim.Owner, cancellationToken).ConfigureAwait(false) ? claim.Owner : null;
    }

    private async Task<AppTreeOwnershipConflict?> ClaimOneAsync(
        AppTreeOwner owner,
        long revision,
        AppTreeClaimPlan item,
        List<string>? acquired,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            var read = await _ledger.GetAsync(item.Key, cancellationToken).ConfigureAwait(false);
            var (standing, holder) = await StandingAsync(read.Claim, owner, item.Key, cancellationToken).ConfigureAwait(false);
            if (standing == Standing.Own)
                return null;
            if (standing == Standing.Held)
                return Held(item, holder!, owner);

            if (await CheckFreshAsync(item).ConfigureAwait(false) is { } refused)
                return refused;

            var claim = new AppTreeClaim
            {
                Tenant = owner.Tenant,
                Slug = owner.Slug,
                Publisher = owner.Publisher,
                Kind = item.Kind,
                ClaimedAtUtc = _time.GetUtcNow(),
                InstallRevision = revision,
            };
            if (await _ledger.TrySetAsync(item.Key, claim, read.Version, cancellationToken).ConfigureAwait(false))
            {
                acquired?.Add(item.Key);
                return null;
            }
        }

        throw new InvalidOperationException(
            $"The ownership claim on tree '{item.TreeName}' was changed concurrently {MaxAttempts} times; retry.");
    }

    private async Task<(Standing Standing, AppTreeClaim? Holder)> StandingAsync(
        AppTreeClaim? claim,
        AppTreeOwner claimant,
        string key,
        CancellationToken cancellationToken)
    {
        if (claim is null || claim.Released)
            return (Standing.Free, null);
        if (claim.Owner == claimant)
            return (Standing.Own, claim);
        if (await IsInstalledAsync(claim.Owner, cancellationToken).ConfigureAwait(false))
            return (Standing.Held, claim);
        if (claim.Kind == AppTreeClaimKind.Structural && await _facts.ExistsAsync(key).ConfigureAwait(false))
            return (Standing.Held, claim);
        return (Standing.Free, null);
    }

    private async Task<AppTreeOwnershipConflict?> CheckFreshAsync(AppTreeClaimPlan item)
    {
        if (item.Kind == AppTreeClaimKind.Structural && await _facts.ExistsAsync(item.Key).ConfigureAwait(false))
        {
            return new(item.TreeName, AppTreeOwnershipConflictReason.PreExistingUnownedTree, null,
                $"Tree '{item.TreeName}' already exists and is not owned by any app install (a pre-existing unowned tree); it cannot be taken over.");
        }

        if (await _facts.GetDerivedFromAsync(item.Key).ConfigureAwait(false) is not null)
        {
            return new(item.TreeName, AppTreeOwnershipConflictReason.DerivedTree, null,
                $"Tree '{item.TreeName}' is a physical copy derived from another tree and cannot be owned.");
        }

        var backing = await _facts.ResolveAsync(item.Key).ConfigureAwait(false);
        var foreignAlias = !string.Equals(backing, item.Key, StringComparison.Ordinal)
            && !string.Equals(await _facts.GetDerivedFromAsync(backing).ConfigureAwait(false), item.Key, StringComparison.Ordinal);
        if (!foreignAlias)
        {
            foreach (var alias in await _facts.GetAliasesTargetingAsync(backing).ConfigureAwait(false))
            {
                if (!string.Equals(alias, item.Key, StringComparison.Ordinal))
                {
                    foreignAlias = true;
                    break;
                }
            }
        }

        return foreignAlias
            ? new(item.TreeName, AppTreeOwnershipConflictReason.AliasTarget, null,
                $"Tree '{item.TreeName}' shares its physical data with another tree through an alias and cannot be owned.")
            : null;
    }

    private async Task<bool> IsInstalledAsync(AppTreeOwner owner, CancellationToken cancellationToken)
    {
        var read = await _registry.GetAsync(AppRegistryTreeNames.ComposeKey(owner.Tenant, owner.Slug), cancellationToken).ConfigureAwait(false);
        return read.Record is { } record
            && record.State != AppRegistryLifecycleState.Uninstalled
            && string.Equals(record.Provenance?.Publisher, owner.Publisher, StringComparison.Ordinal);
    }

    private static AppTreeOwnershipConflict Held(AppTreeClaimPlan item, AppTreeClaim holder, AppTreeOwner claimant) =>
        new(item.TreeName, AppTreeOwnershipConflictReason.OwnedByAnotherApp, holder.Slug,
            holder.Slug == claimant.Slug
                ? $"Tree '{item.TreeName}' is owned by another install of app '{holder.Slug}' from a different publisher."
                : $"Tree '{item.TreeName}' is owned by app '{holder.Slug}'.");

    private enum Standing
    {
        Free,
        Own,
        Held,
    }
}
