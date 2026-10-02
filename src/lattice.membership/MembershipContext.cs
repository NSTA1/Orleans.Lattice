using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The real <see cref="ILatticeMembershipContext"/>: resolves the ambient
/// <see cref="LatticeCredentialContext"/> into a <see cref="LatticeSubject"/> by
/// selecting the first authenticator that recognizes the credential, mapping the
/// resulting principal against the local directory, and expanding groups to the
/// full transitive closure. Resolution is served from the per-silo
/// <see cref="MembershipResolutionCache"/> once warm.
/// </summary>
internal sealed class MembershipContext : ILatticeMembershipContext
{
    private static readonly ValueTask<LatticeSubject> AnonymousResult = new(LatticeSubject.Anonymous);

    private readonly ILatticeCredentialAuthenticator[] _authenticators;
    private readonly ILatticeSubjectMapper _mapper;
    private readonly ILatticeMembershipDirectory _directory;
    private readonly MembershipResolutionCache _cache;
    private readonly IOptionsMonitor<LatticeMembershipOptions> _options;
    private readonly ITenantGroupClaimFilter _claimFilter;

    /// <summary>
    /// Initializes a new <see cref="MembershipContext"/> with the inactive tenant
    /// group claim filter, so resolution behaves exactly as it did before the
    /// filter seam existed.
    /// </summary>
    /// <param name="authenticators">The registered credential authenticators, tried in registration order.</param>
    /// <param name="mapper">The subject mapper that merges principal and directory groups.</param>
    /// <param name="directory">The membership directory.</param>
    /// <param name="cache">The per-silo resolution cache.</param>
    /// <param name="options">The membership options monitor.</param>
    public MembershipContext(
        IEnumerable<ILatticeCredentialAuthenticator> authenticators,
        ILatticeSubjectMapper mapper,
        ILatticeMembershipDirectory directory,
        MembershipResolutionCache cache,
        IOptionsMonitor<LatticeMembershipOptions> options)
        : this(authenticators, mapper, directory, cache, options, new NullTenantGroupClaimFilter())
    {
    }

    /// <summary>Initializes a new <see cref="MembershipContext"/>.</summary>
    /// <param name="authenticators">The registered credential authenticators, tried in registration order.</param>
    /// <param name="mapper">The subject mapper that merges principal and directory groups.</param>
    /// <param name="directory">The membership directory.</param>
    /// <param name="cache">The per-silo resolution cache.</param>
    /// <param name="options">The membership options monitor.</param>
    /// <param name="claimFilter">
    /// The tenant group claim filter consulted, when active, on the claim-derived
    /// groups before expansion.
    /// </param>
    public MembershipContext(
        IEnumerable<ILatticeCredentialAuthenticator> authenticators,
        ILatticeSubjectMapper mapper,
        ILatticeMembershipDirectory directory,
        MembershipResolutionCache cache,
        IOptionsMonitor<LatticeMembershipOptions> options,
        ITenantGroupClaimFilter claimFilter)
    {
        ArgumentNullException.ThrowIfNull(authenticators);
        ArgumentNullException.ThrowIfNull(mapper);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(claimFilter);
        _authenticators = authenticators.ToArray();
        _mapper = mapper;
        _directory = directory;
        _cache = cache;
        _options = options;
        _claimFilter = claimFilter;
    }

    /// <summary>The tenant group claim filter this context consults.</summary>
    internal ITenantGroupClaimFilter ClaimFilter => _claimFilter;

    /// <inheritdoc />
    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        if (!LatticeCredentialContext.IsActive)
        {
            return AnonymousResult;
        }

        var credential = LatticeCredentialContext.Current!.Value;

        // The key covers the whole credential, not just its token: authenticator
        // selection reads Scheme, and the credential contract lets an
        // authenticator resolve from PrincipalId or Metadata. Keying on the
        // token alone would serve one credential's subject to a different one.
        var cacheKey = MembershipCacheKey.For(credential);

        // Warm fast path: avoid allocating the cache-miss resolver closure when
        // the subject is already cached and still within its freshness bound.
        if (_cache.TryGetCached(cacheKey, out var cached))
        {
            return new ValueTask<LatticeSubject>(cached);
        }

        return _cache.ResolveAsync(
            cacheKey,
            ct => ResolveUncachedAsync(credential, ct),
            cancellationToken);
    }

    /// <inheritdoc />
    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        // No credential on the ambient context resolves to anonymous with no
        // directory read, so it is always available synchronously.
        if (!LatticeCredentialContext.IsActive)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }

        // A warm cache hit serves the subject without re-authenticating or
        // touching the directory; a miss returns false so the caller takes the
        // async, gate-bypassing resolution path.
        var cacheKey = MembershipCacheKey.For(LatticeCredentialContext.Current!.Value);
        return _cache.TryGetCached(cacheKey, out subject);
    }

    private async ValueTask<ResolvedSubject> ResolveUncachedAsync(LatticeCredential credential, CancellationToken cancellationToken)
    {
        ILatticeCredentialAuthenticator? selected = null;
        foreach (var authenticator in _authenticators)
        {
            if (authenticator.CanHandle(credential))
            {
                selected = authenticator;
                break;
            }
        }

        if (selected is null)
        {
            return new ResolvedSubject(LatticeSubject.Anonymous, null);
        }

        var principal = await selected.AuthenticateAsync(credential, cancellationToken).ConfigureAwait(false);
        if (principal is null)
        {
            // Invalid or expired credential: anonymous, never a stale subject.
            return new ResolvedSubject(LatticeSubject.Anonymous, null);
        }

        var mergeMode = _options.CurrentValue.GroupMergeMode;
        var opts = _options.CurrentValue;
        IReadOnlyCollection<string> directoryGroups = mergeMode == SubjectGroupMergeMode.TokenOnly
            ? Array.Empty<string>()
            : await _directory.GroupsOfAsync(principal.SubjectId, cancellationToken).ConfigureAwait(false);

        var subject = _mapper.Map(principal, directoryGroups);

        // Strip claim-asserted t/ group ids before they are expanded (epic
        // #4154, D2). Inactive (tenancy absent): one bool read. Active (tenancy
        // registered, whatever its flag): one prefix test per group.
        subject = ApplyTenantGroupClaimFilter(subject, directoryGroups);

        // The directory groups above are already transitively expanded, but the
        // mapper also unions in token-asserted and claim-projected seed groups
        // that are not. Unless the directory is being ignored entirely
        // (TokenOnly), run the merged set back through the directory closure so a
        // nested policy on an ancestor group still applies to a federated
        // identity that carries only the child group in its token. Skipped when
        // no such unexpanded seeds exist, keeping the pure-directory path to a
        // single directory round-trip.
        var hasUnexpandedSeeds =
            (mergeMode != SubjectGroupMergeMode.DirectoryOnly && principal.AssertedGroups is { Count: > 0 })
            || opts.ClaimToGroups is not null;
        if (mergeMode != SubjectGroupMergeMode.TokenOnly && hasUnexpandedSeeds && subject.GroupIds.Count > 0)
        {
            var expanded = await _directory.ExpandGroupsAsync(subject.GroupIds, cancellationToken).ConfigureAwait(false);
            subject = subject with { GroupIds = expanded };
        }

        return new ResolvedSubject(subject, principal.ExpiresAt);
    }

    /// <summary>
    /// Returns <paramref name="subject"/> unchanged (its group set is handed back,
    /// not copied, and nothing is allocated) unless the tenant group claim filter
    /// is active and a claim-derived group is in the reserved <c>t/</c> namespace.
    /// The filter is active whenever the tenancy add-on is registered, whatever
    /// its delegated tenant access administration flag says. When active, the
    /// claim-derived groups (every group the mapper added beyond
    /// <paramref name="directoryGroups"/>: token-asserted, overage and
    /// claim-projected ids) are first scanned with one ordinal prefix test each,
    /// allocating nothing; only when one is a <c>t/</c> id are they run through
    /// the filter, and those it removes are dropped from the subject.
    /// Directory-derived groups are never filtered: a tenant group the directory
    /// records the subject in is a real membership. This runs on the cold
    /// (cache-miss) resolution path only.
    /// </summary>
    /// <param name="subject">The mapped subject.</param>
    /// <param name="directoryGroups">The directory-derived groups the subject was mapped with.</param>
    /// <returns>The subject to expand.</returns>
    internal LatticeSubject ApplyTenantGroupClaimFilter(LatticeSubject subject, IReadOnlyCollection<string> directoryGroups)
    {
        if (!_claimFilter.IsActive)
        {
            return subject;
        }

        return StripClaimDerivedTenantGroups(subject, directoryGroups);
    }

    private LatticeSubject StripClaimDerivedTenantGroups(LatticeSubject subject, IReadOnlyCollection<string> directoryGroups)
    {
        // The filter is active whenever the tenancy add-on is registered, so this
        // runs on every cold resolution there. Find a claim-derived t/ id first
        // (one ordinal prefix test per group, no allocation) and only build the
        // claim-derived list when there is something to strip.
        if (!HasClaimDerivedTenantTierGroup(subject.GroupIds, directoryGroups))
        {
            return subject;
        }

        List<string>? claimDerived = null;
        foreach (var group in subject.GroupIds)
        {
            if (!directoryGroups.Contains(group))
            {
                (claimDerived ??= new List<string>()).Add(group);
            }
        }

        if (claimDerived is null)
        {
            return subject;
        }

        var before = claimDerived.Count;
        _claimFilter.Filter(claimDerived);
        if (claimDerived.Count == before)
        {
            return subject;
        }

        var kept = new HashSet<string>(claimDerived, StringComparer.Ordinal);
        var groups = new HashSet<string>(StringComparer.Ordinal);
        foreach (var group in subject.GroupIds)
        {
            if (kept.Contains(group) || directoryGroups.Contains(group))
            {
                groups.Add(group);
            }
        }

        return subject with { GroupIds = groups };
    }

    private static bool HasClaimDerivedTenantTierGroup(IReadOnlyCollection<string> groups, IReadOnlyCollection<string> directoryGroups)
    {
        // The mapper hands back a HashSet; enumerate the concrete type so the
        // struct enumerator is not boxed and the scan allocates nothing.
        switch (groups)
        {
            case HashSet<string> set:
                foreach (var group in set)
                {
                    if (IsClaimDerivedTenantTier(group, directoryGroups))
                    {
                        return true;
                    }
                }

                return false;
            case string[] array:
                foreach (var group in array)
                {
                    if (IsClaimDerivedTenantTier(group, directoryGroups))
                    {
                        return true;
                    }
                }

                return false;
        }

        foreach (var group in groups)
        {
            if (IsClaimDerivedTenantTier(group, directoryGroups))
            {
                return true;
            }
        }

        return false;
    }

    // The prefix test runs first, so the directory lookup is paid only for a t/ id.
    private static bool IsClaimDerivedTenantTier(string group, IReadOnlyCollection<string> directoryGroups) =>
        TenantGroupNesting.IsTenantTier(group) && !directoryGroups.Contains(group);
}
