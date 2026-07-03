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

    /// <summary>Initializes a new <see cref="MembershipContext"/>.</summary>
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
    {
        ArgumentNullException.ThrowIfNull(authenticators);
        ArgumentNullException.ThrowIfNull(mapper);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(options);
        _authenticators = authenticators.ToArray();
        _mapper = mapper;
        _directory = directory;
        _cache = cache;
        _options = options;
    }

    /// <inheritdoc />
    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        if (!LatticeCredentialContext.IsActive)
        {
            return AnonymousResult;
        }

        var credential = LatticeCredentialContext.Current!.Value;
        var cacheKey = credential.Token ?? string.Empty;

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
}
