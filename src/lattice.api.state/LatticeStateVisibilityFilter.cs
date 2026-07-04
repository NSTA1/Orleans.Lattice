using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Resolves the caller subject and decides which trees the state API may expose
/// to that subject, so the read-only surfaces that do <em>not</em> flow through
/// the gated <see cref="ILattice"/> data-plane (the catalog, structure, and
/// per-tree summary reads) never leak the existence or shape of data the caller
/// cannot read. The per-entry / per-key reads (<c>GetEntry</c>, <c>ScanEntries</c>,
/// history, tag-member scans) are already filtered by the core enforcement wired
/// into the public <see cref="ILattice"/> surface once the caller identity flows
/// on the ambient <see cref="LatticeCredentialContext"/>; this filter adds the
/// tree-level visibility decisions that surface does not make.
/// </summary>
/// <remarks>
/// <para>
/// <b>Precedence.</b> This runs after (and independently of) the transport-level
/// <c>ILatticeStateApiAuthorizer</c> (the coarse allow / deny gate on the gRPC
/// binding, keyed by headers, operation, and target tree). The transport gate
/// decides whether a call may run at all; this filter then prunes the results of
/// a call that was allowed to run.
/// </para>
/// <para>
/// <b>Zero-cost default.</b> When no real access gate is registered (the core
/// default <see cref="NullLatticeAccessGate"/>), or the host opted out with
/// <see cref="LatticeStateApiReadVisibility.Disabled"/>, <see cref="Enabled"/> is
/// <see langword="false"/>: no subject is resolved, no gate is consulted, and the
/// state API behaves byte-for-byte as it did before authorization.
/// </para>
/// <para>
/// <b>Fail-closed.</b> When enabled and the caller identity cannot be resolved
/// (an anonymous subject), every read is denied - the catalog is empty and every
/// tree-scoped read reports not-found - regardless of the policy's default
/// effect, so an unauthenticated state-API caller can never read cluster state.
/// </para>
/// <para>
/// <b>Non-recursion.</b> Subject resolution reads the membership directory's own
/// dogfooded trees through the gated surface, so on a subject-cache miss it runs
/// under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> to bypass the
/// gate and avoid re-entering it; the warm cached/anonymous path resolves
/// synchronously with no scope and no directory read. The per-tree read decision
/// (<see cref="CanReadTreeAsync"/>) is a pure in-memory policy evaluation that
/// performs no tree reads, so it is safe to call outside a system-origin scope
/// (and its verdict is unaffected by one).
/// </para>
/// </remarks>
internal sealed class LatticeStateVisibilityFilter
{
    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;
    private readonly ILatticeReadGrantProbe? _grantProbe;

    /// <summary>
    /// Builds the filter from the silo service provider and the resolved state
    /// API options. Resolves the registered access gate and membership context
    /// once; both are silo singletons.
    /// </summary>
    public LatticeStateVisibilityFilter(IServiceProvider services, LatticeApiStateOptions options)
        : this(
            (services ?? throw new ArgumentNullException(nameof(services)))
                .GetService<ILatticeAccessGate>() ?? NullGate,
            services.GetService<ILatticeMembershipContext>(),
            (options ?? throw new ArgumentNullException(nameof(options))).ReadVisibility,
            services.GetService<ILatticeReadGrantProbe>())
    {
    }

    /// <summary>
    /// Test-friendly constructor taking the collaborators directly, so the
    /// filter's decisions can be asserted without standing up a cluster.
    /// </summary>
    internal LatticeStateVisibilityFilter(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        LatticeStateApiReadVisibility visibility,
        ILatticeReadGrantProbe? grantProbe = null)
    {
        _gate = gate ?? throw new ArgumentNullException(nameof(gate));
        _membership = membership;
        _grantProbe = grantProbe;
        Enabled = visibility != LatticeStateApiReadVisibility.Disabled && gate is not NullLatticeAccessGate;
    }

    private static readonly ILatticeAccessGate NullGate = new NullLatticeAccessGate();

    /// <summary>
    /// <see langword="true"/> when auth-backed visibility filtering is active: a
    /// real access gate is registered and the host did not opt out. When
    /// <see langword="false"/> the state API does no subject resolution and no
    /// per-tree filtering.
    /// </summary>
    public bool Enabled { get; }

    /// <summary>
    /// Resolves the caller subject for a state-API read, or <see langword="null"/>
    /// when <see cref="Enabled"/> is <see langword="false"/> (the caller then
    /// applies no filtering). Resolution runs under a system-origin scope - entered
    /// only on a subject-cache miss - so the membership directory's own reads
    /// bypass the gate and cannot recurse.
    /// </summary>
    public ValueTask<LatticeSubject?> ResolveSubjectAsync(CancellationToken cancellationToken)
    {
        if (!Enabled)
        {
            return new ValueTask<LatticeSubject?>((LatticeSubject?)null);
        }

        return ResolveEnabledAsync(cancellationToken);
    }

    private async ValueTask<LatticeSubject?> ResolveEnabledAsync(CancellationToken cancellationToken) =>
        await LatticeAccessGateSubjectResolver.ResolveAsync(_membership, cancellationToken)
            .ConfigureAwait(false);

    /// <summary>
    /// <see langword="true"/> when every read must be denied for
    /// <paramref name="subject"/> because the caller identity is unresolved
    /// (anonymous). Applied regardless of the policy's default effect so an
    /// unauthenticated caller is always fail-closed.
    /// </summary>
    public static bool DeniesAllReads(in LatticeSubject subject) => subject.IsAnonymous;

    /// <summary>
    /// <see langword="true"/> when <paramref name="subject"/> may read at least
    /// one key of <paramref name="treeId"/> (a whole-tree or partial / prefix
    /// grant both count as visible); <see langword="false"/> when the subject has
    /// no read access to the tree at all. The underlying per-key filtering for a
    /// partial grant is applied automatically by the gated data-plane surface, so
    /// this only decides whether the tree itself is visible.
    /// </summary>
    public async ValueTask<bool> CanReadTreeAsync(
        string treeId,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        if (subject.IsAnonymous)
        {
            return false;
        }

        var request = new LatticeAccessRequest(treeId, LatticeOperation.RangeRead, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        return decision.Allowed;
    }

    /// <summary>
    /// <see langword="true"/> when <paramref name="subject"/> can read at least
    /// one key of <paramref name="treeId"/> - the structural existence-hiding
    /// signal. Unlike <see cref="CanReadTreeAsync"/>, this distinguishes a subject
    /// with a partial (prefix) grant, which must keep the tree visible, from a
    /// subject with no grant at all, which must not learn the tree exists: a
    /// per-key gate decision reports allow-with-filter for both once the tree
    /// carries per-key rules. Uses the registered
    /// <see cref="ILatticeReadGrantProbe"/> when present; when it is absent (no
    /// auth add-on, so nothing is being hidden) it falls back to the plain
    /// per-tree decision. An anonymous subject is always <see langword="false"/>.
    /// </summary>
    public async ValueTask<bool> CanReadAnyKeyAsync(
        string treeId,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        if (subject.IsAnonymous)
        {
            return false;
        }

        if (_grantProbe is { } probe)
        {
            return await probe
                .HasAnyGrantAsync(treeId, subject, LatticeOperation.RangeRead, cancellationToken)
                .ConfigureAwait(false);
        }

        return await CanReadTreeAsync(treeId, subject, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves change-feed read access for a whole tree in a single gate
    /// evaluation: whether <paramref name="subject"/> may observe the tree at all
    /// (<c>Allowed</c>) and, for a partial (prefix) grant, the per-key predicate
    /// (<c>KeyFilter</c>, keeping a key when it returns <see langword="true"/>) the
    /// caller applies to each emitted change so a partially-authorized subscriber
    /// only observes changes to keys it may read. A whole-tree grant returns a
    /// <see langword="null"/> filter (emit every key); an anonymous or unauthorized
    /// subject returns <c>Allowed == false</c>. Unlike the per-entry reads, the
    /// change feed does not flow through the gated <see cref="ILattice"/> surface
    /// (it tails the write-ahead log directly), so this filter is the only point
    /// that honours the data-plane read policy for the live stream.
    /// </summary>
    internal async ValueTask<(bool Allowed, Func<string, bool>? KeyFilter)> ResolveTreeReadAccessAsync(
        string treeId,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        if (subject.IsAnonymous)
        {
            return (false, null);
        }

        var request = new LatticeAccessRequest(treeId, LatticeOperation.RangeRead, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        return (decision.Allowed, decision.KeyFilter);
    }
}
