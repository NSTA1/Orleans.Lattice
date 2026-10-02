using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The default <see cref="ILatticeAuthorizationPolicyStore"/>. Dogfoods the
/// reserved <c>sys-auth-policy</c> <c>ILattice</c> tree: each rule is stored as a
/// JSON value under the composite key <c>{treeId}\u001f{ruleId}</c>, so a tree's
/// rules form a contiguous prefix range that <see cref="ListRulesForTreeAsync"/>
/// scans directly, and <see cref="ListRulesAsync"/> is a full-tree scan. Every
/// mutation runs through the standard write path, so it is durably captured by
/// the per-key history view created at bootstrap.
/// </summary>
/// <remarks>
/// <para>
/// The store is authorization <b>infrastructure</b>: it reads and writes the
/// policy tree that feeds the enforcement gate itself, so every operation runs
/// under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>. This both
/// avoids a bootstrap paradox (the very first rule cannot be authorized by a
/// rule that does not exist yet) and breaks the re-entrancy cycle where the
/// compiled-snapshot maintainer's own scan of the policy tree would otherwise
/// call back into a cold gate and deadlock. Authorizing <i>who</i> may edit
/// policy is a higher-layer concern (a bootstrap administrator or an admin API
/// grain), not the store's.
/// </para>
/// <para>
/// The one origin-sensitive rule the store itself enforces is app-owned rule
/// protection: a write or delete of a rule id in the
/// <see cref="LatticeAppRuleIds.Prefix"/> namespace is admitted only when the
/// caller is already system-origin (the app compiler), and is otherwise rejected
/// with <see cref="LatticeAppOwnedRuleException"/>. The store exposes no bulk,
/// replace, or import verb, so <see cref="PutRuleAsync"/> and
/// <see cref="RemoveRuleAsync"/> are the only mutation paths the guard needs to
/// cover. Replication applies, backup restores, and other infrastructure paths
/// that write the reserved policy tree directly do so under system origin rather
/// than through this store (so they may carry app-owned rules unimpeded), and a
/// user-origin write to that reserved <c>sys-</c> tree is refused by the core
/// library, so the guard cannot be sidestepped by writing the tree around the
/// store.
/// </para>
/// <para>
/// The tenant-tier namespace (<see cref="LatticeTenantRuleIds.Prefix"/>) is
/// guarded identically, raising <see cref="LatticeTenantOwnedRuleException"/> off
/// system origin before anything is read. Independently of origin, every write is
/// checked by <see cref="TenantRuleConfinement.EnsureConfined"/>: a tenant-tier
/// rule must be confined to its own tenant (D7), a tenant-wide scope is accepted
/// only on a tenant-tier rule (D9), and a tenant group subject only on its tenant's
/// trees (D4), for operators and system-origin writers alike. The internal
/// <see cref="ITenantPolicyRuleStore.PurgeTenantRulesAsync"/> removes a deleted
/// tenant's tenant-tier rules.
/// </para>
/// </remarks>
internal sealed class LatticeAuthorizationPolicyStore(
    IGrainFactory grainFactory,
    AuthInitializer initializer,
    IOptionsMonitor<LatticeAuthOptions> options) : ILatticeAuthorizationPolicyStore, ITenantPolicyRuleStore
{
    private ILattice Policy => grainFactory.GetGrain<ILattice>(AuthConstants.PolicyTree);

    /// <inheritdoc />
    public async Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        EnsureAppOwnedRuleWritable(rule.RuleId, nameof(rule));
        EnsureTenantOwnedRuleWritable(rule.RuleId, nameof(rule));

        // Tenant confinement (D4, D7, D9): where a tenant-tier rule, a tenant-wide
        // scope, and a tenant group subject may appear. Applies to every caller,
        // system origin included, and reads nothing.
        TenantRuleConfinement.EnsureConfined(rule);

        // Authoring guard (the single seam that decides whether a reserved-namespace
        // rule may be persisted): an ordinary tree is always authorable; the reserved
        // sys-auth-* namespace is rejected fail-closed except for the whole-tree Admin
        // delegation grant on the policy tree, and only when the operator has opted in.
        AuthConstants.EnsureAuthorableRuleScope(
            rule,
            options.CurrentValue.AccessAdministrationDelegationEnabled,
            options.CurrentValue.AllTreesGrantsEnabled);

        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Policy.SetAsync(RuleKey(rule.Scope.TreeId, rule.RuleId), rule, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Policy.GetAsync<LatticeAuthorizationRule>(RuleKey(treeId, ruleId), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        EnsureAppOwnedRuleWritable(ruleId, nameof(ruleId));
        EnsureTenantOwnedRuleWritable(ruleId, nameof(ruleId));
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Policy.DeleteAsync(RuleKey(treeId, ruleId), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
        string treeId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var prefix = TreePrefix(treeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Policy
                .ScanEntriesAsync<LatticeAuthorizationRule>(prefix, PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is { } rule)
                {
                    yield return rule;
                }
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // ScanEntriesAsync (not EntriesAsync) so the scan transparently recovers
        // from a mid-flight Orleans.Runtime.EnumerationAbortedException without
        // duplicates or gaps. The compiled-policy snapshot maintainer rescans this
        // same policy tree in the background on every edit, so a caller's list scan
        // routinely overlaps a maintainer scan; the resilient scan converges rather
        // than surfacing the transient abort. The scan runs under system-origin so
        // it bypasses the enforcement gate it feeds (see the type remarks).
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // A never-written policy tree is unregistered and holds no rules. Reading
            // it would register it, and the gate's cold warm-up can run inside the
            // tree registry's non-interleaved SetAliasAsync turn, where that
            // registration queues behind the turn awaiting it (issue 4128).
            if (!await grainFactory.GetLatticeRegistry().ExistsAsync(AuthConstants.PolicyTree).ConfigureAwait(false))
            {
                yield break;
            }

            await foreach (var entry in Policy
                .ScanEntriesAsync<LatticeAuthorizationRule>(cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is { } rule)
                {
                    yield return rule;
                }
            }
        }
    }

    /// <summary>
    /// App-owned rule write guard: the single seam that decides whether a rule id in
    /// the <see cref="LatticeAppRuleIds.Prefix"/> namespace may be written or deleted.
    /// It is admitted only when the caller is already inside a system-origin scope
    /// (the app compiler), and rejected fail-closed otherwise, before any read or
    /// write is issued, so a rejected delete does not disclose whether the rule
    /// exists. The check must run against the <i>caller's</i> ambient origin, so it
    /// precedes the store's own system-origin scope. Allocation-free on the ordinary
    /// (non-app) path: an ordinal prefix test and nothing else.
    /// </summary>
    /// <param name="ruleId">The targeted rule id.</param>
    /// <param name="paramName">The store parameter the id came from.</param>
    /// <exception cref="LatticeAppOwnedRuleException">The id is app-owned and the caller is not system-origin.</exception>
    private static void EnsureAppOwnedRuleWritable(string ruleId, string paramName)
    {
        if (LatticeAppRuleIds.IsAppOwned(ruleId) && !LatticeAccessGateContext.IsSystemOrigin)
        {
            throw LatticeAppOwnedRuleException.Rejected(ruleId, paramName);
        }
    }

    /// <summary>
    /// Tenant-tier rule write guard, mirroring <see cref="EnsureAppOwnedRuleWritable"/>:
    /// a rule id in the <see cref="LatticeTenantRuleIds.Prefix"/> namespace may be
    /// written or deleted only from inside a system-origin scope (the tenant policy
    /// facade, after it has confined the rule to its tenant), and is rejected
    /// fail-closed otherwise, before any read or write, so a rejected delete does not
    /// disclose whether the rule exists. Allocation-free on the ordinary path.
    /// </summary>
    /// <param name="ruleId">The targeted rule id.</param>
    /// <param name="paramName">The store parameter the id came from.</param>
    /// <exception cref="LatticeTenantOwnedRuleException">The id is tenant-tier and the caller is not system-origin.</exception>
    private static void EnsureTenantOwnedRuleWritable(string ruleId, string paramName)
    {
        if (LatticeTenantRuleIds.IsTenantOwned(ruleId) && !LatticeAccessGateContext.IsSystemOrigin)
        {
            throw LatticeTenantOwnedRuleException.Rejected(ruleId, paramName);
        }
    }

    /// <inheritdoc />
    public Task<int> PurgeTenantRulesAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
        TenantRulePurge.PurgeAsync(this, tenant, cancellationToken);

    private static string RuleKey(string treeId, string ruleId) =>
        string.Create(
            treeId.Length + 1 + ruleId.Length,
            (treeId, ruleId),
            static (span, state) =>
            {
                var pos = 0;
                state.treeId.AsSpan().CopyTo(span);
                pos += state.treeId.Length;
                span[pos++] = AuthConstants.RuleKeySeparator;
                state.ruleId.AsSpan().CopyTo(span[pos..]);
            });

    private static string TreePrefix(string treeId) =>
        $"{treeId}{AuthConstants.RuleKeySeparator}";

    /// <summary>
    /// The exclusive upper bound of every key sharing <paramref name="prefix"/>,
    /// or <see langword="null"/> when the prefix has no finite upper bound
    /// (every code unit is <see cref="char.MaxValue"/>), meaning the scan is
    /// open-ended above. Delegates to the shared
    /// <see cref="LatticeKeyRange.PrefixUpperBound(string)"/> so the rollover-safe
    /// algorithm has a single definition.
    /// </summary>
    internal static string? PrefixUpperBound(string prefix) =>
        LatticeKeyRange.PrefixUpperBound(prefix);
}
