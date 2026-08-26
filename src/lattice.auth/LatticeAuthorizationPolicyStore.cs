using System.Runtime.CompilerServices;
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
/// The store is authorization <b>infrastructure</b>: it reads and writes the
/// policy tree that feeds the enforcement gate itself, so every operation runs
/// under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>. This both
/// avoids a bootstrap paradox (the very first rule cannot be authorized by a
/// rule that does not exist yet) and breaks the re-entrancy cycle where the
/// compiled-snapshot maintainer's own scan of the policy tree would otherwise
/// call back into a cold gate and deadlock. Authorizing <i>who</i> may edit
/// policy is a higher-layer concern (a bootstrap administrator or an admin API
/// grain), not the store's.
/// </remarks>
internal sealed class LatticeAuthorizationPolicyStore(
    IGrainFactory grainFactory,
    AuthInitializer initializer,
    IOptionsMonitor<LatticeAuthOptions> options) : ILatticeAuthorizationPolicyStore
{
    private ILattice Policy => grainFactory.GetGrain<ILattice>(AuthConstants.PolicyTree);

    /// <inheritdoc />
    public async Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);

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
