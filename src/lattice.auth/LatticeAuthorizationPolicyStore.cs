using System.Runtime.CompilerServices;

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
internal sealed class LatticeAuthorizationPolicyStore(
    IGrainFactory grainFactory,
    AuthInitializer initializer) : ILatticeAuthorizationPolicyStore
{
    private ILattice Policy => grainFactory.GetGrain<ILattice>(AuthConstants.PolicyTree);

    /// <inheritdoc />
    public async Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        AuthConstants.ThrowIfReservedTree(rule.Scope.TreeId, "rule.Scope.TreeId");

        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        await Policy.SetAsync(RuleKey(rule.Scope.TreeId, rule.RuleId), rule, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        return Policy.GetAsync<LatticeAuthorizationRule>(RuleKey(treeId, ruleId), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await Policy.DeleteAsync(RuleKey(treeId, ruleId), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
        string treeId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var prefix = TreePrefix(treeId);
        foreach (var rule in await ScanAsync(prefix, PrefixUpperBound(prefix), cancellationToken).ConfigureAwait(false))
        {
            yield return rule;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var rule in await ScanAsync(startInclusive: null, endExclusive: null, cancellationToken).ConfigureAwait(false))
        {
            yield return rule;
        }
    }

    // A streaming scan over the policy tree grain can be aborted mid-flight when a
    // concurrent enumeration is active over the same tree (the compiled-policy
    // snapshot maintainer rescans the policy tree in the background on every
    // policy edit, so a caller's list scan now routinely overlaps a maintainer
    // scan). Orleans surfaces that as an enumeration-aborted error. It is
    // transient, so the scan is buffered and retried as a whole with a short
    // backoff; the policy rule set is bounded and small, so eager buffering is
    // cheap and the retry converges once the overlapping scan drains.
    private const int MaxScanAttempts = 8;

    private async Task<List<LatticeAuthorizationRule>> ScanAsync(
        string? startInclusive,
        string? endExclusive,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var rules = new List<LatticeAuthorizationRule>();
                await foreach (var entry in Policy
                    .EntriesAsync<LatticeAuthorizationRule>(startInclusive, endExclusive, cancellationToken: cancellationToken)
                    .ConfigureAwait(false))
                {
                    if (entry.Value is { } rule)
                    {
                        rules.Add(rule);
                    }
                }

                return rules;
            }
            catch (Exception ex) when (attempt < MaxScanAttempts && IsTransientScanFailure(ex))
            {
                await Task.Delay(25 * attempt, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// A streaming scan over a grain can be aborted when its server-side
    /// enumerator is evicted by a concurrent enumeration over the same tree or the
    /// grain deactivates between pages. Orleans reports this as an
    /// enumeration-aborted error; it is transient and a fresh scan converges.
    /// </summary>
    private static bool IsTransientScanFailure(Exception ex) =>
        ex.GetType().Name == "EnumerationAbortedException";

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
    /// The exclusive upper bound of every key sharing <paramref name="prefix"/>:
    /// the prefix with its final separator advanced to the next code point.
    /// </summary>
    private static string PrefixUpperBound(string prefix)
    {
        var chars = prefix.ToCharArray();
        chars[^1] = (char)(chars[^1] + 1);
        return new string(chars);
    }
}
