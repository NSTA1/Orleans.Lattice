using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The writes that replace an app's stored owned rule set with a freshly compiled one:
/// rules to put (new or changed) and stored owned rules to remove (no longer compiled).
/// Rules outside the app's owned id prefix are never included.
/// </summary>
public sealed class AppRuleSetDiff
{
    internal AppRuleSetDiff(
        IReadOnlyList<LatticeAuthorizationRule> toUpsert,
        IReadOnlyList<LatticeAuthorizationRule> toDelete)
    {
        ToUpsert = toUpsert;
        ToDelete = toDelete;
    }

    /// <summary>Compiled rules absent from the stored set, or stored with different content, ordered by rule id.</summary>
    public IReadOnlyList<LatticeAuthorizationRule> ToUpsert { get; }

    /// <summary>
    /// Stored owned rules no longer in the compiled set, ordered by rule id. Remove each by its
    /// <see cref="LatticeScope.TreeId"/> and <see cref="LatticeAuthorizationRule.RuleId"/>.
    /// </summary>
    public IReadOnlyList<LatticeAuthorizationRule> ToDelete { get; }

    /// <summary><c>true</c> when the stored owned set already equals the compiled set.</summary>
    public bool IsEmpty => ToUpsert.Count == 0 && ToDelete.Count == 0;
}
