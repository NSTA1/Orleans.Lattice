using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>Rules a journey writes through the API, as an administrator scripting the policy would.</summary>
internal sealed partial class ExplorerWorld
{
    /// <summary>
    /// Writes <paramref name="rule"/> straight to the policy store, bypassing the
    /// Explorer, so a journey can show what the console does with a rule it would
    /// not author itself (a condition, issue #5426).
    /// </summary>
    /// <param name="rule">The rule to write, replacing any with its id in its tree.</param>
    public async Task PutRuleAsync(LatticeAuthorizationRule rule)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await Head.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>().PutRuleAsync(rule);
        }
    }

    /// <summary>
    /// Removes the rule <paramref name="ruleId"/> from <paramref name="treeId"/> if there is
    /// one, so a journey that writes it leaves the world as it found it.
    /// </summary>
    /// <param name="treeId">The tree the rule governs.</param>
    /// <param name="ruleId">The rule id.</param>
    public async Task RemoveRuleAsync(string treeId, string ruleId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await Head.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>().RemoveRuleAsync(treeId, ruleId);
        }
    }
}
