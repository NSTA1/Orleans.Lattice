using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>Grants and revocations a journey makes on a tree of its own (issue #5428).</summary>
internal sealed partial class ExplorerWorld
{
    /// <summary>Grants <paramref name="user"/> read and range read on the whole of <paramref name="treeId"/>.</summary>
    /// <param name="user">The user.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="ruleId">The rule's id, which <see cref="RevokeAsync"/> takes back.</param>
    public async Task GrantReadAsync(string user, string treeId, string ruleId)
    {
        var policy = Head.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        using (LatticeSystemOrigin.Enter())
        {
            await policy.PutRuleAsync(new LatticeAuthorizationRule(
                ruleId: ruleId,
                subject: LatticeSubjectSelector.User(user),
                scope: LatticeScope.Tree(treeId),
                operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                effect: LatticeEffect.Allow));
        }
    }

    /// <summary>Takes back the rule <see cref="GrantReadAsync"/> wrote.</summary>
    /// <param name="treeId">The tree.</param>
    /// <param name="ruleId">The rule's id.</param>
    public async Task RevokeAsync(string treeId, string ruleId)
    {
        var policy = Head.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        using (LatticeSystemOrigin.Enter())
        {
            await policy.RemoveRuleAsync(treeId, ruleId);
        }
    }
}
