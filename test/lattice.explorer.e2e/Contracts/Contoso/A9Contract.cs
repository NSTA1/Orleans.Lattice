using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A9, access in depth (#5373). Consumes the policy-as-code pipeline's 230 rules
/// (<see cref="ContosoHarness.SyncPolicyRulesAsync"/>), among them the conditional
/// <c>factory-conditional-legacy</c>. Produces the nested group
/// <see cref="ContosoAccess.FactoryStaff"/> and its telemetry read rule, and no scratch rule
/// (no rule id starting <see cref="ContosoAccess.ScratchRulePrefix"/>).
/// </summary>
public sealed class A9Contract() : ChapterContract("A9")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        await ContosoHarness.SyncPolicyRulesAsync(context.Estate);
        var auth = ContractFacades.Of(context).Auth;
        await ContractCalls.UpsertGroupAsync(auth, ContosoAccess.FactoryStaff);
        await ContractCalls.PutRuleAsync(auth, ContosoAccess.FactoryStaffTelemetryRead);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var auth = ContractFacades.Of(context).Auth;
        var staff = ContosoAccess.FactoryStaff;
        if (context.IsProduced("C3"))
        {
            // C3 deletes data-stewards, so the nested group keeps only the auditors.
            staff = staff with { Groups = [.. staff.Groups.Where(group => group != StoryNames.Contoso.Groups.DataStewards)] };
        }

        Require(await ContractCalls.GroupHoldsAsync(auth, staff), $"factory-staff holds {string.Join(", ", staff.Groups)}");
        Require(await ContractCalls.RuleHoldsAsync(auth, ContosoAccess.FactoryStaffTelemetryRead), "factory-staff-telemetry-read allows factory-staff to read factory-telemetry");

        var rules = await ContractCalls.AllRulesAsync(auth);
        var scratch = rules.Where(rule => rule.RuleId.StartsWith(ContosoAccess.ScratchRulePrefix, StringComparison.Ordinal)).Select(rule => rule.RuleId).ToList();
        Require(scratch.Count == 0, $"no scratch rule remains (found {string.Join(", ", scratch)})");
        Require(
            rules.Any(rule => rule.RuleId == ContosoHarness.ConditionalLegacyRule && rule.Condition == ContosoHarness.ConditionalLegacyCondition),
            "the pipeline's conditional rule factory-conditional-legacy is kept as it is");
        Require(
            Enumerable.Range(1, StoryNames.Contoso.Rules.SyncedRuleCount - 1).All(number => rules.Any(rule => rule.RuleId == ContosoHarness.SyncedRuleId(number))),
            "the pipeline's synced rules are kept");
    }
}
