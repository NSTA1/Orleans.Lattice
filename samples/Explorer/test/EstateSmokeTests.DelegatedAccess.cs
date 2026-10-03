using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// Delegated tenant access administration in the estate (epic #4154): globex keeps
/// its own group holding alice in its member set, and Explain for alice on globex's
/// invoices is decided by the Platform layer, over globex's own shadowed rule.
/// </summary>
public sealed partial class EstateSmokeTests
{
    [Test]
    public async Task Globex_keeps_its_own_group_in_its_member_set_and_explain_shows_the_platform_layer_deciding()
    {
        using var _ = LatticeCredentialContext.Use(SampleSeeder.BasicToken(SampleIdentities.GlobexAdmin), scheme: DemoBasicAuthenticator.Scheme);
        var directory = _sample.East.Services.GetRequiredService<ILatticeTenantDirectoryAdmin>();
        var policy = _sample.East.Services.GetRequiredService<ILatticeTenantPolicyAdmin>();
        const string Globex = SampleIdentities.GlobexTenant;

        var members = await directory.ListGroupMembersAsync(Globex, SampleIdentities.GlobexOperatorsGroup);
        var memberSet = await directory.ListMembersAsync(Globex, new TenantAccessPageRequest());
        var invoices = await policy.ExplainAsync(Globex, SampleIdentities.Alice, SampleIdentities.GlobexInvoicesTree, null, LatticeOperation.Read);
        var orders = await policy.ExplainAsync(Globex, SampleIdentities.Alice, SampleIdentities.TenantOrdersTree, null, LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(members.Select(member => (member.MemberId, member.Kind)), Is.EqualTo(new[] { (SampleIdentities.Alice, TenantSubjectKind.User) }));
            Assert.That(
                memberSet.Entries.Select(entry => (entry.SubjectId, entry.Kind)),
                Does.Contain((SampleIdentities.GlobexOperatorsGroup, TenantSubjectKind.TenantGroup)));
            Assert.That(invoices.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(invoices.DecidingRuleId, Is.EqualTo(SampleSeeder.GlobexInvoicesPlatformRuleId));
            Assert.That(invoices.MatchedRules.Select(rule => rule.RuleId), Does.Contain(SampleSeeder.GlobexInvoicesRuleId));
            Assert.That(orders.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(orders.DecidingRuleId, Is.EqualTo(SampleSeeder.GlobexOrdersRuleId));
        });
    }
}
