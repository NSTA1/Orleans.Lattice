using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Defence-in-depth tests for decision D4 (epic #4154, T1 review): a tenant's admin
/// and member sets may name only its own tenant groups, cluster groups and users.
/// The facades refuse anything else on write, but a record can arrive by
/// replication or restore without them, so the compiled policy and the record's
/// group-aware probes ignore another tenant's group and any malformed <c>t/</c>
/// entry. <see cref="TenantAccessEntries.IsAdmissible"/> is the shared rule.
/// </summary>
[TestFixture]
public sealed class TenantAccessEntriesTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [TestCase("alice", ExpectedResult = true)]
    [TestCase("entra-sales", ExpectedResult = true)]
    [TestCase("team/x", ExpectedResult = true)]
    [TestCase("t/acme/editors", ExpectedResult = true)]
    [TestCase("t/beta/editors", ExpectedResult = false)]
    [TestCase("t/acmex/editors", ExpectedResult = false)]
    [TestCase("t/acme", ExpectedResult = false)]
    [TestCase("t/acme/", ExpectedResult = false)]
    [TestCase("t/acme/BAD id", ExpectedResult = false)]
    [TestCase("t/acme/a/b", ExpectedResult = false)]
    [TestCase("t/", ExpectedResult = false)]
    [TestCase(null, ExpectedResult = false)]
    public bool IsAdmissible_for_acme(string? entry) => TenantAccessEntries.IsAdmissible(entry, Acme);

    [Test]
    public void IsAdmissible_for_the_uninitialised_tenant_admits_only_non_tenant_entries()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantAccessEntries.IsAdmissible("alice", default), Is.True);
            Assert.That(TenantAccessEntries.IsAdmissible("t/acme/editors", default), Is.False);
        });
    }

    [Test]
    public void Compile_never_admits_a_member_of_another_tenants_group_listed_in_the_member_set()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["alice"], members: ["t/beta/x"])], true);

        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeTenantPolicyEngine.ValidateActiveTenant(policy, "mallory", ["t/beta/x"], Acme).Allowed,
                Is.False,
                "a member of t/beta/x is never admitted to acme");
            Assert.That(policy.ResolveAllowedTenants("mallory", ["t/beta/x"]), Is.Empty);
            Assert.That(policy.TryGetTenant("acme", out var tenant) && tenant!.Members.Count == 0, Is.True, "the foreign entry is not compiled");
        });
    }

    [Test]
    public void Compile_never_admits_through_a_foreign_or_malformed_admin_entry()
    {
        var policy = CompiledTenantPolicy.Compile(
            [Record("acme", admins: ["alice", "t/beta/admins", "t/acme/BAD id"], members: ["t/acme/editors"])],
            true);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantPolicyEngine.ValidateActiveTenant(policy, "m", ["t/beta/admins"], Acme).Allowed, Is.False);
            Assert.That(LatticeTenantPolicyEngine.ValidateActiveTenant(policy, "m", ["t/acme/BAD id"], Acme).Allowed, Is.False);
            Assert.That(LatticeTenantPolicyEngine.ValidateActiveTenant(policy, "e", ["t/acme/editors"], Acme).Allowed, Is.True, "acme's own group still admits");
            Assert.That(LatticeTenantPolicyEngine.ValidateActiveTenant(policy, "alice", [], Acme).Allowed, Is.True);
        });
    }

    [Test]
    public void Compile_with_the_flag_off_does_not_filter_the_admin_set()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["alice", "t/beta/admins"])], false);

        Assert.That(policy.TryGetTenant("acme", out var tenant) && tenant!.Admins.Count == 2, Is.True, "the flag-off compile is unchanged");
    }

    [Test]
    public void TenantRecord_group_probes_ignore_another_tenants_group()
    {
        var record = Record("acme", admins: ["alice"]);
        record.AddAdminSubject("t/beta/admins", Clock(50), "replica");
        record.AddMemberSubject("t/beta/x", Clock(51), "replica");
        record.AddMemberSubject("t/acme/editors", Clock(52), "replica");

        Assert.Multiple(() =>
        {
            Assert.That(record.IsAdmin("m", ["t/beta/admins"]), Is.False);
            Assert.That(record.IsMember("m", ["t/beta/x"]), Is.False);
            Assert.That(record.IsMember("e", ["t/acme/editors"]), Is.True);
        });
    }
}
