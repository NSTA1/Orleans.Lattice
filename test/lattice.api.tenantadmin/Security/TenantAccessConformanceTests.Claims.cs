using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>Claims case: an identity provider cannot assert its way into a tenant group (D2).</summary>
public sealed partial class TenantAccessConformanceTests
{
    [Test]
    public async Task Case05_a_token_asserting_a_tenant_admin_group_gains_nothing()
    {
        const string Admin = "c05-alice";
        const string RealAdmin = "c05-ruth";
        const string ClusterGroup = "c05-entra";
        var a = await _fixture.SeedTenantAsync("c05-a", Admin);
        var admins = GroupOf(a, "admins");
        var orders = TreeOf(a, "orders");
        await _fixture.SeedClusterGroupAsync(ClusterGroup);

        using (As(Admin))
        {
            await _fixture.Directory.UpsertGroupAsync(a.Value, new TenantGroupDescriptor { Name = "admins" });
            await _fixture.Directory.AddGroupMemberAsync(a.Value, "admins", RealAdmin);
            await _fixture.AccessAdmin.AddAdminSubjectAsync(a.Value, admins);
            await _fixture.Directory.AddMemberAsync(a.Value, ClusterGroup, TenantSubjectKind.ClusterGroup);
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("admins-read", "admins", "orders", subjectKind: TenantSubjectKind.TenantGroup));
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("entra-read", ClusterGroup, "invoices", subjectKind: TenantSubjectKind.ClusterGroup));
        }

        // Positive control: a real member of the admin group, recorded in the
        // directory, is an admin and reads the tree the group is granted.
        using (As(RealAdmin))
        {
            var page = await _fixture.Directory.ListGroupsAsync(a.Value, new TenantAccessPageRequest());
            Assert.That(page.Entries.Select(g => g.Name), Does.Contain("admins"));
        }

        await WaitAllowedAsync(RealAdmin, a, orders, "a directory member of the tenant group holds its grants");

        var callers = new (string Path, Func<IDisposable> Use)[]
        {
            ("JWT groups claim", () => TenantAccessConformanceClusterFixture.AsJwt("c05-mallory", admins, ClusterGroup)),
            ("asserted groups", () => As("c05-mallet", admins, ClusterGroup)),
        };

        foreach (var (path, use) in callers)
        {
            LatticeSubject resolved;
            LatticeAccessDecision onOrders;
            Exception? listGroups;
            Exception? posture;
            Exception? addAdmin;
            using (use())
            {
                resolved = await _fixture.Silo.GetRequiredService<ILatticeMembershipContext>().ResolveCurrentAsync();
                onOrders = await _fixture.DecideAsync(a.Value, orders);
                listGroups = await CaptureAsync(() => _fixture.Directory.ListGroupsAsync(a.Value, new TenantAccessPageRequest()));
                posture = await CaptureAsync(() => _fixture.Policy.GetPostureAsync(a.Value));
                addAdmin = await CaptureAsync(() => _fixture.AccessAdmin.AddAdminSubjectAsync(a.Value, resolved.SubjectId));
            }

            // The cluster-group assertion is the proof the path carries asserted
            // groups at all, and with it the subject can act as the tenant, so the
            // refusal on the orders tree is the stripped admin group, not the gate.
            await TestPoll.UntilAsync(
                async () =>
                {
                    using (use())
                    {
                        return (await _fixture.DecideAsync(a.Value, TreeOf(a, "invoices"))).Allowed;
                    }
                },
                $"{path}: the asserted cluster group, a member entry, admits the caller to act as the tenant",
                Deadline);

            Assert.Multiple(() =>
            {
                Assert.That(resolved.GroupIds, Does.Contain(ClusterGroup), $"{path}: asserted cluster groups are kept");
                Assert.That(resolved.GroupIds, Does.Not.Contain(admins), $"{path}: the asserted tenant group is stripped");
                Assert.That(onOrders.Allowed, Is.False, $"{path}: the asserted tenant group grants nothing on the data plane");
                Assert.That(listGroups, Is.TypeOf<LatticeAuthorizationDeniedException>(), $"{path}: no tenant-admin authority on the directory");
                Assert.That(posture, Is.TypeOf<LatticeAuthorizationDeniedException>(), $"{path}: no tenant-admin authority on the policy facade");
                Assert.That(addAdmin, Is.TypeOf<LatticeAuthorizationDeniedException>(), $"{path}: cannot promote itself");
            });
        }
    }
}
