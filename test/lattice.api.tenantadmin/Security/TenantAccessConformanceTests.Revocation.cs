namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>Revocation case: removing a member or an edge takes effect on the very next request (T1, issue #4053).</summary>
public sealed partial class TenantAccessConformanceTests
{
    [Test]
    public async Task Case09_removing_a_member_or_a_members_group_edge_is_refused_on_the_next_request()
    {
        const string Admin = "c09-alice";
        const string Bob = "c09-bob";
        const string Carl = "c09-carl";
        const string Dave = "c09-dave";
        var a = await _fixture.SeedTenantAsync("c09-a", Admin);
        var orders = TreeOf(a, "orders");

        using (As(Admin))
        {
            await _fixture.Directory.UpsertGroupAsync(a.Value, new TenantGroupDescriptor { Name = "eng" });
            await _fixture.Directory.AddGroupMemberAsync(a.Value, "eng", Bob);
            await _fixture.Directory.AddGroupMemberAsync(a.Value, "eng", Dave);
            await _fixture.Directory.AddMemberAsync(a.Value, "eng", TenantSubjectKind.TenantGroup);
            await _fixture.Directory.AddMemberAsync(a.Value, Carl);
            await _fixture.Policy.PutRuleAsync(a.Value, TenantWideRule("eng-read", "eng", subjectKind: TenantSubjectKind.TenantGroup));
            await _fixture.Policy.PutRuleAsync(a.Value, TenantWideRule("carl-read", Carl));
        }

        await WaitAllowedAsync(Bob, a, orders, "bob reads through the member group");
        await WaitAllowedAsync(Dave, a, orders, "dave reads through the member group");
        await WaitAllowedAsync(Carl, a, orders, "carl reads as a direct member");

        // Each removal below is followed by exactly one request, with no poll: the
        // registry write leaves the compiled tenant snapshot non-authoritative until
        // its rebuild lands, and the gate must confirm against the registry inside
        // that window rather than serve the pre-removal snapshot.
        using (As(Admin))
        {
            await _fixture.Directory.RemoveMemberAsync(a.Value, Carl);
        }

        var carlAfter = await AllowsAsync(Carl, a, orders);

        using (As(Admin))
        {
            await _fixture.Directory.RemoveGroupMemberAsync(a.Value, "eng", Bob);
        }

        var bobAfter = await AllowsAsync(Bob, a, orders);
        var daveStill = await AllowsAsync(Dave, a, orders);

        using (As(Admin))
        {
            await _fixture.Directory.RemoveMemberAsync(a.Value, "eng", TenantSubjectKind.TenantGroup);
        }

        var daveAfter = await AllowsAsync(Dave, a, orders);

        Assert.Multiple(() =>
        {
            Assert.That(carlAfter, Is.False, "a removed direct member is refused on the next request");
            Assert.That(bobAfter, Is.False, "a member whose group edge was removed is refused on the next request");
            Assert.That(daveStill, Is.True, "the edge removal touched only bob");
            Assert.That(daveAfter, Is.False, "removing the group from the member set refuses its members on the next request");
        });
    }
}
