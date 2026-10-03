using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Round-trip tests for the eleven <see cref="ILatticeTenantDirectoryAdmin"/> RPCs:
/// each client member is driven through the real Orleans marshallers into the
/// service and onto a substitute facade, proving every argument reaches the facade
/// unaltered and every result comes back intact. Also proves the client's local
/// argument guards, the server's null guards, and that the RPCs answer
/// <see cref="StatusCode.Unimplemented"/> when the facade is not registered.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcTenantDirectoryTests
{
    private TenantAccessGrpcHarness _h = null!;

    private ILatticeTenantDirectoryAdmin Client => _h.Client;

    [SetUp]
    public void SetUp() => _h = new TenantAccessGrpcHarness();

    [TearDown]
    public void TearDown() => _h.Dispose();

    // ---- round trips -----------------------------------------------------

    [Test]
    public async Task ListGroups_round_trips_the_page_request_and_the_page()
    {
        _h.Directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantGroupPage
            {
                Entries = [new TenantGroupDescriptor { Name = "ops", DisplayName = "Operations" }],
                NextPageToken = "ops",
            });

        var page = await Client.ListGroupsAsync("acme", new TenantAccessPageRequest { PageSize = 1, PageToken = "dev" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Single().Name, Is.EqualTo("ops"));
            Assert.That(page.Entries.Single().DisplayName, Is.EqualTo("Operations"));
            Assert.That(page.NextPageToken, Is.EqualTo("ops"));
        });
        await _h.Directory.Received(1).ListGroupsAsync(
            "acme", Arg.Is<TenantAccessPageRequest>(p => p.PageSize == 1 && p.PageToken == "dev"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetGroup_round_trips_a_found_group()
    {
        _h.Directory.GetGroupAsync("acme", "ops", Arg.Any<CancellationToken>())
            .Returns(new TenantGroupDescriptor { Name = "ops", DisplayName = "Operations" });

        var group = await Client.GetGroupAsync("acme", "ops");

        Assert.That(group, Is.EqualTo(new TenantGroupDescriptor { Name = "ops", DisplayName = "Operations" }));
    }

    [Test]
    public async Task GetGroup_round_trips_an_absent_group_as_null()
    {
        _h.Directory.GetGroupAsync("acme", "ghost", Arg.Any<CancellationToken>()).Returns((TenantGroupDescriptor?)null);

        var group = await Client.GetGroupAsync("acme", "ghost");

        Assert.That(group, Is.Null);
    }

    [Test]
    public async Task UpsertGroup_round_trips_the_group()
    {
        _h.Directory.UpsertGroupAsync("acme", Arg.Any<TenantGroupDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(call => call.Arg<TenantGroupDescriptor>());

        var stored = await Client.UpsertGroupAsync("acme", new TenantGroupDescriptor { Name = "ops", DisplayName = "Ops" });

        Assert.That(stored, Is.EqualTo(new TenantGroupDescriptor { Name = "ops", DisplayName = "Ops" }));
        await _h.Directory.Received(1).UpsertGroupAsync(
            "acme", new TenantGroupDescriptor { Name = "ops", DisplayName = "Ops" }, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemoveGroup_round_trips_the_cascade_report()
    {
        _h.Directory.RemoveGroupAsync("acme", "ops", Arg.Any<CancellationToken>()).Returns(new TenantGroupRemovalResult
        {
            TenantId = "acme",
            GroupName = "ops",
            Removed = true,
            EdgesRemoved = 3,
            RemovedFromMemberSet = true,
            RemovedFromAdminSet = true,
            RemovedRuleIds = ["r1", "r2"],
        });

        var result = await Client.RemoveGroupAsync("acme", "ops");

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.GroupName, Is.EqualTo("ops"));
            Assert.That(result.Removed, Is.True);
            Assert.That(result.EdgesRemoved, Is.EqualTo(3));
            Assert.That(result.RemovedFromMemberSet, Is.True);
            Assert.That(result.RemovedFromAdminSet, Is.True);
            Assert.That(result.RemovedRuleIds, Is.EqualTo(new[] { "r1", "r2" }));
        });
    }

    [Test]
    public async Task ListGroupMembers_round_trips_every_member_and_its_kind()
    {
        _h.Directory.ListGroupMembersAsync("acme", "ops", Arg.Any<CancellationToken>()).Returns(new TenantGroupMember[]
        {
            new() { MemberId = "alice", Kind = TenantSubjectKind.User },
            new() { MemberId = "devs", Kind = TenantSubjectKind.TenantGroup },
            new() { MemberId = "staff", Kind = TenantSubjectKind.ClusterGroup },
        });

        var members = await Client.ListGroupMembersAsync("acme", "ops");

        Assert.That(members, Is.EqualTo(new TenantGroupMember[]
        {
            new() { MemberId = "alice", Kind = TenantSubjectKind.User },
            new() { MemberId = "devs", Kind = TenantSubjectKind.TenantGroup },
            new() { MemberId = "staff", Kind = TenantSubjectKind.ClusterGroup },
        }));
    }

    [Test]
    public async Task ListGroupMembers_round_trips_an_empty_group_as_an_empty_list()
    {
        _h.Directory.ListGroupMembersAsync("acme", "ops", Arg.Any<CancellationToken>()).Returns(Array.Empty<TenantGroupMember>());

        var members = await Client.ListGroupMembersAsync("acme", "ops");

        Assert.That(members, Is.Not.Null.And.Empty);
    }

    [TestCase(TenantSubjectKind.User)]
    [TestCase(TenantSubjectKind.TenantGroup)]
    [TestCase(TenantSubjectKind.ClusterGroup)]
    public async Task AddGroupMember_carries_the_member_kind_to_the_facade(TenantSubjectKind kind)
    {
        _h.Directory.AddGroupMemberAsync("acme", "ops", "m1", kind, Arg.Any<CancellationToken>())
            .Returns(Change("ops", "m1", kind));

        var result = await Client.AddGroupMemberAsync("acme", "ops", "m1", kind);

        Assert.That(result, Is.EqualTo(Change("ops", "m1", kind)));
        await _h.Directory.Received(1).AddGroupMemberAsync("acme", "ops", "m1", kind, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AddGroupMember_defaults_the_member_kind_to_user()
    {
        _h.Directory.AddGroupMemberAsync("acme", "ops", "alice", TenantSubjectKind.User, Arg.Any<CancellationToken>())
            .Returns(Change("ops", "alice", TenantSubjectKind.User));

        await Client.AddGroupMemberAsync("acme", "ops", "alice");

        await _h.Directory.Received(1).AddGroupMemberAsync("acme", "ops", "alice", TenantSubjectKind.User, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemoveGroupMember_round_trips()
    {
        _h.Directory.RemoveGroupMemberAsync("acme", "ops", "devs", TenantSubjectKind.TenantGroup, Arg.Any<CancellationToken>())
            .Returns(Change("ops", "devs", TenantSubjectKind.TenantGroup, changed: false));

        var result = await Client.RemoveGroupMemberAsync("acme", "ops", "devs", TenantSubjectKind.TenantGroup);

        Assert.That(result, Is.EqualTo(Change("ops", "devs", TenantSubjectKind.TenantGroup, changed: false)));
    }

    [Test]
    public async Task ListMembers_round_trips_the_page_request_and_the_page()
    {
        _h.Directory.ListMembersAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantMemberPage
            {
                Entries =
                [
                    new TenantMemberEntry { SubjectId = "bob", Kind = TenantSubjectKind.User },
                    new TenantMemberEntry { SubjectId = "readers", Kind = TenantSubjectKind.TenantGroup },
                ],
            });

        var page = await Client.ListMembersAsync("acme", new TenantAccessPageRequest { PageSize = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[]
            {
                new TenantMemberEntry { SubjectId = "bob", Kind = TenantSubjectKind.User },
                new TenantMemberEntry { SubjectId = "readers", Kind = TenantSubjectKind.TenantGroup },
            }));
            Assert.That(page.NextPageToken, Is.Null);
        });
        await _h.Directory.Received(1).ListMembersAsync(
            "acme", Arg.Is<TenantAccessPageRequest>(p => p.PageSize == 2 && p.PageToken == null), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AddMember_round_trips()
    {
        _h.Directory.AddMemberAsync("acme", "staff", TenantSubjectKind.ClusterGroup, Arg.Any<CancellationToken>())
            .Returns(Change(null, "staff", TenantSubjectKind.ClusterGroup));

        var result = await Client.AddMemberAsync("acme", "staff", TenantSubjectKind.ClusterGroup);

        Assert.That(result, Is.EqualTo(Change(null, "staff", TenantSubjectKind.ClusterGroup)));
    }

    [Test]
    public async Task RemoveMember_round_trips()
    {
        _h.Directory.RemoveMemberAsync("acme", "bob", TenantSubjectKind.User, Arg.Any<CancellationToken>())
            .Returns(Change(null, "bob", TenantSubjectKind.User));

        var result = await Client.RemoveMemberAsync("acme", "bob");

        Assert.That(result, Is.EqualTo(Change(null, "bob", TenantSubjectKind.User)));
        await _h.Directory.Received(1).RemoveMemberAsync("acme", "bob", TenantSubjectKind.User, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveSubject_round_trips_the_standing_and_its_entries()
    {
        _h.Directory.ResolveSubjectAsync("acme", "bob", TenantSubjectKind.User, Arg.Any<CancellationToken>())
            .Returns(new TenantSubjectResolution
            {
                TenantId = "acme",
                SubjectId = "bob",
                SubjectKind = TenantSubjectKind.User,
                IsAdmin = true,
                IsMember = true,
                AdminEntries = [new TenantMemberEntry { SubjectId = "admins", Kind = TenantSubjectKind.TenantGroup }],
                MemberEntries = [new TenantMemberEntry { SubjectId = "bob", Kind = TenantSubjectKind.User }],
            });

        var resolution = await Client.ResolveSubjectAsync("acme", "bob");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.IsAdmin, Is.True);
            Assert.That(resolution.IsMember, Is.True);
            Assert.That(resolution.AdminEntries.Single(), Is.EqualTo(new TenantMemberEntry { SubjectId = "admins", Kind = TenantSubjectKind.TenantGroup }));
            Assert.That(resolution.MemberEntries.Single(), Is.EqualTo(new TenantMemberEntry { SubjectId = "bob", Kind = TenantSubjectKind.User }));
        });
    }

    [Test]
    public async Task The_cancellation_token_reaches_the_facade()
    {
        using var cts = new CancellationTokenSource();
        _h.Directory.GetGroupAsync("acme", "ops", Arg.Any<CancellationToken>()).Returns((TenantGroupDescriptor?)null);

        await Client.GetGroupAsync("acme", "ops", cts.Token);

        await _h.Directory.Received(1).GetGroupAsync("acme", "ops", cts.Token);
    }

    // ---- client argument guards ------------------------------------------

    private static IEnumerable<TestCaseData> InvalidArgumentCalls()
    {
        var page = new TenantAccessPageRequest();
        var group = new TenantGroupDescriptor { Name = "ops" };
        yield return Case("ListGroups tenant", c => c.ListGroupsAsync("", page));
        yield return Case("ListGroups page", c => c.ListGroupsAsync("acme", null!));
        yield return Case("GetGroup tenant", c => c.GetGroupAsync(null!, "ops"));
        yield return Case("GetGroup name", c => c.GetGroupAsync("acme", ""));
        yield return Case("UpsertGroup tenant", c => c.UpsertGroupAsync("", group));
        yield return Case("UpsertGroup group", c => c.UpsertGroupAsync("acme", null!));
        yield return Case("RemoveGroup name", c => c.RemoveGroupAsync("acme", null!));
        yield return Case("ListGroupMembers name", c => c.ListGroupMembersAsync("acme", ""));
        yield return Case("AddGroupMember member", c => c.AddGroupMemberAsync("acme", "ops", ""));
        yield return Case("AddGroupMember group", c => c.AddGroupMemberAsync("acme", "", "alice"));
        yield return Case("RemoveGroupMember tenant", c => c.RemoveGroupMemberAsync(null!, "ops", "alice"));
        yield return Case("ListMembers page", c => c.ListMembersAsync("acme", null!));
        yield return Case("AddMember subject", c => c.AddMemberAsync("acme", ""));
        yield return Case("RemoveMember tenant", c => c.RemoveMemberAsync("", "bob"));
        yield return Case("ResolveSubject subject", c => c.ResolveSubjectAsync("acme", null!));

        static TestCaseData Case(string name, Func<ILatticeTenantDirectoryAdmin, Task> call) =>
            new TestCaseData(call).SetArgDisplayNames(name);
    }

    [TestCaseSource(nameof(InvalidArgumentCalls))]
    public void A_missing_argument_is_refused_locally_before_any_call(Func<ILatticeTenantDirectoryAdmin, Task> call)
    {
        Assert.That(async () => await call(Client), Throws.InstanceOf<ArgumentException>());
        Assert.That(_h.Directory.ReceivedCalls(), Is.Empty, "a locally refused call must never reach the server");
    }

    // ---- optional facade --------------------------------------------------

    private static IEnumerable<TestCaseData> EveryDirectoryCall()
    {
        var page = new TenantAccessPageRequest();
        yield return Case("ListTenantGroups", c => c.ListGroupsAsync("acme", page));
        yield return Case("GetTenantGroup", c => c.GetGroupAsync("acme", "ops"));
        yield return Case("UpsertTenantGroup", c => c.UpsertGroupAsync("acme", new TenantGroupDescriptor { Name = "ops" }));
        yield return Case("RemoveTenantGroup", c => c.RemoveGroupAsync("acme", "ops"));
        yield return Case("ListTenantGroupMembers", c => c.ListGroupMembersAsync("acme", "ops"));
        yield return Case("AddTenantGroupMember", c => c.AddGroupMemberAsync("acme", "ops", "alice"));
        yield return Case("RemoveTenantGroupMember", c => c.RemoveGroupMemberAsync("acme", "ops", "alice"));
        yield return Case("ListTenantMembers", c => c.ListMembersAsync("acme", page));
        yield return Case("AddTenantMember", c => c.AddMemberAsync("acme", "bob"));
        yield return Case("RemoveTenantMember", c => c.RemoveMemberAsync("acme", "bob"));
        yield return Case("ResolveTenantSubject", c => c.ResolveSubjectAsync("acme", "bob"));

        static TestCaseData Case(string name, Func<ILatticeTenantDirectoryAdmin, Task> call) =>
            new TestCaseData(call).SetArgDisplayNames(name);
    }

    [TestCaseSource(nameof(EveryDirectoryCall))]
    public void Every_directory_rpc_reports_unimplemented_when_the_facade_is_absent(Func<ILatticeTenantDirectoryAdmin, Task> call)
    {
        using var harness = new TenantAccessGrpcHarness(withDirectory: false);

        var fault = Assert.ThrowsAsync<RpcException>(async () => await call(harness.Client));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public async Task The_policy_rpcs_still_serve_when_only_the_directory_facade_is_absent()
    {
        using var harness = new TenantAccessGrpcHarness(withDirectory: false);
        harness.Policy.GetPostureAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantAccessPosture { TenantId = "acme", Enabled = true });

        var posture = await harness.Client.GetPostureAsync("acme");

        Assert.That(posture.Enabled, Is.True);
    }

    // ---- server-side argument guards --------------------------------------

    [Test]
    public void The_server_refuses_a_null_request_or_context()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _h.Service.ListTenantGroups(null!, TenantAccessGrpcHarness.Context("ListTenantGroups")),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _h.Service.GetTenantGroup(new TenantAdminGroupRequest { TenantId = "acme", GroupName = "ops" }, null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _h.Service.ListTenantGroupMembers(null!, TenantAccessGrpcHarness.Context("ListTenantGroupMembers")),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _h.Service.AddTenantMember(null!, TenantAccessGrpcHarness.Context("AddTenantMember")),
                Throws.ArgumentNullException);
        });
    }

    private static TenantMembershipChangeResult Change(string? group, string subject, TenantSubjectKind kind, bool changed = true) =>
        new()
        {
            TenantId = "acme",
            GroupName = group,
            SubjectId = subject,
            SubjectKind = kind,
            Changed = changed,
        };
}
