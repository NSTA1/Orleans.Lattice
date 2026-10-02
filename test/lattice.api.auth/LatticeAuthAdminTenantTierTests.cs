using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Unit coverage for the cluster authorization facade's alignment with the tenant
/// tier (epic #4154, F3): the reserved <c>t/</c> group grammar (D2), the tenant group
/// nesting invariant surfacing unchanged (D3), the cluster-only default group
/// listing, the refusal to author tenant-tier rules and the break-glass removal (D7),
/// tenant-tier rules in the listings and explain surfaces (D8, D9), and the delegated
/// tenant access administration posture flag (D10). Instantiates
/// <see cref="LatticeAuthAdmin"/> directly over in-memory fakes; no cluster is
/// involved.
/// </summary>
[TestFixture]
public sealed partial class LatticeAuthAdminTenantTierTests
{
    private const string ClusterGroup = "g-ops";
    private const string TenantGroup = "t/acme/readers";
    private const string UserId = "u-1";

    private static LatticeAuthAdmin CreateAdmin(
        ILatticeMembershipDirectory? directory = null,
        ILatticeAuthorizationPolicyStore? store = null,
        ILatticeIdentityDirectory? identityDirectory = null,
        bool validationRequired = false,
        ITenantRuleLayer? tenantRuleLayer = null,
        ILatticeDecisionEngine? decisionEngine = null)
    {
        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        authMonitor.CurrentValue.Returns(new LatticeAuthOptions());
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        membershipMonitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();
        identityMonitor.CurrentValue.Returns(new LatticeIdentityDirectoryOptions { ValidationRequired = validationRequired });

        return new LatticeAuthAdmin(
            store ?? new InMemoryPolicyStore(),
            directory ?? Substitute.For<ILatticeMembershipDirectory>(),
            new AllowAllAccessGate(),
            new AnonymousMembershipContext(),
            identityDirectory ?? new NullIdentityDirectory(),
            new ILatticeCredentialAuthenticator[] { new AnonymousCredentialAuthenticator() },
            Options.Create(new LatticeApiAuthOptions()),
            authMonitor,
            membershipMonitor,
            identityMonitor,
            tenants: null,
            tenantRuleLayer,
            decisionEngine);
    }

    // ----- UpsertGroupAsync: the t/ grammar is reserved (D2) -----

    [TestCase("t/acme/readers")]
    [TestCase("t/default/readers")]
    [TestCase("t/Acme/readers")]
    [TestCase("t/acme")]
    [TestCase("t/")]
    [TestCase("t/acme/a/b")]
    public void UpsertGroupAsync_refuses_every_id_in_the_reserved_tenant_namespace(string groupId)
    {
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        var admin = CreateAdmin(directory);

        var ex = Assert.ThrowsAsync<LatticeTenantOwnedGroupException>(
            () => admin.UpsertGroupAsync(new AuthGroup { GroupId = groupId, DisplayName = "Readers" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.GroupId, Is.EqualTo(groupId));
            Assert.That(ex.ParamName, Is.EqualTo("group"));
            Assert.That(ex.Message, Does.Contain("ILatticeTenantDirectoryAdmin"),
                "the refusal must point the operator at the tenant directory facade");
        });
        directory.DidNotReceiveWithAnyArgs().UpsertGroupAsync(default!, default);
    }

    [Test]
    public void UpsertGroupAsync_refuses_a_tenant_group_before_directory_validation()
    {
        var identity = new RecordingIdentityDirectory();
        var admin = CreateAdmin(identityDirectory: identity, validationRequired: true);

        Assert.ThrowsAsync<LatticeTenantOwnedGroupException>(
            () => admin.UpsertGroupAsync(new AuthGroup { GroupId = TenantGroup }));
        Assert.That(identity.ResolveCallCount, Is.Zero);
    }

    [TestCase("g-ops")]
    [TestCase("tenant/acme")]
    [TestCase("T/acme/readers")]
    [TestCase("x/t/acme")]
    public async Task UpsertGroupAsync_writes_a_cluster_group(string groupId)
    {
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        var admin = CreateAdmin(directory);

        await admin.UpsertGroupAsync(new AuthGroup { GroupId = groupId, DisplayName = "Ops" });

        await directory.Received(1).UpsertGroupAsync(
            Arg.Is<MembershipGroup>(g => g.GroupId == groupId && g.DisplayName == "Ops"), Arg.Any<CancellationToken>());
    }

    // ----- AddMemberAsync: the nesting invariant surfaces unchanged (D3) -----

    [Test]
    public void AddMemberAsync_surfaces_the_directory_nesting_refusal_unchanged()
    {
        var refusal = new LatticeTenantGroupNestingException("nested");
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.AddMemberAsync(ClusterGroup, TenantGroup, MembershipMemberKind.Group, Arg.Any<CancellationToken>())
            .ThrowsAsync(refusal);
        var admin = CreateAdmin(directory);

        var ex = Assert.ThrowsAsync<LatticeTenantGroupNestingException>(
            () => admin.AddMemberAsync(ClusterGroup, TenantGroup, MembershipMemberKind.Group));

        Assert.That(ex, Is.SameAs(refusal));
    }

    [Test]
    public void AddMemberAsync_lets_the_nesting_invariant_decide_a_tenant_group_member_when_validation_is_required()
    {
        // A tenant group is local to the cluster: no upstream identity directory knows
        // it, so validating it there would mask the directory's typed nesting refusal
        // behind a directory-validation error.
        var identity = new RecordingIdentityDirectory
        {
            [ClusterGroup] = new DirectoryPrincipal(ClusterGroup, "Ops", DirectoryPrincipalKind.Group),
        };
        var refusal = new LatticeTenantGroupNestingException("nested");
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.AddMemberAsync(ClusterGroup, TenantGroup, MembershipMemberKind.Group, Arg.Any<CancellationToken>())
            .ThrowsAsync(refusal);
        var admin = CreateAdmin(directory, identityDirectory: identity, validationRequired: true);

        var ex = Assert.ThrowsAsync<LatticeTenantGroupNestingException>(
            () => admin.AddMemberAsync(ClusterGroup, TenantGroup, MembershipMemberKind.Group));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.SameAs(refusal));
            Assert.That(identity.Resolved, Is.EqualTo(new[] { ClusterGroup }), "only the cluster group is validated upstream");
        });
    }

    [Test]
    public async Task AddMemberAsync_validates_the_user_but_not_the_tenant_group_parent()
    {
        var identity = new RecordingIdentityDirectory
        {
            [UserId] = new DirectoryPrincipal(UserId, "Alice", DirectoryPrincipalKind.User),
        };
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        var admin = CreateAdmin(directory, identityDirectory: identity, validationRequired: true);

        await admin.AddMemberAsync(TenantGroup, UserId);

        Assert.That(identity.Resolved, Is.EqualTo(new[] { UserId }));
        await directory.Received(1).AddMemberAsync(TenantGroup, UserId, MembershipMemberKind.User, Arg.Any<CancellationToken>());
    }

    [Test]
    public void AddMemberAsync_still_validates_cluster_ids_when_validation_is_required()
    {
        var identity = new RecordingIdentityDirectory();
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        var admin = CreateAdmin(directory, identityDirectory: identity, validationRequired: true);

        Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            () => admin.AddMemberAsync(ClusterGroup, UserId));
        directory.DidNotReceiveWithAnyArgs().AddMemberAsync(default!, default!, default, default);
    }

    // ----- ListGroupsAsync: cluster groups by default -----

    private static ILatticeMembershipDirectory DirectoryListing(params string[] groupIds)
    {
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.ListGroupsAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Yield(groupIds.Select(id => new MembershipGroup(id, id))));
        return directory;
    }

    private static async IAsyncEnumerable<T> Yield<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    [Test]
    public async Task ListGroupsAsync_lists_cluster_groups_only_by_default()
    {
        var admin = CreateAdmin(DirectoryListing("a-ops", "t/acme/readers", "t/bad", "t/zeta/x", "zeta"));

        var page = await admin.ListGroupsAsync(new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(g => g.GroupId), Is.EqualTo(new[] { "a-ops", "zeta" }));
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListGroupsAsync_lists_every_tenant_group_when_asked()
    {
        var admin = CreateAdmin(DirectoryListing("a-ops", "t/acme/readers", "t/bad", "t/zeta/x", "zeta"));

        var page = await admin.ListGroupsAsync(new AuthPageRequest { IncludeTenantGroups = true });

        Assert.That(page.Entries.Select(g => g.GroupId),
            Is.EqualTo(new[] { "a-ops", "t/acme/readers", "t/bad", "t/zeta/x", "zeta" }));
    }

    [Test]
    public async Task ListGroupsAsync_cuts_full_pages_after_dropping_tenant_groups()
    {
        var admin = CreateAdmin(DirectoryListing("a-ops", "t/acme/readers", "t/acme/writers", "zeta"));

        var first = await admin.ListGroupsAsync(new AuthPageRequest { PageSize = 1 });
        var second = await admin.ListGroupsAsync(new AuthPageRequest { PageSize = 1, PageToken = first.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries.Select(g => g.GroupId), Is.EqualTo(new[] { "a-ops" }));
            Assert.That(first.NextPageToken, Is.EqualTo("a-ops"));
            Assert.That(second.Entries.Select(g => g.GroupId), Is.EqualTo(new[] { "zeta" }));
            Assert.That(second.NextPageToken, Is.Null, "no further cluster group remains");
        });
    }

    // ----- GetAccessModelAsync: the delegated tenant access posture flag (D10) -----

    [Test]
    public async Task GetAccessModelAsync_reports_delegated_tenant_access_off_without_a_tenant_rule_layer()
    {
        var model = await CreateAdmin().GetAccessModelAsync();

        Assert.That(model.DelegatedTenantAccessAdministrationEnabled, Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetAccessModelAsync_reports_the_tenant_rule_layer_switch(bool active)
    {
        var layer = new SwitchableTenantRuleLayer { IsActive = active };

        var model = await CreateAdmin(tenantRuleLayer: layer).GetAccessModelAsync();

        Assert.That(model.DelegatedTenantAccessAdministrationEnabled, Is.EqualTo(active));
    }

    [Test]
    public async Task GetAccessModelAsync_reads_the_switch_live_on_every_call()
    {
        var layer = new SwitchableTenantRuleLayer();
        var admin = CreateAdmin(tenantRuleLayer: layer);

        var before = await admin.GetAccessModelAsync();
        layer.IsActive = true;
        var after = await admin.GetAccessModelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(before.DelegatedTenantAccessAdministrationEnabled, Is.False);
            Assert.That(after.DelegatedTenantAccessAdministrationEnabled, Is.True);
        });
    }

    // ----- Fakes -----

    private sealed class SwitchableTenantRuleLayer : ITenantRuleLayer
    {
        public bool IsActive { get; set; }
    }

    private sealed class AllowAllAccessGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }

    private sealed class AnonymousMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }
    }

    /// <summary>A real identity directory that records every id it is asked to resolve.</summary>
    private sealed class RecordingIdentityDirectory : ILatticeIdentityDirectory
    {
        private readonly Dictionary<string, DirectoryPrincipal> _principals = new(StringComparer.Ordinal);

        public List<string> Resolved { get; } = new();

        public int ResolveCallCount => Resolved.Count;

        public string ProviderId => "recording";

        public DirectoryPrincipal this[string id]
        {
            set => _principals[id] = value;
        }

        public string DescribeEntry(DirectoryPrincipalKind? kind) => "Enter a principal id.";

        public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default) =>
            Task.FromResult(DirectorySearchPage.Empty);

        public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)
        {
            Resolved.Add(principalId);
            return Task.FromResult(_principals.TryGetValue(principalId, out var principal) ? principal : null);
        }
    }
}
