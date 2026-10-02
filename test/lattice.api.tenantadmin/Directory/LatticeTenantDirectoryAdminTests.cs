using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Directory.DirectoryTestSupport;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// Unit tests for <see cref="LatticeTenantDirectoryAdmin"/>, the tenant directory
/// facade. This file pins the order of checks every operation shares - argument
/// syntax, then tenant-tier authorization (operator, or an admin of the tenant
/// directly or through a group), then the delegated-access feature gate, then the
/// reserved default tenant refusal - and the constructor guards. The behaviour of
/// each operation lives in the sibling partial files. All doubles are in-memory and
/// deterministic.
/// </summary>
[TestFixture]
public sealed partial class LatticeTenantDirectoryAdminTests
{
    private const string Tenant = "acme";
    private const string OtherTenant = "globex";
    private const string Operator = "root";
    private const string Alice = "alice";
    private const string Gina = "gina";

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private static TenantRecord Record(string tenantId, params string[] adminSubjects)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Stamp(1),
            "seed");

        var stamp = 2L;
        foreach (var subjectId in adminSubjects)
        {
            record.AddAdminSubject(subjectId, Stamp(stamp++), "seed");
        }

        return record;
    }

    /// <summary>
    /// The facade over in-memory doubles: <c>acme</c> administered by <c>alice</c>,
    /// <c>globex</c> by <c>gina</c>, the reserved default tenant, and an access gate
    /// that makes <c>root</c> the only platform operator.
    /// </summary>
    private sealed class Harness
    {
        public Harness(
            LatticeSubject caller,
            bool enabled,
            ILatticeIdentityDirectory? identityDirectory,
            bool validationRequired)
        {
            Registry.Seed(Record(Tenant, Alice));
            Registry.Seed(Record(OtherTenant, Gina));
            Registry.Seed(Record(TenantId.DefaultId, Operator));
            Flag = new SettableFlag(enabled);

            var authorizer = new TenantRegionResidencyAuthorizer(
                new AdminSubjectGate(Operator), Registry, new FixedMembershipContext(caller), Flag.Read);

            Admin = new LatticeTenantDirectoryAdmin(
                Registry,
                authorizer,
                new IncrementingClock(),
                Options.Create(new ClusterOptions { ClusterId = "region-a" }),
                Store,
                Rules,
                Flag.Read,
                identityDirectory,
                new FixedOptionsMonitor<LatticeIdentityDirectoryOptions>(
                    new LatticeIdentityDirectoryOptions { ValidationRequired = validationRequired }));
        }

        public MergingTenantRegistry Registry { get; } = new();

        public FakeTenantDirectoryStore Store { get; } = new();

        public FakeTenantGroupRuleCascade Rules { get; } = new();

        public SettableFlag Flag { get; }

        public LatticeTenantDirectoryAdmin Admin { get; }

        public TenantRecord Committed(string tenantId) => Registry.Peek(tenantId)!;
    }

    private static Harness Build(
        string caller = Operator,
        bool enabled = true,
        IReadOnlyCollection<string>? groups = null,
        ILatticeIdentityDirectory? identityDirectory = null,
        bool validationRequired = false) =>
        new(new LatticeSubject(caller, groups), enabled, identityDirectory, validationRequired);

    /// <summary>One invocation of every facade member against <paramref name="tenantId"/>, with valid arguments.</summary>
    private static IEnumerable<TestCaseData> EveryOperation()
    {
        yield return Op("ListGroups", (a, t) => a.ListGroupsAsync(t, new TenantAccessPageRequest()));
        yield return Op("GetGroup", (a, t) => a.GetGroupAsync(t, "eng"));
        yield return Op("UpsertGroup", (a, t) => a.UpsertGroupAsync(t, new TenantGroupDescriptor { Name = "eng" }));
        yield return Op("RemoveGroup", (a, t) => a.RemoveGroupAsync(t, "eng"));
        yield return Op("ListGroupMembers", (a, t) => a.ListGroupMembersAsync(t, "eng"));
        yield return Op("AddGroupMember", (a, t) => a.AddGroupMemberAsync(t, "eng", "bob"));
        yield return Op("RemoveGroupMember", (a, t) => a.RemoveGroupMemberAsync(t, "eng", "bob"));
        yield return Op("ListMembers", (a, t) => a.ListMembersAsync(t, new TenantAccessPageRequest()));
        yield return Op("AddMember", (a, t) => a.AddMemberAsync(t, "bob"));
        yield return Op("RemoveMember", (a, t) => a.RemoveMemberAsync(t, "bob"));
        yield return Op("ResolveSubject", (a, t) => a.ResolveSubjectAsync(t, "bob"));

        static TestCaseData Op(string name, Func<ILatticeTenantDirectoryAdmin, string, Task> call) =>
            new TestCaseData(call).SetArgDisplayNames(name);
    }

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_rejects_each_null_required_argument()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);
        var clock = new IncrementingClock();
        var options = Options.Create(new ClusterOptions());
        var store = new FakeTenantDirectoryStore();
        var rules = new FakeTenantGroupRuleCascade();
        Func<bool> flag = static () => true;

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTenantDirectoryAdmin(null!, authorizer, clock, options, store, rules, flag), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantDirectoryAdmin(registry, null!, clock, options, store, rules, flag), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantDirectoryAdmin(registry, authorizer, null!, options, store, rules, flag), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantDirectoryAdmin(registry, authorizer, clock, null!, store, rules, flag), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantDirectoryAdmin(registry, authorizer, clock, options, null!, rules, flag), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantDirectoryAdmin(registry, authorizer, clock, options, store, null!, flag), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantDirectoryAdmin(registry, authorizer, clock, options, store, rules, null!), Throws.ArgumentNullException);
        });
    }

    // ---- authorization ---------------------------------------------------

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_admits_a_platform_operator_on_any_tenant(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build(Operator);
        harness.Store.SeedGroup("t/globex/eng");

        Assert.That(async () => await call(harness.Admin, OtherTenant), Throws.Nothing);
    }

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_admits_an_exact_id_tenant_admin_of_that_tenant(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");

        Assert.That(async () => await call(harness.Admin, Tenant), Throws.Nothing);
    }

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_denies_the_admin_of_another_tenant_and_writes_nothing(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/globex/eng");

        Assert.That(async () => await call(harness.Admin, OtherTenant), Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.Multiple(() =>
        {
            Assert.That(harness.Store.Writes, Is.Zero);
            Assert.That(harness.Registry.Puts, Is.Zero);
        });
    }

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_reports_a_missing_tenant_as_a_denial_to_a_non_operator(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build(Alice);

        Assert.That(async () => await call(harness.Admin, "nowhere"), Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_admits_a_group_admin_while_the_feature_is_enabled(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build("bob", groups: ["t/acme/admins"]);
        harness.Store.SeedGroup("t/acme/eng");
        var record = harness.Committed(Tenant);
        record.AddAdminSubject("t/acme/admins", Stamp(50), "seed");

        Assert.That(async () => await call(harness.Admin, Tenant), Throws.Nothing);
    }

    [Test]
    public void A_group_admin_of_one_tenant_is_denied_on_another()
    {
        var harness = Build("bob", groups: ["t/acme/admins"]);
        harness.Committed(Tenant).AddAdminSubject("t/acme/admins", Stamp(50), "seed");

        Assert.That(
            async () => await harness.Admin.ListGroupsAsync(OtherTenant, new TenantAccessPageRequest()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- feature gate ----------------------------------------------------

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_refuses_an_authorized_caller_while_the_feature_is_disabled(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build(Operator, enabled: false);
        harness.Store.SeedGroup("t/acme/eng");

        var ex = Assert.ThrowsAsync<TenantAccessAdministrationDisabledException>(async () => await call(harness.Admin, Tenant));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo(Tenant));
            Assert.That(harness.Store.Writes, Is.Zero);
            Assert.That(harness.Registry.Puts, Is.Zero);
        });
    }

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_denies_an_unauthorized_caller_before_revealing_the_feature_posture(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build("mallory", enabled: false);

        Assert.That(async () => await call(harness.Admin, Tenant), Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_group_admin_is_not_authorized_while_the_feature_is_disabled()
    {
        var harness = Build("bob", enabled: false, groups: ["t/acme/admins"]);
        harness.Committed(Tenant).AddAdminSubject("t/acme/admins", Stamp(50), "seed");

        Assert.That(
            async () => await harness.Admin.ListGroupsAsync(Tenant, new TenantAccessPageRequest()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task The_feature_flag_is_read_live_on_every_call()
    {
        var harness = Build(Operator);
        await harness.Admin.ListGroupsAsync(Tenant, new TenantAccessPageRequest());

        harness.Flag.Enabled = false;
        Assert.That(
            async () => await harness.Admin.ListGroupsAsync(Tenant, new TenantAccessPageRequest()),
            Throws.TypeOf<TenantAccessAdministrationDisabledException>());

        harness.Flag.Enabled = true;
        Assert.That(async () => await harness.Admin.ListGroupsAsync(Tenant, new TenantAccessPageRequest()), Throws.Nothing);
    }

    // ---- reserved default tenant -----------------------------------------

    [TestCaseSource(nameof(EveryOperation))]
    public void Every_operation_refuses_the_reserved_default_tenant(Func<ILatticeTenantDirectoryAdmin, string, Task> call)
    {
        var harness = Build(Operator);

        Assert.That(async () => await call(harness.Admin, TenantId.DefaultId), Throws.TypeOf<ReservedTenantOperationException>());
        Assert.That(harness.Store.Writes, Is.Zero);
    }

    [Test]
    public void The_default_tenant_reports_the_disabled_feature_before_the_reservation()
    {
        var harness = Build(Operator, enabled: false);

        Assert.That(
            async () => await harness.Admin.ListGroupsAsync(TenantId.DefaultId, new TenantAccessPageRequest()),
            Throws.TypeOf<TenantAccessAdministrationDisabledException>());
    }

    // ---- argument syntax -------------------------------------------------

    [TestCase(null)]
    [TestCase("")]
    [TestCase("Not A Tenant!")]
    public void An_invalid_tenant_id_is_rejected(string? tenantId)
    {
        var harness = Build(Operator);

        Assert.That(
            async () => await harness.Admin.ListGroupsAsync(tenantId!, new TenantAccessPageRequest()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void A_null_page_request_is_rejected()
    {
        var harness = Build(Operator);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await harness.Admin.ListGroupsAsync(Tenant, null!), Throws.ArgumentNullException);
            Assert.That(async () => await harness.Admin.ListMembersAsync(Tenant, null!), Throws.ArgumentNullException);
        });
    }

    [TestCase("")]
    [TestCase("Eng")]
    [TestCase("eng/x")]
    [TestCase("t/acme/eng")]
    public void An_invalid_local_group_name_is_rejected_before_authorization(string groupName)
    {
        // A denied caller still gets the argument error, proving syntax is checked first.
        var harness = Build("mallory");

        Assert.That(async () => await harness.Admin.GetGroupAsync(Tenant, groupName), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void An_undefined_subject_kind_is_rejected()
    {
        var harness = Build(Operator);

        Assert.That(
            async () => await harness.Admin.AddMemberAsync(Tenant, "bob", (TenantSubjectKind)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
