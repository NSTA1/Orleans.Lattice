using System.Globalization;
using Grpc.Core;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// The fault ladder of the delegated tenant access RPCs, proved on both facades: each
/// typed failure the directory and policy facades document maps onto its gRPC
/// status (feature disabled to <c>FailedPrecondition</c>, confinement to
/// <c>InvalidArgument</c>, a reached cap to <c>ResourceExhausted</c> with its
/// dimension trailers, a denial to <c>PermissionDenied</c> carrying the facade's own
/// message), a facade-shaped <see cref="RpcException"/> passes through intact, a
/// cancellation is <c>Cancelled</c>, and an unexpected fault is an <c>Internal</c>
/// that does not leak its message. The service is driven directly so the raised
/// exception is observed exactly as the server produced it.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcTenantAccessFaultMappingTests
{
    private TenantAccessGrpcHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new TenantAccessGrpcHarness();

    [TearDown]
    public void TearDown() => _h.Dispose();

    private static IEnumerable<TestCaseData> FaultCases()
    {
        yield return Case("disabled", new TenantAccessAdministrationDisabledException("acme"), StatusCode.FailedPrecondition);
        yield return Case("confinement", new TenantAccessConfinementException(
            "acme", TenantAccessConfinementRule.ForeignTenantGroup, "names another tenant's group", "memberId"), StatusCode.InvalidArgument);
        yield return Case("quota", Quota(), StatusCode.ResourceExhausted);
        yield return Case("denied", new LatticeAuthorizationDeniedException("caller may not administer tenant 'acme'"), StatusCode.PermissionDenied);
        yield return Case("tenant-access-denied", new LatticeTenantAccessDeniedException("no valid active tenant"), StatusCode.PermissionDenied);
        yield return Case("reserved", new ReservedTenantOperationException(TenantId.DefaultId, "list-groups"), StatusCode.FailedPrecondition);
        yield return Case("last-admin", new TenantLastAdminSubjectException("acme", "t/acme/admins"), StatusCode.FailedPrecondition);
        yield return Case("not-found", new TenantNotFoundException("ghost"), StatusCode.NotFound);
        yield return Case("argument", new ArgumentException("bad group name", "groupName"), StatusCode.InvalidArgument);
        yield return Case("precondition", new InvalidOperationException("not now"), StatusCode.FailedPrecondition);

        static TestCaseData Case(string name, Exception fault, StatusCode expected) =>
            new TestCaseData(fault, expected).SetArgDisplayNames(name);
    }

    private static LatticeQuotaExceededException Quota() =>
        new("Tenant 'acme' has reached its MaxGroups cap of 500.", "_lattice_membership", "MaxGroups", 500, 500, "acme");

    [TestCaseSource(nameof(FaultCases))]
    public void A_directory_fault_maps_to_its_status_with_the_facade_message(Exception fault, StatusCode expected)
    {
        _h.Directory.UpsertGroupAsync("acme", Arg.Any<TenantGroupDescriptor>(), Arg.Any<CancellationToken>()).ThrowsAsync(fault);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Service.UpsertTenantGroup(
            new TenantAdminGroupUpsertRequest { TenantId = "acme", Group = new TenantGroupDescriptor { Name = "ops" } },
            TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.UpsertTenantGroupMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(expected));
            Assert.That(ex.Status.Detail, Is.EqualTo(fault.Message),
                "the binding forwards the facade's own caller-safe message, as the pre-epic RPCs do");
        });
    }

    [TestCaseSource(nameof(FaultCases))]
    public void A_policy_fault_maps_to_its_status_with_the_facade_message(Exception fault, StatusCode expected)
    {
        _h.Policy.PutRuleAsync("acme", Arg.Any<TenantRuleDraft>(), Arg.Any<CancellationToken>()).ThrowsAsync(fault);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Service.PutTenantRule(
            new TenantAdminRulePutRequest
            {
                TenantId = "acme",
                Rule = new TenantRuleDraft { RuleId = "r", SubjectId = "bob", TreeName = "orders" },
            },
            TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.PutTenantRuleMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(expected));
            Assert.That(ex.Status.Detail, Is.EqualTo(fault.Message));
        });
    }

    [Test]
    public void A_reached_cap_carries_its_dimension_and_figures_as_trailers()
    {
        _h.Directory.AddMemberAsync("acme", "bob", TenantSubjectKind.User, Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeQuotaExceededException(
                "Tenant 'acme' has reached its MaxMemberSubjects cap of 5000.", "_lattice_tenants", "MaxMemberSubjects", 5000, 5000, "acme"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Service.AddTenantMember(
            new TenantAdminMemberRequest { TenantId = "acme", SubjectId = "bob" },
            TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.AddTenantMemberMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
            Assert.That(ex.Trailers.GetValue(LatticeTenantAdminGrpcService.QuotaDimensionTrailer), Is.EqualTo("MaxMemberSubjects"));
            Assert.That(ex.Trailers.GetValue(LatticeTenantAdminGrpcService.QuotaCurrentTrailer), Is.EqualTo(5000.ToString(CultureInfo.InvariantCulture)));
            Assert.That(ex.Trailers.GetValue(LatticeTenantAdminGrpcService.QuotaLimitTrailer), Is.EqualTo(5000.ToString(CultureInfo.InvariantCulture)));
            Assert.That(ex.Trailers.GetValue("lattice-quota-tree"), Is.Null, "the shared tree a cap protects is not disclosed");
        });
    }

    [Test]
    public void A_cap_with_no_numeric_ceiling_omits_the_figures()
    {
        _h.Policy.PutRuleAsync("acme", Arg.Any<TenantRuleDraft>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeQuotaExceededException("over", "tree", "MaxTenantRules", 0, 0));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Service.PutTenantRule(
            new TenantAdminRulePutRequest { TenantId = "acme", Rule = new TenantRuleDraft { RuleId = "r", SubjectId = "bob" } },
            TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.PutTenantRuleMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Trailers.GetValue(LatticeTenantAdminGrpcService.QuotaDimensionTrailer), Is.EqualTo("MaxTenantRules"));
            Assert.That(ex.Trailers.GetValue(LatticeTenantAdminGrpcService.QuotaCurrentTrailer), Is.Null);
            Assert.That(ex.Trailers.GetValue(LatticeTenantAdminGrpcService.QuotaLimitTrailer), Is.Null);
        });
    }

    [Test]
    public void A_facade_shaped_rpc_exception_passes_through_with_its_detail()
    {
        _h.Directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new RpcException(new Status(StatusCode.Unavailable, "membership store offline")));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Service.ListTenantGroups(
            new TenantAdminAccessListRequest { TenantId = "acme", Page = new TenantAccessPageRequest() },
            TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.ListTenantGroupsMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(ex.Status.Detail, Is.EqualTo("membership store offline"));
        });
    }

    [Test]
    public void A_cancellation_maps_to_cancelled()
    {
        _h.Policy.GetPostureAsync("acme", Arg.Any<CancellationToken>()).ThrowsAsync(new OperationCanceledException());

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Service.GetTenantAccessPosture(
            new TenantAdminTenantRequest { TenantId = "acme" },
            TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.GetTenantAccessPostureMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void An_unexpected_fault_maps_to_internal_without_leaking_its_message()
    {
        _h.Directory.ResolveSubjectAsync("acme", "bob", TenantSubjectKind.User, Arg.Any<CancellationToken>())
            .ThrowsAsync(new NotSupportedException("internal detail that must not leak"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Service.ResolveTenantSubject(
            new TenantAdminMemberRequest { TenantId = "acme", SubjectId = "bob" },
            TenantAccessGrpcHarness.Context(LatticeTenantAdminGrpcMethods.ResolveTenantSubjectMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("internal detail"));
        });
    }

    [Test]
    public void A_fault_reaches_the_client_as_the_same_status()
    {
        _h.Directory.RemoveGroupAsync("acme", "admins", Arg.Any<CancellationToken>())
            .ThrowsAsync(new TenantLastAdminSubjectException("acme", "t/acme/admins"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _h.Client.RemoveGroupAsync("acme", "admins"));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }
}
