using Grpc.Core;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Proves the binding's two independent, fail-closed authorization gates. The
/// first is the transport meta-authorizer (<see cref="ILatticeAuthApiAuthorizer"/>,
/// default <see cref="DenyAllAuthApiAuthorizer"/>): unconfigured, it rejects every
/// admin RPC with <see cref="StatusCode.PermissionDenied"/> at the edge before the
/// facade is reached. The second is the facade's own administrator check, reached
/// only once the transport gate allows the call: an anonymous or non-administrator
/// caller is denied there even past a permissive meta-authorizer, and the denial is
/// mapped to <see cref="StatusCode.PermissionDenied"/> carrying only non-sensitive
/// tree / operation / subject / reason trailers.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiGrpcAuthorizationTests
{
    private AuthApiGrpcClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiGrpcClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    /// <summary>
    /// One representative invocation per RPC, used to prove the transport gate
    /// rejects the whole surface uniformly. Requests are well-formed but never
    /// reach the facade when the meta-authorizer denies.
    /// </summary>
    private static IEnumerable<TestCaseData> AllOperations()
    {
        static TestCaseData Case(string name, Func<LatticeAuthApiGrpcClient, Task> op) =>
            new TestCaseData(op).SetName($"every_admin_op_is_denied_by_the_default_meta_authorizer({name})");

        yield return Case("UpsertGroup", c => c.UpsertGroupAsync(new AuthGroup { GroupId = "g" }));
        yield return Case("GetGroup", c => c.GetGroupAsync(new AuthGroupRef { GroupId = "g" }));
        yield return Case("RemoveGroup", c => c.RemoveGroupAsync(new AuthGroupRef { GroupId = "g" }));
        yield return Case("ListGroups", c => c.ListGroupsAsync(new AuthPageRequest()));
        yield return Case("AddMember", c => c.AddMemberAsync(new AuthMemberEdge { GroupId = "g", MemberId = "m" }));
        yield return Case("RemoveMember", c => c.RemoveMemberAsync(new AuthMemberEdge { GroupId = "g", MemberId = "m" }));
        yield return Case("ListGroupMembers", c => c.ListGroupMembersAsync(new AuthGroupRef { GroupId = "g" }));
        yield return Case("ListSubjectGroups", c => c.ListSubjectGroupsAsync(new AuthMemberRef { MemberId = "m" }));
        yield return Case("PutRule", c => c.PutRuleAsync(new AuthPutRule
        {
            Rule = new LatticeAuthorizationRule(
                "r", LatticeSubjectSelector.User("u"), LatticeScope.Tree("t"), LatticeOperation.Read, LatticeEffect.Allow),
        }));
        yield return Case("GetRule", c => c.GetRuleAsync(new AuthRuleRef { TreeId = "t", RuleId = "r" }));
        yield return Case("RemoveRule", c => c.RemoveRuleAsync(new AuthRuleRef { TreeId = "t", RuleId = "r" }));
        yield return Case("ListRules", c => c.ListRulesAsync(new AuthPageRequest()));
        yield return Case("ListRulesForTree", c => c.ListRulesForTreeAsync(new AuthTreeRulesPage { TreeId = "t" }));
        yield return Case("Explain", c => c.ExplainAsync(new AuthExplainQuery
        {
            SubjectId = "u",
            Operation = LatticeOperation.Read,
            Scope = LatticeScope.Tree("t"),
        }));
        yield return Case("EffectivePermissions", c => c.EffectivePermissionsAsync(new AuthSubjectRef { SubjectId = "u" }));
        yield return Case("SearchDirectory", c => c.SearchDirectoryAsync(new DirectorySearchRequest { Term = "u" }));
        yield return Case("ResolveDirectoryPrincipal", c => c.ResolveDirectoryPrincipalAsync(new AuthPrincipalRef { PrincipalId = "u" }));
        yield return Case("GetAccessModel", c => c.GetAccessModelAsync(new AuthAccessModelQuery()));
    }

    [TestCaseSource(nameof(AllOperations))]
    public async Task every_admin_op_is_denied_by_the_default_meta_authorizer(Func<LatticeAuthApiGrpcClient, Task> operation)
    {
        // Default-deny transport gate (no authorizer registered, enforcement on).
        // Even a bootstrap-administrator credential cannot pass the edge.
        await using var host = await _fixture.CreateGrpcHostAsync(requireAuthorization: true);
        var client = host.ClientFor(AuthApiGrpcClusterFixture.BootstrapAdmin);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await operation(client));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task explicit_allow_all_meta_authorizer_permits_the_administrator()
    {
        await using var host = await _fixture.CreateGrpcHostAsync(
            requireAuthorization: true,
            authorizer: new AllowAllAuthApiAuthorizer());
        var admin = host.ClientFor(AuthApiGrpcClusterFixture.BootstrapAdmin);

        // Past the permissive transport gate, the facade's administrator check
        // admits the bootstrap admin: the call completes without throwing.
        await admin.UpsertGroupAsync(new AuthGroup { GroupId = "allow-all-ok", DisplayName = "Ok" });

        var fetched = await admin.GetGroupAsync(new AuthGroupRef { GroupId = "allow-all-ok" });
        Assert.That(fetched.Group, Is.Not.Null);
    }

    [Test]
    public async Task anonymous_caller_past_the_meta_authorizer_is_denied_by_the_facade_admin_check()
    {
        await using var host = await _fixture.CreateGrpcHostAsync(
            requireAuthorization: true,
            authorizer: new AllowAllAuthApiAuthorizer());

        // No credential header: the transport gate allows the call, but the facade
        // resolves an anonymous caller and its administrator check fails closed.
        var anonymous = host.ClientFor(subject: null);

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await anonymous.UpsertGroupAsync(new AuthGroup { GroupId = "anon-should-fail" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));

            var trailers = ex.Trailers;
            Assert.That(
                trailers.GetValue(LatticeAuthApiGrpcService.DeniedTreeTrailer),
                Is.EqualTo(LatticeAuthReservedTrees.PolicyTreeId),
                "the denial is scoped to the reserved policy tree");
            Assert.That(
                trailers.GetValue(LatticeAuthApiGrpcService.DeniedSubjectTrailer),
                Is.EqualTo("anonymous"),
                "the anonymous caller's subject is surfaced in the trailer");
            Assert.That(trailers.GetValue(LatticeAuthApiGrpcService.DeniedOperationTrailer), Is.Not.Null);
            Assert.That(trailers.GetValue(LatticeAuthApiGrpcService.DeniedReasonTrailer), Is.Not.Null);
        });
    }

    [Test]
    public async Task non_administrator_past_the_meta_authorizer_is_denied_by_the_facade_admin_check()
    {
        await using var host = await _fixture.CreateGrpcHostAsync(
            requireAuthorization: true,
            authorizer: new AllowAllAuthApiAuthorizer());

        // A resolvable but non-administrator subject: past the transport gate, the
        // facade's administrator check still denies, surfacing the real subject.
        var nonAdmin = host.ClientFor("grpc-non-admin");

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await nonAdmin.PutRuleAsync(new AuthPutRule
            {
                Rule = new LatticeAuthorizationRule(
                    "nonadmin-rule",
                    LatticeSubjectSelector.User("x"),
                    LatticeScope.Tree("t"),
                    LatticeOperation.Read,
                    LatticeEffect.Allow),
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(
                ex.Trailers.GetValue(LatticeAuthApiGrpcService.DeniedSubjectTrailer),
                Is.EqualTo("grpc-non-admin"));
        });
    }

    [Test]
    public async Task disabled_transport_gate_still_defers_to_the_facade_admin_check()
    {
        // RequireAuthorization=false removes the transport gate entirely, but the
        // facade's administrator check is not bypassed: an anonymous caller is
        // still denied. This is the load-bearing second gate.
        await using var host = await _fixture.CreateGrpcHostAsync(requireAuthorization: false);
        var anonymous = host.ClientFor(subject: null);

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await anonymous.UpsertGroupAsync(new AuthGroup { GroupId = "anon-group" }));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
