using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Unit coverage that the transport meta-authorizer gate covers each of the three
/// new identity-directory / access-model RPCs exactly like every other admin RPC:
/// the default-deny authorizer rejects the call with
/// <see cref="StatusCode.PermissionDenied"/> before the continuation runs, and a
/// permissive authorizer lets it through. Driven directly against
/// <see cref="LatticeAuthApiGrpcAuthInterceptor.UnaryServerHandler{TRequest, TResponse}"/>
/// with a fake <see cref="ServerCallContext"/>, so no cluster or channel is
/// needed - the gating decision keys off the gRPC method name alone.
/// </summary>
[TestFixture]
public sealed class DirectoryRpcInterceptorGatingTests
{
    private static LatticeAuthApiGrpcAuthInterceptor CreateInterceptor(ILatticeAuthApiAuthorizer authorizer)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeAuthApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeAuthApiGrpcOptions());
        return new LatticeAuthApiGrpcAuthInterceptor(
            authorizer,
            options,
            NullLogger<LatticeAuthApiGrpcAuthInterceptor>.Instance);
    }

    private static string FullName(string methodName) => $"/{LatticeAuthApiGrpcMethods.ServiceName}/{methodName}";

    private static IEnumerable<TestCaseData> NewDirectoryRpcs()
    {
        yield return new TestCaseData(
            LatticeAuthApiGrpcMethods.SearchDirectoryMethodName,
            (object)new DirectorySearchRequest { Term = "al" })
            .SetName("SearchDirectory");
        yield return new TestCaseData(
            LatticeAuthApiGrpcMethods.ResolveDirectoryPrincipalMethodName,
            (object)new AuthPrincipalRef { PrincipalId = "alice" })
            .SetName("ResolveDirectoryPrincipal");
        yield return new TestCaseData(
            LatticeAuthApiGrpcMethods.GetAccessModelMethodName,
            (object)new AuthAccessModelQuery())
            .SetName("GetAccessModel");
    }

    [TestCaseSource(nameof(NewDirectoryRpcs))]
    public void Default_deny_authorizer_denies_the_new_rpc_before_the_continuation_runs(string methodName, object request)
    {
        var interceptor = CreateInterceptor(new DenyAllAuthApiAuthorizer());
        var context = new LoopbackServerCallContext(FullName(methodName));
        var continued = false;

        Task<string> Continuation(object req, ServerCallContext ctx)
        {
            continued = true;
            return Task.FromResult("ok");
        }

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await interceptor.UnaryServerHandler<object, string>(request, context, Continuation));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(continued, Is.False, "a denied call must never reach the facade continuation");
        });
    }

    [TestCaseSource(nameof(NewDirectoryRpcs))]
    public async Task Permissive_authorizer_admits_the_new_rpc_to_the_continuation(string methodName, object request)
    {
        var interceptor = CreateInterceptor(new AllowAllAuthApiAuthorizer());
        var context = new LoopbackServerCallContext(FullName(methodName));
        var continued = false;

        Task<string> Continuation(object req, ServerCallContext ctx)
        {
            continued = true;
            return Task.FromResult("ok");
        }

        var response = await interceptor.UnaryServerHandler<object, string>(request, context, Continuation);

        Assert.Multiple(() =>
        {
            Assert.That(continued, Is.True, "a permitted call must reach the facade continuation");
            Assert.That(response, Is.EqualTo("ok"));
        });
    }
}
