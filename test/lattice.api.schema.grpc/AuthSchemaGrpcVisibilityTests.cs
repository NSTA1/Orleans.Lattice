using Grpc.Core;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Transport-level authorization coverage for the schema control-API gRPC
/// binding. Proves the default-deny posture end-to-end: with authorization
/// enforced and a credential-gated authorizer, every schema control-API RPC is
/// rejected with <see cref="StatusCode.PermissionDenied"/> unless the call
/// carries a credential header, and accepted when it does - while the
/// unauthenticated <c>GetAuthScheme</c> discovery RPC is reachable either way so
/// a client can learn how to sign in before it holds a credential.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthSchemaGrpcVisibilityTests
{
    private const string Tree = "orders";
    private const string CredentialScheme = "Bearer";
    private const string Operator = "schema-operator";

    private GrpcSchemaClusterFixture _fixture = null!;
    private GrpcSchemaHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcSchemaClusterFixture();
        await _fixture.InitializeAsync();

        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", "{}"u8.ToArray());

        _host = await _fixture.CreateGrpcHostAsync(
            new CredentialPresentSchemaApiAuthorizer(),
            requireAuthorization: true);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private static CallOptions WithSubject(string? subject)
    {
        if (subject is null)
        {
            return new CallOptions();
        }

        var headers = new global::Grpc.Core.Metadata { { "authorization", $"{CredentialScheme} {subject}" } };
        return new CallOptions(headers);
    }

    private async Task<TResponse> CallAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        string? subject)
        where TRequest : class
        where TResponse : class
    {
        var invoker = _host.Channel.CreateCallInvoker();
        using var call = invoker.AsyncUnaryCall(method, host: null, WithSubject(subject), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public async Task get_auth_scheme_is_reachable_without_a_credential()
    {
        var advertisement = await CallAsync(
            _host.Methods.GetAuthScheme,
            new AuthSchemeAdvertisementRequest(),
            subject: null);

        Assert.That(advertisement, Is.Not.Null);
    }

    [Test]
    public void get_policy_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.GetPolicy,
            new SchemaTreeRequest { TreeId = Tree },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task get_policy_with_a_credential_is_accepted()
    {
        var response = await CallAsync(
            _host.Methods.GetPolicy,
            new SchemaTreeRequest { TreeId = Tree },
            Operator);

        Assert.That(response, Is.Not.Null);
    }

    [Test]
    public void set_policy_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.SetPolicy,
            new SetPolicyRequest { TreeId = Tree, Policy = JsonPolicy() },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task set_policy_with_a_credential_is_accepted()
    {
        var response = await CallAsync(
            _host.Methods.SetPolicy,
            new SetPolicyRequest { TreeId = Tree, Policy = JsonPolicy() },
            Operator);

        Assert.That(response, Is.Not.Null);
    }

    [Test]
    public void scan_compliance_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.ScanCompliance,
            new SchemaTreeRequest { TreeId = Tree },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task scan_compliance_with_a_credential_is_accepted()
    {
        var response = await CallAsync(
            _host.Methods.ScanCompliance,
            new SchemaTreeRequest { TreeId = Tree },
            Operator);

        Assert.That(response.Report.TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public void probe_capabilities_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.ProbeCapabilities,
            new SchemaTreeRequest { TreeId = Tree },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task probe_capabilities_with_a_credential_is_accepted()
    {
        var capabilities = await CallAsync(
            _host.Methods.ProbeCapabilities,
            new SchemaTreeRequest { TreeId = Tree },
            Operator);

        Assert.That(capabilities.TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public void remediate_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.Remediate,
            new RemediateRequest
            {
                TreeId = Tree,
                Transform = LatticeValueTransform.Passthrough(),
                TargetPolicy = JsonPolicy(),
            },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void stream_dead_letters_without_a_credential_is_permission_denied()
    {
        var invoker = _host.Channel.CreateCallInvoker();
        using var call = invoker.AsyncServerStreamingCall(
            _host.Methods.StreamDeadLetters,
            host: null,
            WithSubject(subject: null),
            new SchemaTreeRequest { TreeId = Tree });

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            while (await call.ResponseStream.MoveNext(CancellationToken.None))
            {
            }
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void stream_dead_letters_with_a_credential_is_accepted()
    {
        var invoker = _host.Channel.CreateCallInvoker();
        using var call = invoker.AsyncServerStreamingCall(
            _host.Methods.StreamDeadLetters,
            host: null,
            WithSubject(Operator),
            new SchemaTreeRequest { TreeId = Tree });

        // Draining without an RpcException proves the authorizer admitted the stream.
        Assert.That(async () =>
        {
            while (await call.ResponseStream.MoveNext(CancellationToken.None))
            {
            }
        }, Throws.Nothing);
    }

    /// <summary>
    /// Authorizer that admits a call only when it carries an <c>authorization</c>
    /// request header, so the tests can drive both the accept and default-deny
    /// paths purely from the wire credential.
    /// </summary>
    private sealed class CredentialPresentSchemaApiAuthorizer : ILatticeSchemaApiAuthorizer
    {
        public Task<bool> IsAuthorizedAsync(
            LatticeSchemaApiAuthorizationContext authorizationContext,
            CancellationToken cancellationToken)
        {
            var header = authorizationContext.Call.RequestHeaders?.GetValue("authorization");
            return Task.FromResult(!string.IsNullOrWhiteSpace(header));
        }
    }
}
