using Grpc.Core;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// End-to-end coverage for the coarse transport-level authorization gate. When a
/// host maps the write-capable data-API gRPC surface with the default-deny
/// posture left in place (<see cref="LatticeDataApiGrpcOptions.RequireAuthorization"/>
/// at its <see langword="true"/> default and the built-in
/// <see cref="DenyAllDataApiAuthorizer"/>), every inbound call is rejected with
/// <see cref="StatusCode.PermissionDenied"/> before it ever reaches the facade -
/// even for a caller the per-key gate would otherwise authorize. This proves the
/// surface is fail-closed and opt-in at the transport boundary, independent of
/// the per-tree / per-key enforcement.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoarseTransportGateDataTests
{
    private const string Writer = "coarse-writer";

    private AuthGrpcDataClusterFixture _fixture = null!;
    private GrpcDataHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthGrpcDataClusterFixture();
        await _fixture.InitializeAsync();

        // Grant the writer at the per-key layer so the only thing that can reject
        // the call is the coarse transport gate.
        await _fixture.RegisterTreeAsync("coarse-tree");
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "coarse-writer-tree",
            LatticeSubjectSelector.User(Writer),
            LatticeScope.Tree("coarse-tree"),
            LatticeOperation.Write,
            LatticeEffect.Allow));

        _host = await _fixture.CreateGrpcHostAsync(requireAuthorization: true);
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

    [Test]
    public void default_deny_coarse_gate_rejects_even_a_per_key_authorized_caller()
    {
        var headers = new global::Grpc.Core.Metadata
        {
            { "authorization", $"{AuthGrpcDataClusterFixture.CredentialScheme} {Writer}" },
        };
        var invoker = _host.Channel.CreateCallInvoker();

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            using var call = invoker.AsyncUnaryCall(
                _host.Methods.Set,
                host: null,
                new CallOptions(headers),
                new DataSetRequest { TreeId = "coarse-tree", Key = "k1", Value = new byte[] { 1 } });
            await call.ResponseAsync.ConfigureAwait(false);
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));

        // Nothing reached the facade, so nothing was persisted.
        Assert.That(_fixture.ReadRawAsync("coarse-tree", "k1").Result, Is.Null);
    }
}
