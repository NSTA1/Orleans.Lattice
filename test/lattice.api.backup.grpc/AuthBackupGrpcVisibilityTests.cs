using Grpc.Core;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Transport-level authorization coverage for the backup control-API gRPC
/// binding. Proves the default-deny posture end-to-end: with authorization
/// enforced and a credential-gated authorizer, every backup control-API RPC is
/// rejected with <see cref="StatusCode.PermissionDenied"/> unless the call
/// carries a credential header, and accepted when it does - while the
/// unauthenticated <c>GetAuthScheme</c> discovery RPC is reachable either way so
/// a client can learn how to sign in before it holds a credential.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthBackupGrpcVisibilityTests
{
    private const string Source = "orders";
    private const string CredentialScheme = "Bearer";
    private const string Operator = "backup-operator";

    private GrpcBackupClusterFixture _fixture = null!;
    private GrpcBackupHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcBackupClusterFixture();
        await _fixture.InitializeAsync();

        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", new byte[] { 1, 2, 3 });

        _host = await _fixture.CreateGrpcHostAsync(
            new CredentialPresentBackupApiAuthorizer(),
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
    public void list_backups_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.ListBackups,
            new BackupCatalogRequest(),
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task list_backups_with_a_credential_is_accepted()
    {
        var page = await CallAsync(
            _host.Methods.ListBackups,
            new BackupCatalogRequest(),
            Operator);

        Assert.That(page, Is.Not.Null);
    }

    [Test]
    public void create_backup_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.CreateBackup,
            new BackupCaptureRequestMessage
            {
                Name = "denied",
                Scope = BackupScopeSelector.WholeTree(Source),
            },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task create_backup_with_a_credential_is_accepted()
    {
        var response = await CallAsync(
            _host.Methods.CreateBackup,
            new BackupCaptureRequestMessage
            {
                Name = "allowed",
                Scope = BackupScopeSelector.WholeTree(Source),
            },
            Operator);

        Assert.That(response.BackupId, Is.Not.Empty);
    }

    [Test]
    public void create_backup_set_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.CreateBackupSet,
            new BackupSetCaptureRequestMessage
            {
                Name = "denied-set",
                Scopes = new[] { BackupScopeSelector.WholeTree(Source) },
            },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task create_backup_set_with_a_credential_is_accepted()
    {
        var response = await CallAsync(
            _host.Methods.CreateBackupSet,
            new BackupSetCaptureRequestMessage
            {
                Name = "allowed-set",
                Scopes = new[] { BackupScopeSelector.WholeTree(Source) },
            },
            Operator);

        Assert.That(response.SetManifest.MemberBackupIds, Is.Not.Empty);
    }

    [Test]
    public void restore_backup_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.RestoreBackup,
            new RestoreRequestMessage { BackupId = "does-not-matter", TargetTreeId = "target" },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void stream_backups_without_a_credential_is_permission_denied()
    {
        var invoker = _host.Channel.CreateCallInvoker();
        using var call = invoker.AsyncServerStreamingCall(
            _host.Methods.StreamBackups,
            host: null,
            WithSubject(subject: null),
            new BackupStreamRequest());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            while (await call.ResponseStream.MoveNext(CancellationToken.None))
            {
            }
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task stream_backups_with_a_credential_is_accepted()
    {
        var invoker = _host.Channel.CreateCallInvoker();
        using var call = invoker.AsyncServerStreamingCall(
            _host.Methods.StreamBackups,
            host: null,
            WithSubject(Operator),
            new BackupStreamRequest());

        // Draining without an RpcException proves the authorizer admitted the stream.
        Assert.That(async () =>
        {
            while (await call.ResponseStream.MoveNext(CancellationToken.None))
            {
            }
        }, Throws.Nothing);

        await Task.CompletedTask;
    }

    [Test]
    public void schedule_backup_without_a_credential_is_permission_denied()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.ScheduleBackup,
            new BackupScheduleRequestMessage
            {
                Scope = BackupScopeSelector.WholeTree(Source),
                Incremental = false,
                IntervalTicks = TimeSpan.FromMinutes(20).Ticks,
            },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task schedule_backup_with_a_credential_is_accepted()
    {
        var response = await CallAsync(
            _host.Methods.ScheduleBackup,
            new BackupScheduleRequestMessage
            {
                Scope = BackupScopeSelector.WholeTree(Source),
                Incremental = false,
                IntervalTicks = TimeSpan.FromMinutes(20).Ticks,
            },
            Operator);

        Assert.That(response.Scheduled, Is.True);
    }

    /// <summary>
    /// <c>authorization</c> request header, so the tests can drive both the
    /// accept and default-deny paths purely from the wire credential.
    /// </summary>
    private sealed class CredentialPresentBackupApiAuthorizer : ILatticeBackupApiAuthorizer
    {
        public Task<bool> IsAuthorizedAsync(
            LatticeBackupApiAuthorizationContext authorizationContext,
            CancellationToken cancellationToken)
        {
            var header = authorizationContext.Call.RequestHeaders?.GetValue("authorization");
            return Task.FromResult(!string.IsNullOrWhiteSpace(header));
        }
    }
}
