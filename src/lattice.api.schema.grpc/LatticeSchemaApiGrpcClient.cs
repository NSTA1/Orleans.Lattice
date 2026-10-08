using System.Runtime.CompilerServices;
using Grpc.Core;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Strongly-typed client for the schema control-API gRPC surface. Wraps a gRPC
/// <see cref="CallInvoker"/> and the code-first method definitions, re-exposing
/// the transport-agnostic <see cref="ILatticeSchemaControl"/> facade surface over
/// the wire: policy management, dead-letter streaming / counting, versioning,
/// remediation, the read-only compliance audit, the capability probe, and
/// auth-scheme discovery. The Orleans.Lattice.Explorer schema tab (and any CLI
/// or dashboard) consumes the API through this client rather than hand-rolling
/// channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the
/// <see cref="CallInvoker"/> / <c>GrpcChannel</c> the caller supplies. Build one
/// with <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service
/// provider that has Orleans serialization registered (<c>AddSerializer()</c>)
/// so the wire marshallers match the server exactly.
/// </remarks>
public sealed class LatticeSchemaApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeSchemaGrpcMethods _methods;

    internal LatticeSchemaApiGrpcClient(CallInvoker invoker, LatticeSchemaGrpcMethods methods)
    {
        _invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
        _methods = methods ?? throw new ArgumentNullException(nameof(methods));
    }

    /// <summary>
    /// Creates a client over <paramref name="callInvoker"/>, building the wire
    /// marshallers from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="callInvoker">
    /// The gRPC call invoker, typically <c>channel.CreateCallInvoker()</c>.
    /// </param>
    /// <param name="serializerProvider">
    /// A service provider with Orleans serialization registered
    /// (<c>AddSerializer()</c>), used to resolve the per-message serializers.
    /// </param>
    /// <returns>A ready-to-use client.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static LatticeSchemaApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeSchemaApiGrpcClient(
            callInvoker,
            LatticeSchemaGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>Sets or replaces the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="policy">The policy to apply. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    public async Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(policy);
        await UnaryAsync(
            _methods.SetPolicy,
            new SetPolicyRequest { TreeId = treeId, Policy = policy },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Clears the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a policy was removed; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var response = await UnaryAsync(
            _methods.ClearPolicy,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken).ConfigureAwait(false);
        return response.Removed;
    }

    /// <summary>Reads the enforcement policy for <paramref name="treeId"/>, or <c>null</c> when none exists.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The policy, or <c>null</c> when none exists.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var response = await UnaryAsync(
            _methods.GetPolicy,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken).ConfigureAwait(false);
        return response.Found ? response.Policy : null;
    }

    /// <summary>Streams the strict-mode dead-letter entries retained for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async stream of dead-letter entries.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return ServerStreamingAsync(
            _methods.StreamDeadLetters,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>Counts the strict-mode dead-letter entries retained for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The dead-letter entry count.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var response = await UnaryAsync(
            _methods.CountDeadLetters,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken).ConfigureAwait(false);
        return response.Count;
    }

    /// <summary>Opts <paramref name="treeId"/> in to envelope versioning (or replaces its config).</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="config">The version configuration to install.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task SetVersionConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await UnaryAsync(
            _methods.SetVersionConfig,
            new SetVersionConfigRequest { TreeId = treeId, Config = config },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads the current version config for <paramref name="treeId"/>, or <c>null</c> when unversioned.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The version config, or <c>null</c> when the tree is unversioned.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var response = await UnaryAsync(
            _methods.GetVersionConfig,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken).ConfigureAwait(false);
        return response.Found ? response.Config : null;
    }

    /// <summary>Advances <paramref name="treeId"/>'s target schema version.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated config.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var response = await UnaryAsync(
            _methods.AdvanceTargetVersion,
            new AdvanceVersionRequest { TreeId = treeId, NewTargetVersion = newTargetVersion },
            cancellationToken).ConfigureAwait(false);
        return response.Config;
    }

    /// <summary>Opts <paramref name="treeId"/> back out of envelope versioning.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a config was removed; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var response = await UnaryAsync(
            _methods.ClearVersionConfig,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken).ConfigureAwait(false);
        return response.Removed;
    }

    /// <summary>Reads the current or last-known remediation status for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The remediation status report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var response = await UnaryAsync(
            _methods.GetRemediationStatus,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken).ConfigureAwait(false);
        return response.Report;
    }

    /// <summary>
    /// Starts an accept-then-poll compliance scan of <paramref name="treeId"/> and
    /// returns its handle at once; poll <see cref="GetComplianceScanStatusAsync"/>
    /// for progress and the outcome.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> lets the server generate one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started scan.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<LatticeOperationHandle> StartComplianceScanAsync(
        string treeId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.StartComplianceScan,
            new SchemaComplianceScanStartRequest { TreeId = treeId, OperationId = operationId },
            cancellationToken);
    }

    /// <summary>Reads a compliance-scan operation's status.</summary>
    /// <param name="operationId">The operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status, or <see langword="null"/> when no such operation is visible to the caller.</returns>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeOperationStatus?> GetComplianceScanStatusAsync(
        string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var response = await UnaryAsync(
            _methods.GetComplianceScanStatus,
            new SchemaComplianceOperationRequest { OperationId = operationId },
            cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    /// <summary>Lists one page of the caller's compliance-scan operations, newest-first.</summary>
    /// <param name="request">The page request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The page.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    public Task<LatticeOperationPage> ListComplianceScansAsync(
        LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return UnaryAsync(_methods.ListComplianceScans, request, cancellationToken);
    }

    /// <summary>Requests cancellation of a compliance-scan operation.</summary>
    /// <param name="operationId">The operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status after the request, or <see langword="null"/> when no such operation is visible to the caller.</returns>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeOperationStatus?> CancelComplianceScanAsync(
        string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var response = await UnaryAsync(
            _methods.CancelComplianceScan,
            new SchemaComplianceOperationRequest { OperationId = operationId },
            cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    /// <summary>
    /// Probes which schema-management operations the current caller may perform
    /// over <paramref name="treeId"/>, with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's allowed schema-management operation set for <paramref name="treeId"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.ProbeCapabilities,
            new SchemaTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads the endpoint's advertised auth schemes. Unauthenticated: this RPC is
    /// exempt from the server's authorization interceptor, so a client can learn
    /// how to sign in before it holds any credential.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The advertised auth schemes, in the server's preference order.</returns>
    public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)
    {
        var response = await UnaryAsync(
            _methods.GetAuthScheme,
            new AuthSchemeAdvertisementRequest(),
            cancellationToken).ConfigureAwait(false);
        return response.Schemes;
    }

    /// <summary>
    /// Starts a tracked remediation of <paramref name="treeId"/> and returns as soon
    /// as it is accepted; poll <see cref="GetSchemaOperationStatusAsync"/> for its
    /// phase, values processed and outcome.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="transform">The per-value remediation transform.</param>
    /// <param name="targetPolicy">The policy the transformed values must satisfy. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> has the server generate one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="targetPolicy"/> is <c>null</c>.</exception>
    public Task<LatticeOperationHandle> StartRemediationAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        return UnaryAsync(
            _methods.StartRemediation,
            new RemediateRequest { TreeId = treeId, Transform = transform, TargetPolicy = targetPolicy, OperationId = operationId },
            cancellationToken);
    }

    /// <summary>
    /// Starts a tracked eager migration of <paramref name="treeId"/> to its current
    /// target version and returns as soon as it is accepted.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> has the server generate one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<LatticeOperationHandle> StartMigrationAsync(
        string treeId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.StartMigration,
            new SchemaMigrationStartRequest { TreeId = treeId, OperationId = operationId },
            cancellationToken);
    }

    /// <summary>
    /// Starts a tracked advance of <paramref name="treeId"/>'s target version to
    /// <paramref name="newTargetVersion"/>, then an eager migration to it, and returns
    /// as soon as it is accepted.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> has the server generate one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<LatticeOperationHandle> StartAdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.StartAdvanceAndMigrate,
            new AdvanceVersionRequest { TreeId = treeId, NewTargetVersion = newTargetVersion, OperationId = operationId },
            cancellationToken);
    }

    /// <summary>Reads a tracked schema operation's status.</summary>
    /// <param name="operationId">The operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status, or <see langword="null"/> when no such operation is visible to the caller.</returns>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeOperationStatus?> GetSchemaOperationStatusAsync(
        string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var response = await UnaryAsync(
            _methods.GetSchemaOperationStatus,
            new SchemaOperationRequest { OperationId = operationId },
            cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    /// <summary>Lists one page of the caller's tracked schema operations, newest-first.</summary>
    /// <param name="request">The page request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The page.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    public Task<LatticeOperationPage> ListSchemaOperationsAsync(
        LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return UnaryAsync(_methods.ListSchemaOperations, request, cancellationToken);
    }

    /// <summary>
    /// Requests cancellation of a tracked schema operation. A remediation that has
    /// reached cutover is not cancelled and runs on to completion.
    /// </summary>
    /// <param name="operationId">The operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status after the request, or <see langword="null"/> when no such operation is visible to the caller.</returns>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeOperationStatus?> CancelSchemaOperationAsync(
        string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var response = await UnaryAsync(
            _methods.CancelSchemaOperation,
            new SchemaOperationRequest { OperationId = operationId },
            cancellationToken).ConfigureAwait(false);
        return response.Status;
    }
    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncUnaryCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        return await call.ResponseAsync.ConfigureAwait(false);
    }

    private async IAsyncEnumerable<TResponse> ServerStreamingAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncServerStreamingCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current;
        }
    }
}
