using System.Globalization;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Abstract base for the schema control-API gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c> reflects
/// against to discover and register the unary RPCs (<c>SetPolicy</c>,
/// <c>ClearPolicy</c>, <c>GetPolicy</c>, <c>CountDeadLetters</c>,
/// <c>SetVersionConfig</c>, <c>GetVersionConfig</c>, <c>AdvanceTargetVersion</c>,
/// <c>AdvanceAndMigrate</c>, <c>MigrateToTargetVersion</c>,
/// <c>ClearVersionConfig</c>, <c>Remediate</c>, <c>GetRemediationStatus</c>,
/// <c>ScanCompliance</c>, <c>ProbeCapabilities</c>, <c>GetAuthScheme</c>) and the
/// server-streaming RPC (<c>StreamDeadLetters</c>).
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c> produces
/// for a <c>.proto</c> service: the base type bears the binding metadata the
/// binder discovers, and the derived type is the concrete implementation
/// resolved from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="LatticeSchemaGrpcServiceBase.BindService"/> once at startup with a
/// <see langword="null"/> instance to record method metadata, then resolves the
/// actual instance per request.
/// </remarks>
[BindServiceMethod(typeof(LatticeSchemaGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeSchemaGrpcServiceBase
{
    /// <summary>Sets or replaces a tree's schema policy. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaAckResponse> SetPolicy(SetPolicyRequest request, ServerCallContext context);

    /// <summary>Clears a tree's schema policy. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaRemovedResponse> ClearPolicy(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Reads a tree's schema policy. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<GetPolicyResponse> GetPolicy(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Streams a tree's dead-letter entries. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task StreamDeadLetters(
        SchemaTreeRequest request,
        IServerStreamWriter<LatticeSchemaDeadLetterEntry> responseStream,
        ServerCallContext context);

    /// <summary>Counts a tree's dead-letter entries. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaCountResponse> CountDeadLetters(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Sets or replaces a tree's version config. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaAckResponse> SetVersionConfig(SetVersionConfigRequest request, ServerCallContext context);

    /// <summary>Reads a tree's version config. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<GetVersionConfigResponse> GetVersionConfig(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Advances a tree's target schema version. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<VersionConfigResponse> AdvanceTargetVersion(AdvanceVersionRequest request, ServerCallContext context);

    /// <summary>Advances a tree's target version and eagerly migrates. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaRemediationReportResponse> AdvanceAndMigrate(AdvanceVersionRequest request, ServerCallContext context);

    /// <summary>Migrates a tree to its current target version. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaRemediationReportResponse> MigrateToTargetVersion(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Clears a tree's version config. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaRemovedResponse> ClearVersionConfig(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Runs (or resumes) a tree's remediation. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaRemediationReportResponse> Remediate(RemediateRequest request, ServerCallContext context);

    /// <summary>Reads a tree's remediation status. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaRemediationReportResponse> GetRemediationStatus(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Scans a tree for compliance against its policy. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<SchemaComplianceReportResponse> ScanCompliance(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>Probes the caller's schema-management capabilities for a tree. Implemented in <see cref="LatticeSchemaGrpcService"/>.</summary>
    public abstract Task<LatticeSchemaCapabilities> ProbeCapabilities(SchemaTreeRequest request, ServerCallContext context);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. Unauthenticated: this RPC
    /// is exempt from the authorization interceptor so a client can learn how to
    /// sign in before it holds any credential. Implemented in
    /// <see cref="LatticeSchemaGrpcService"/>.
    /// </summary>
    public abstract Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at
    /// startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual service
    /// instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeSchemaGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeSchemaGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeSchemaGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeSchemaApiGrpcServiceCollectionExtensions.AddLatticeSchemaApiGrpc)} ran and that "
                + $"{nameof(LatticeSchemaApiGrpcServiceCollectionExtensions.MapLatticeSchemaApiGrpc)} pre-resolved "
                + "LatticeSchemaGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.SetPolicy, (UnaryServerMethod<SetPolicyRequest, SchemaAckResponse>?)null);
            binder.AddMethod(methods.ClearPolicy, (UnaryServerMethod<SchemaTreeRequest, SchemaRemovedResponse>?)null);
            binder.AddMethod(methods.GetPolicy, (UnaryServerMethod<SchemaTreeRequest, GetPolicyResponse>?)null);
            binder.AddMethod(methods.StreamDeadLetters, (ServerStreamingServerMethod<SchemaTreeRequest, LatticeSchemaDeadLetterEntry>?)null);
            binder.AddMethod(methods.CountDeadLetters, (UnaryServerMethod<SchemaTreeRequest, SchemaCountResponse>?)null);
            binder.AddMethod(methods.SetVersionConfig, (UnaryServerMethod<SetVersionConfigRequest, SchemaAckResponse>?)null);
            binder.AddMethod(methods.GetVersionConfig, (UnaryServerMethod<SchemaTreeRequest, GetVersionConfigResponse>?)null);
            binder.AddMethod(methods.AdvanceTargetVersion, (UnaryServerMethod<AdvanceVersionRequest, VersionConfigResponse>?)null);
            binder.AddMethod(methods.AdvanceAndMigrate, (UnaryServerMethod<AdvanceVersionRequest, SchemaRemediationReportResponse>?)null);
            binder.AddMethod(methods.MigrateToTargetVersion, (UnaryServerMethod<SchemaTreeRequest, SchemaRemediationReportResponse>?)null);
            binder.AddMethod(methods.ClearVersionConfig, (UnaryServerMethod<SchemaTreeRequest, SchemaRemovedResponse>?)null);
            binder.AddMethod(methods.Remediate, (UnaryServerMethod<RemediateRequest, SchemaRemediationReportResponse>?)null);
            binder.AddMethod(methods.GetRemediationStatus, (UnaryServerMethod<SchemaTreeRequest, SchemaRemediationReportResponse>?)null);
            binder.AddMethod(methods.ScanCompliance, (UnaryServerMethod<SchemaTreeRequest, SchemaComplianceReportResponse>?)null);
            binder.AddMethod(methods.ProbeCapabilities, (UnaryServerMethod<SchemaTreeRequest, LatticeSchemaCapabilities>?)null);
            binder.AddMethod(methods.GetAuthScheme, (UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>?)null);
            return;
        }

        binder.AddMethod(methods.SetPolicy, new UnaryServerMethod<SetPolicyRequest, SchemaAckResponse>(serviceImpl.SetPolicy));
        binder.AddMethod(methods.ClearPolicy, new UnaryServerMethod<SchemaTreeRequest, SchemaRemovedResponse>(serviceImpl.ClearPolicy));
        binder.AddMethod(methods.GetPolicy, new UnaryServerMethod<SchemaTreeRequest, GetPolicyResponse>(serviceImpl.GetPolicy));
        binder.AddMethod(methods.StreamDeadLetters, new ServerStreamingServerMethod<SchemaTreeRequest, LatticeSchemaDeadLetterEntry>(serviceImpl.StreamDeadLetters));
        binder.AddMethod(methods.CountDeadLetters, new UnaryServerMethod<SchemaTreeRequest, SchemaCountResponse>(serviceImpl.CountDeadLetters));
        binder.AddMethod(methods.SetVersionConfig, new UnaryServerMethod<SetVersionConfigRequest, SchemaAckResponse>(serviceImpl.SetVersionConfig));
        binder.AddMethod(methods.GetVersionConfig, new UnaryServerMethod<SchemaTreeRequest, GetVersionConfigResponse>(serviceImpl.GetVersionConfig));
        binder.AddMethod(methods.AdvanceTargetVersion, new UnaryServerMethod<AdvanceVersionRequest, VersionConfigResponse>(serviceImpl.AdvanceTargetVersion));
        binder.AddMethod(methods.AdvanceAndMigrate, new UnaryServerMethod<AdvanceVersionRequest, SchemaRemediationReportResponse>(serviceImpl.AdvanceAndMigrate));
        binder.AddMethod(methods.MigrateToTargetVersion, new UnaryServerMethod<SchemaTreeRequest, SchemaRemediationReportResponse>(serviceImpl.MigrateToTargetVersion));
        binder.AddMethod(methods.ClearVersionConfig, new UnaryServerMethod<SchemaTreeRequest, SchemaRemovedResponse>(serviceImpl.ClearVersionConfig));
        binder.AddMethod(methods.Remediate, new UnaryServerMethod<RemediateRequest, SchemaRemediationReportResponse>(serviceImpl.Remediate));
        binder.AddMethod(methods.GetRemediationStatus, new UnaryServerMethod<SchemaTreeRequest, SchemaRemediationReportResponse>(serviceImpl.GetRemediationStatus));
        binder.AddMethod(methods.ScanCompliance, new UnaryServerMethod<SchemaTreeRequest, SchemaComplianceReportResponse>(serviceImpl.ScanCompliance));
        binder.AddMethod(methods.ProbeCapabilities, new UnaryServerMethod<SchemaTreeRequest, LatticeSchemaCapabilities>(serviceImpl.ProbeCapabilities));
        binder.AddMethod(methods.GetAuthScheme, new UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(serviceImpl.GetAuthScheme));
    }
}

/// <summary>
/// Server-side gRPC service for the schema control API. Adapts each RPC onto the
/// transport-agnostic <see cref="ILatticeSchemaControl"/> facade, mapping the
/// facade's plain results onto the serializable wire responses and translating
/// typed not-founds, argument failures, precondition failures, admission
/// refusals, and authorization denials onto gRPC status codes. The streaming RPC
/// drains the facade's <see cref="IAsyncEnumerable{T}"/> straight to the response
/// stream with bounded memory.
/// </summary>
internal sealed class LatticeSchemaGrpcService : LatticeSchemaGrpcServiceBase
{
    /// <summary>
    /// Trailer key carrying the breached quota dimension (<c>keys</c> or
    /// <c>bytes</c> for the per-tree admission caps a remediation build can
    /// reach), so a client can branch on it without parsing the status message.
    /// </summary>
    internal const string QuotaDimensionTrailer = "lattice-quota-dimension";

    /// <summary>Trailer key carrying the tree whose admission quota was breached.</summary>
    internal const string QuotaTreeTrailer = "lattice-quota-tree";

    /// <summary>
    /// Trailer key carrying the observed value on the breached dimension at the
    /// moment the write was refused. Omitted for a dimension that carries no
    /// numeric ceiling.
    /// </summary>
    internal const string QuotaCurrentTrailer = "lattice-quota-current";

    /// <summary>
    /// Trailer key carrying the configured ceiling on the breached dimension.
    /// Omitted for a dimension that carries no numeric ceiling.
    /// </summary>
    internal const string QuotaLimitTrailer = "lattice-quota-limit";

    private readonly ILatticeSchemaControl _control;
    private readonly ILatticeSchemaApiCredentialBridge _credentialBridge;
    private readonly ILatticeSchemaApiAuthSchemeSource _authSchemeSource;
    private readonly ILogger<LatticeSchemaGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is
    /// unused in the body but load-bearing on the constructor: resolving it
    /// forces the DI container to build the <see cref="LatticeSchemaGrpcMethods"/>
    /// singleton (whose factory populates
    /// <see cref="LatticeSchemaGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static <see cref="LatticeSchemaGrpcServiceBase.BindService"/>
    /// hook always observes a populated holder.
    /// </summary>
    public LatticeSchemaGrpcService(
        LatticeSchemaGrpcMethods methods,
        ILatticeSchemaControl control,
        ILatticeSchemaApiCredentialBridge credentialBridge,
        ILatticeSchemaApiAuthSchemeSource authSchemeSource,
        ILogger<LatticeSchemaGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(authSchemeSource);
        ArgumentNullException.ThrowIfNull(logger);

        _control = control;
        _credentialBridge = credentialBridge;
        _authSchemeSource = authSchemeSource;
        _logger = logger;
    }

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the schema engine's own fail-closed access gate resolves the
    /// caller's subject. Returns <see langword="null"/> (no scope) when the call
    /// carries no credential, leaving the caller anonymous. This is orthogonal
    /// to, and runs after, the transport-level
    /// <see cref="ILatticeSchemaApiAuthorizer"/> gate.
    /// </summary>
    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <inheritdoc />
    public override Task<SchemaAckResponse> SetPolicy(SetPolicyRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            await control.SetPolicyAsync(req.TreeId, req.Policy, ct).ConfigureAwait(false);
            return new SchemaAckResponse();
        });

    /// <inheritdoc />
    public override Task<SchemaRemovedResponse> ClearPolicy(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var removed = await control.ClearPolicyAsync(req.TreeId, ct).ConfigureAwait(false);
            return new SchemaRemovedResponse { Removed = removed };
        });

    /// <inheritdoc />
    public override Task<GetPolicyResponse> GetPolicy(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var policy = await control.GetPolicyAsync(req.TreeId, ct).ConfigureAwait(false);
            return policy is null
                ? new GetPolicyResponse { Found = false }
                : new GetPolicyResponse { Found = true, Policy = policy };
        });

    /// <inheritdoc />
    public override async Task StreamDeadLetters(
        SchemaTreeRequest request,
        IServerStreamWriter<LatticeSchemaDeadLetterEntry> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);

        try
        {
            await foreach (var entry in _control
                .ListDeadLettersAsync(request.TreeId, context.CancellationToken)
                .ConfigureAwait(false))
            {
                await responseStream.WriteAsync(entry).ConfigureAwait(false);
            }
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            // Client tore down the stream; a clean return ends it.
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Schema: gRPC dead-letter stream failed.");
            throw new RpcException(new Status(StatusCode.Internal, "The schema dead-letter stream failed."));
        }
    }

    /// <inheritdoc />
    public override Task<SchemaCountResponse> CountDeadLetters(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var count = await control.CountDeadLettersAsync(req.TreeId, ct).ConfigureAwait(false);
            return new SchemaCountResponse { Count = count };
        });

    /// <inheritdoc />
    public override Task<SchemaAckResponse> SetVersionConfig(SetVersionConfigRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            await control.SetVersionConfigAsync(req.TreeId, req.Config, ct).ConfigureAwait(false);
            return new SchemaAckResponse();
        });

    /// <inheritdoc />
    public override Task<GetVersionConfigResponse> GetVersionConfig(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var config = await control.GetVersionConfigAsync(req.TreeId, ct).ConfigureAwait(false);
            return config is { } value
                ? new GetVersionConfigResponse { Found = true, Config = value }
                : new GetVersionConfigResponse { Found = false };
        });

    /// <inheritdoc />
    public override Task<VersionConfigResponse> AdvanceTargetVersion(AdvanceVersionRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var config = await control.AdvanceTargetVersionAsync(req.TreeId, req.NewTargetVersion, ct).ConfigureAwait(false);
            return new VersionConfigResponse { Config = config };
        });

    /// <inheritdoc />
    public override Task<SchemaRemediationReportResponse> AdvanceAndMigrate(AdvanceVersionRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var report = await control.AdvanceAndMigrateAsync(req.TreeId, req.NewTargetVersion, ct).ConfigureAwait(false);
            return new SchemaRemediationReportResponse { Report = report };
        });

    /// <inheritdoc />
    public override Task<SchemaRemediationReportResponse> MigrateToTargetVersion(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var report = await control.MigrateToTargetVersionAsync(req.TreeId, ct).ConfigureAwait(false);
            return new SchemaRemediationReportResponse { Report = report };
        });

    /// <inheritdoc />
    public override Task<SchemaRemovedResponse> ClearVersionConfig(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var removed = await control.ClearVersionConfigAsync(req.TreeId, ct).ConfigureAwait(false);
            return new SchemaRemovedResponse { Removed = removed };
        });

    /// <inheritdoc />
    public override Task<SchemaRemediationReportResponse> Remediate(RemediateRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var report = await control.RemediateAsync(req.TreeId, req.Transform, req.TargetPolicy, ct).ConfigureAwait(false);
            return new SchemaRemediationReportResponse { Report = report };
        });

    /// <inheritdoc />
    public override Task<SchemaRemediationReportResponse> GetRemediationStatus(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var report = await control.GetRemediationStatusAsync(req.TreeId, ct).ConfigureAwait(false);
            return new SchemaRemediationReportResponse { Report = report };
        });

    /// <inheritdoc />
    public override Task<SchemaComplianceReportResponse> ScanCompliance(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var report = await control.ScanComplianceAsync(req.TreeId, ct).ConfigureAwait(false);
            return new SchemaComplianceReportResponse { Report = report };
        });

    /// <inheritdoc />
    public override Task<LatticeSchemaCapabilities> ProbeCapabilities(SchemaTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.ProbeCapabilitiesAsync(req.TreeId, ct));

    /// <inheritdoc />
    public override Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        // Unauthenticated by design (the interceptor exempts this method), so no
        // credential is bridged and only the public advertisement is returned.
        return Task.FromResult(_authSchemeSource.GetAdvertisement());
    }

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeSchemaControl, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(_control, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The schema control-API request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (KeyNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (LatticeQuotaExceededException ex)
        {
            // A remediation or schema-version migration rebuilds the tree into a
            // fresh destination one entry at a time, so it runs under the same
            // per-tree admission caps (LatticeOptions.MaxLiveKeys /
            // MaxEstimatedBytes) as any other write - the system-origin scope the
            // build runs in exempts it from the access gate, not from admission.
            // Reaching a cap is a capacity outcome, not a precondition failure,
            // so it must be mapped ahead of the InvalidOperationException arm
            // below, which would otherwise shadow this type (it derives from it)
            // and report the breach as FailedPrecondition.
            throw ToResourceExhausted(ex);
        }
        catch (InvalidOperationException ex)
        {
            // A precondition failure: schema versioning is not registered, the
            // tree is unversioned, the target version does not advance, or a
            // conflicting remediation is already in flight. The message is safe
            // and actionable (no secrets), so surface it as FailedPrecondition
            // instead of the opaque Internal below.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Schema: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The schema control-API request failed."));
        }
    }

    /// <summary>
    /// Projects an admission refusal onto <see cref="StatusCode.ResourceExhausted"/>,
    /// attaching the breached dimension - and, where the dimension carries one, the
    /// observed value and the configured ceiling - as response trailers so a client
    /// can branch on the outcome without parsing the message. The trailers echo only
    /// fields the self-contained status message already states; never a key and never
    /// a value.
    /// </summary>
    private static RpcException ToResourceExhausted(LatticeQuotaExceededException ex)
    {
        var trailers = new global::Grpc.Core.Metadata
        {
            { QuotaDimensionTrailer, ex.Dimension },
            { QuotaTreeTrailer, ex.TreeId },
        };

        // A dimension with no numeric ceiling reports zero for both fields, so
        // advertising "current 0 of 0" would be worse than saying nothing.
        if (ex.Limit > 0)
        {
            trailers.Add(QuotaCurrentTrailer, ex.Current.ToString(CultureInfo.InvariantCulture));
            trailers.Add(QuotaLimitTrailer, ex.Limit.ToString(CultureInfo.InvariantCulture));
        }

        // The tenant id is deliberately withheld: it is a server-side attribution
        // decision, and the dimension is what the caller needs to act on.
        return new RpcException(
            new Status(StatusCode.ResourceExhausted, ex.Message),
            trailers);
    }
}
