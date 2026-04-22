using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MultiSiteManufacturing.Host.Federation;
using V1 = MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Host.Grpc;

/// <summary>
/// gRPC surface for site chaos controls. Delegates to
/// <see cref="FederationRouter"/> which in turn coordinates with the
/// per-site and registry grains.
/// </summary>
/// <remarks>
/// The <c>WatchSites</c> stream currently emits a single snapshot then
/// awaits cancellation. Live push is wired in M9 together with the chaos
/// fly-out component.
/// </remarks>
public sealed class SiteControlServiceImpl(FederationRouter router)
    : V1.SiteControlService.SiteControlServiceBase
{
    /// <inheritdoc />
    public override async Task<V1.ListSitesResponse> ListSites(Empty request, ServerCallContext context)
    {
        var sites = await router.ListSitesAsync();
        var response = new V1.ListSitesResponse();
        foreach (var state in sites)
        {
            response.Sites.Add(ProtoMappings.ToProto(state));
        }
        return response;
    }

    /// <inheritdoc />
    public override async Task<V1.SiteState> ConfigureSite(V1.ConfigureSiteRequest request, ServerCallContext context)
    {
        var site = ProtoMappings.FromProto(request.Site);
        var config = ProtoMappings.FromProto(request.Config);
        var state = await router.ConfigureSiteAsync(site, config, context.CancellationToken);
        return ProtoMappings.ToProto(state);
    }

    /// <inheritdoc />
    public override async Task WatchSites(
        Empty request,
        IServerStreamWriter<V1.SiteState> responseStream,
        ServerCallContext context)
    {
        var snapshot = await router.ListSitesAsync();
        foreach (var state in snapshot)
        {
            await responseStream.WriteAsync(ProtoMappings.ToProto(state), context.CancellationToken);
        }

        // Live push lands in M9. Until then, hold the stream open until
        // the client cancels so callers can treat it as a long-lived feed.
        try
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <inheritdoc />
    public override async Task<V1.PresetResult> TriggerPreset(V1.TriggerPresetRequest request, ServerCallContext context)
    {
        var preset = ProtoMappings.FromProto(request.Preset);
        var sites = await router.ApplyPresetAsync(preset, context.CancellationToken);
        var result = new V1.PresetResult();
        foreach (var state in sites)
        {
            result.Sites.Add(ProtoMappings.ToProto(state));
        }
        return result;
    }

    /// <inheritdoc />
    public override async Task<V1.ListBackendsResponse> ListBackends(Empty request, ServerCallContext context)
    {
        var backends = await router.ListBackendChaosAsync();
        var response = new V1.ListBackendsResponse();
        foreach (var state in backends)
        {
            response.Backends.Add(ProtoMappings.ToProto(state));
        }
        return response;
    }

    /// <inheritdoc />
    public override async Task<V1.BackendChaosState> ConfigureBackend(
        V1.ConfigureBackendRequest request,
        ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Backend name is required."));
        }

        try
        {
            var state = await router.ConfigureBackendChaosAsync(
                request.Name,
                ProtoMappings.FromProto(request.Config));
            return ProtoMappings.ToProto(state);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }
}
