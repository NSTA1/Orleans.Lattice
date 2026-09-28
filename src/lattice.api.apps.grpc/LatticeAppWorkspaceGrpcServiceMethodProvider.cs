using Grpc.AspNetCore.Server.Model;

namespace Orleans.Lattice.Api.Apps.Grpc;

// DI-owned binding avoids a process-static serializer holder shared by independent hosts.
internal sealed class LatticeAppWorkspaceGrpcServiceMethodProvider(LatticeAppWorkspaceGrpcMethods methods)
    : IServiceMethodProvider<LatticeAppWorkspaceGrpcService>
{
    public void OnServiceMethodDiscovery(ServiceMethodProviderContext<LatticeAppWorkspaceGrpcService> context)
    {
        context.AddUnaryMethod(methods.ListMyApps, [], static (s, r, c) => s.ListMyApps(r, c));
        context.AddUnaryMethod(methods.DescribeMyApp, [], static (s, r, c) => s.DescribeMyApp(r, c));
        context.AddUnaryMethod(methods.GetIcon, [], static (s, r, c) => s.GetIcon(r, c));
        context.AddUnaryMethod(methods.GetUiAsset, [], static (s, r, c) => s.GetUiAsset(r, c));
    }
}