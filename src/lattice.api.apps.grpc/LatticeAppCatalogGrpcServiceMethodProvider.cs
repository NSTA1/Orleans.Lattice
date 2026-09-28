using Grpc.AspNetCore.Server.Model;

namespace Orleans.Lattice.Api.Apps.Grpc;

// DI-owned binding avoids a process-static serializer holder shared by independent hosts.
internal sealed class LatticeAppCatalogGrpcServiceMethodProvider(LatticeAppCatalogGrpcMethods methods)
    : IServiceMethodProvider<LatticeAppCatalogGrpcService>
{
    public void OnServiceMethodDiscovery(ServiceMethodProviderContext<LatticeAppCatalogGrpcService> context)
    {
        context.AddUnaryMethod(methods.ListSources, [], static (s, r, c) => s.ListSources(r, c));
        context.AddUnaryMethod(methods.ListAvailable, [], static (s, r, c) => s.ListAvailable(r, c));
        context.AddUnaryMethod(methods.DescribeFromSource, [], static (s, r, c) => s.DescribeFromSource(r, c));
        context.AddUnaryMethod(methods.GetIcon, [], static (s, r, c) => s.GetIcon(r, c));
        context.AddUnaryMethod(methods.GetCapabilities, [], static (s, r, c) => s.GetCapabilities(r, c));
    }
}
