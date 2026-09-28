using Grpc.AspNetCore.Server.Model;

namespace Orleans.Lattice.Api.Apps.Grpc;

// DI-owned binding avoids a process-static serializer holder shared by independent hosts.
internal sealed class LatticeAppBridgeGrpcServiceMethodProvider(LatticeAppBridgeGrpcMethods methods)
    : IServiceMethodProvider<LatticeAppBridgeGrpcService>
{
    public void OnServiceMethodDiscovery(ServiceMethodProviderContext<LatticeAppBridgeGrpcService> context)
    {
        context.AddUnaryMethod(methods.Get, [], static (s, r, c) => s.Get(r, c));
        context.AddUnaryMethod(methods.Scan, [], static (s, r, c) => s.Scan(r, c));
        context.AddUnaryMethod(methods.Set, [], static (s, r, c) => s.Set(r, c));
        context.AddUnaryMethod(methods.Delete, [], static (s, r, c) => s.Delete(r, c));
    }
}
