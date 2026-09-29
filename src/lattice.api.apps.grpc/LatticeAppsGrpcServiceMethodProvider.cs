using Grpc.AspNetCore.Server.Model;

namespace Orleans.Lattice.Api.Apps.Grpc;

// DI-owned binding avoids a process-static serializer holder shared by independent hosts.
internal sealed class LatticeAppsGrpcServiceMethodProvider(LatticeAppsGrpcMethods methods)
    : IServiceMethodProvider<LatticeAppsGrpcService>
{
    public void OnServiceMethodDiscovery(ServiceMethodProviderContext<LatticeAppsGrpcService> context)
    {
        context.AddUnaryMethod(methods.Install, [], static (s, r, c) => s.Install(r, c));
        context.AddUnaryMethod(methods.Enable, [], static (s, r, c) => s.Enable(r, c));
        context.AddUnaryMethod(methods.Disable, [], static (s, r, c) => s.Disable(r, c));
        context.AddUnaryMethod(methods.Uninstall, [], static (s, r, c) => s.Uninstall(r, c));
        context.AddUnaryMethod(methods.List, [], static (s, r, c) => s.List(r, c));
        context.AddUnaryMethod(methods.Describe, [], static (s, r, c) => s.Describe(r, c));
        context.AddUnaryMethod(methods.GetConsent, [], static (s, r, c) => s.GetConsent(r, c));
        context.AddUnaryMethod(methods.UpdateConsent, [], static (s, r, c) => s.UpdateConsent(r, c));
        context.AddUnaryMethod(methods.GetCapabilities, [], static (s, r, c) => s.GetCapabilities(r, c));
        context.AddUnaryMethod(methods.GetAuthScheme, [], static (s, r, c) => s.GetAuthScheme(r, c));
        context.AddUnaryMethod(methods.UpdateRoleBindings, [], static (s, r, c) => s.UpdateRoleBindings(r, c));
    }
}
