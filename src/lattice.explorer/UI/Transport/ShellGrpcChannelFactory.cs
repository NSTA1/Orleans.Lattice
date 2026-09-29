using Grpc.Net.Client;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The production <see cref="IShellGrpcChannelFactory"/>: a stateless pass-through
/// to <see cref="LatticeGrpcChannelFactory.CreateChannel"/>. It holds no state, so
/// it is safe to register as a singleton; everything per-circuit lives in
/// <see cref="ShellTransportChannel"/>.
/// </summary>
internal sealed class ShellGrpcChannelFactory : IShellGrpcChannelFactory
{
    /// <inheritdoc />
    public GrpcChannel CreateChannel(LatticeConnectionSettings settings) =>
        LatticeGrpcChannelFactory.CreateChannel(settings);
}
