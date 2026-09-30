using Grpc.Net.Client;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// Builds the gRPC channel a circuit's <see cref="ShellTransportChannel"/> rides.
/// The production implementation defers to the Core
/// <see cref="LatticeGrpcChannelFactory"/>, so the transport handler and the
/// insecure-channel safeguard are decided in exactly one place. The seam exists
/// so a test can substitute the network hop while keeping every other part of
/// the Core connection plumbing, credential attachment included, unchanged.
/// </summary>
internal interface IShellGrpcChannelFactory
{
    /// <summary>Builds a channel for <paramref name="settings"/>. The caller owns and disposes it.</summary>
    /// <param name="settings">The endpoint settings, with the circuit's sign-in already applied.</param>
    /// <returns>The channel.</returns>
    GrpcChannel CreateChannel(LatticeConnectionSettings settings);
}
