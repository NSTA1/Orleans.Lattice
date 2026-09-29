using Grpc.Net.Client;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The test <see cref="IShellGrpcChannelFactory"/>: builds each channel exactly as
/// the Core factory does - same options, same insecure-channel safeguard - and
/// swaps only the transport handler for the in-memory <see cref="ShellTransportPeer"/>.
/// </summary>
/// <param name="peer">The in-memory peer every channel talks to.</param>
internal sealed class ShellTransportPeerChannelFactory(ShellTransportPeer peer) : IShellGrpcChannelFactory
{
    private readonly List<GrpcChannel> _created = [];

    /// <summary>The channels built so far, oldest first.</summary>
    public IReadOnlyList<GrpcChannel> Created => _created;

    /// <summary>The settings each channel was built from, oldest first.</summary>
    public List<LatticeConnectionSettings> Settings { get; } = [];

    /// <inheritdoc />
    public GrpcChannel CreateChannel(LatticeConnectionSettings settings)
    {
        var options = LatticeGrpcChannelFactory.BuildChannelOptions(settings);
        options.HttpHandler?.Dispose();
        options.HttpHandler = peer;
        options.DisposeHttpClient = false;

        var channel = GrpcChannel.ForAddress(settings.Address, options);
        _created.Add(channel);
        Settings.Add(settings);
        return channel;
    }
}
