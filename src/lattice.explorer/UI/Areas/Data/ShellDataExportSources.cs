using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The production <see cref="IDataExportSources"/>: the circuit's own channel for the
/// probe, and, for a download, a fresh channel built from the captured connection by the
/// stateless singleton channel factory.
/// </summary>
/// <param name="channel">The circuit's channel.</param>
/// <param name="channels">The singleton channel factory.</param>
/// <param name="serializer">The singleton serializer.</param>
internal sealed class ShellDataExportSources(
    ShellTransportChannel channel,
    IShellGrpcChannelFactory channels,
    ShellTransportSerializer serializer) : IDataExportSources
{
    /// <inheritdoc />
    public IDataExportSource OpenForCircuit() => new GrpcDataExportSource(channel.Invoker, channel.SerializerServices);

    /// <inheritdoc />
    public Func<IDataExportSource> ForDownload(LatticeConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        // Only the singletons and the settings are captured: never the circuit's channel.
        var factory = channels;
        var codecs = serializer;
        return () => new GrpcDataExportSource(settings, factory, codecs);
    }
}
