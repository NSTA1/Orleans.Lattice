using Grpc.Core;
using Grpc.Net.Client;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Reads an export's pages through the Data API over gRPC, on a channel of its own
/// built from the connection its circuit captured, so the export runs as that user
/// and outlives nothing: the channel is disposed with the export.
/// </summary>
/// <remarks>
/// The channel is the export's own rather than the circuit's, because the download is
/// served on a request outside the circuit and must not depend on, or keep alive, any
/// of the circuit's scoped services. It is built through the same Core plumbing every
/// circuit channel uses, so the transport headers, the plaintext credential gate and
/// the asserted tenant behave identically.
/// </remarks>
internal sealed class GrpcDataExportSource : IDataExportSource
{
    private readonly GrpcChannel? _channel;
    private readonly LatticeDataApiGrpcClient _client;

    /// <summary>Opens a source on a channel of its own, which it disposes.</summary>
    /// <param name="settings">The connection snapshot the circuit captured.</param>
    /// <param name="channels">Builds the channel.</param>
    /// <param name="serializer">The Shell's serializer, for the client's marshallers.</param>
    public GrpcDataExportSource(LatticeConnectionSettings settings, IShellGrpcChannelFactory channels, ShellTransportSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(channels);
        ArgumentNullException.ThrowIfNull(serializer);

        _channel = channels.CreateChannel(settings);
        try
        {
            _client = LatticeDataApiGrpcClient.Create(LatticeGrpcChannelFactory.CreateCallInvoker(_channel, settings), serializer.Services);
        }
        catch
        {
            _channel.Dispose();
            throw;
        }
    }

    /// <summary>Opens a source over an invoker it does not own, such as the circuit's own.</summary>
    /// <param name="invoker">The invoker.</param>
    /// <param name="serializerServices">A provider with Orleans serialization registered.</param>
    public GrpcDataExportSource(CallInvoker invoker, IServiceProvider serializerServices)
    {
        ArgumentNullException.ThrowIfNull(invoker);
        ArgumentNullException.ThrowIfNull(serializerServices);
        _client = LatticeDataApiGrpcClient.Create(invoker, serializerServices);
    }

    /// <inheritdoc />
    public async Task<DataRangePage> ReadAsync(DataRangeRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        try
        {
            return await _client.ReadRangeAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
            when (exception.StatusCode == StatusCode.InvalidArgument && !string.IsNullOrEmpty(request.ContinuationToken))
        {
            // The facade answers a continuation it no longer holds as an invalid argument.
            throw new DataExportCursorExpiredException(exception);
        }
    }

    /// <inheritdoc />
    public void Dispose() => _channel?.Dispose();
}
