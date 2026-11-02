using System.Net.Http;
using System.Runtime.ExceptionServices;
using Grpc.Core;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Drives one streamed bulk load over the duplex <c>StreamBulkLoad</c> RPC for
/// <see cref="LatticeTreeAdminApiGrpcClient.BulkLoadStreamAsync"/>: it cuts the entry
/// stream into chunks, keeps at most
/// <see cref="TreeBulkLoadStreamOptions.MaxUnacknowledgedChunks"/> of them in flight, and
/// when a transient transport fault breaks the call it reopens the stream as a resume
/// and re-sends every chunk not yet acknowledged, so no entry is lost or written twice.
/// </summary>
/// <remarks>
/// It holds the chunk being filled and the unacknowledged chunks, never the input: memory
/// is bounded by the options, whatever the length of the stream.
/// </remarks>
internal sealed class TreeAdminBulkLoadStreamCall
{
    private readonly string _treeId;
    private readonly string _operationId;
    private readonly TreeBulkLoadStreamOptions _options;
    private readonly IProgress<TreeBulkLoadChunkAck>? _progress;
    private readonly Queue<(long Index, IReadOnlyList<DataEntry> Entries)> _unacknowledged = new();
    private readonly Lock _gate = new();
    private long _nextChunkIndex;
    private string? _previousKey;
    private bool _sourceDrained;
    private volatile bool _opened;

    /// <summary>Prepares the load.</summary>
    /// <param name="treeId">The tree to load.</param>
    /// <param name="operationId">The session's idempotency key.</param>
    /// <param name="options">The validated options.</param>
    /// <param name="progress">Receives each chunk's acknowledgement.</param>
    public TreeAdminBulkLoadStreamCall(
        string treeId,
        string operationId,
        TreeBulkLoadStreamOptions options,
        IProgress<TreeBulkLoadChunkAck>? progress)
    {
        _treeId = treeId;
        _operationId = operationId;
        _options = options;
        _progress = progress;
        _nextChunkIndex = options.FirstChunkIndex;
        _opened = options.FirstChunkIndex > 0;
    }

    /// <summary>Whether a fault on the call is one a resumed stream may recover from.</summary>
    /// <param name="exception">The fault.</param>
    internal static bool IsResumable(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unavailable or StatusCode.Aborted or StatusCode.Unknown => true,
        StatusCode.Internal => exception.Status.DebugException is IOException or HttpRequestException,
        _ => false,
    };

    /// <summary>Runs the load to its result, resuming a broken stream up to the configured limit.</summary>
    /// <param name="invoker">The call invoker.</param>
    /// <param name="method">The <c>StreamBulkLoad</c> method.</param>
    /// <param name="entries">The entries, in strictly ascending key order.</param>
    /// <param name="cancellationToken">Stops the load.</param>
    /// <returns>The committed load's result.</returns>
    public async Task<TreeBulkLoadResult> RunAsync(
        CallInvoker invoker,
        Method<TreeAdminBulkLoadStreamRequest, TreeAdminBulkLoadStreamResponse> method,
        IAsyncEnumerable<DataEntry> entries,
        CancellationToken cancellationToken)
    {
        var source = entries.GetAsyncEnumerator(cancellationToken);
        await using (source.ConfigureAwait(false))
        {
            var resumes = 0;
            while (true)
            {
                try
                {
                    return await RunAttemptAsync(invoker, method, source, cancellationToken).ConfigureAwait(false);
                }
                catch (RpcException exception) when (IsResumable(exception)
                    && resumes < _options.MaxResumeAttempts
                    && !cancellationToken.IsCancellationRequested)
                {
                    resumes++;
                }
            }
        }
    }

    private async Task<TreeBulkLoadResult> RunAttemptAsync(
        CallInvoker invoker,
        Method<TreeAdminBulkLoadStreamRequest, TreeAdminBulkLoadStreamResponse> method,
        IAsyncEnumerator<DataEntry> source,
        CancellationToken cancellationToken)
    {
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var call = invoker.AsyncDuplexStreamingCall(method, host: null, new CallOptions(cancellationToken: attempt.Token));
        using var slots = new SemaphoreSlim(_options.MaxUnacknowledgedChunks);
        var receive = ReceiveAsync(call.ResponseStream, slots, attempt);
        var sourceFaulted = false;
        try
        {
            await call.RequestStream.WriteAsync(new TreeAdminBulkLoadStreamRequest
            {
                TreeId = _treeId,
                OperationId = _operationId,
                Resume = _opened,
            }).ConfigureAwait(false);

            (long Index, IReadOnlyList<DataEntry> Entries)[] resend;
            lock (_gate)
            {
                resend = [.. _unacknowledged];
            }

            foreach (var (index, chunk) in resend)
            {
                await SendAsync(call.RequestStream, slots, index, chunk, attempt.Token).ConfigureAwait(false);
            }

            while (!_sourceDrained)
            {
                List<DataEntry> chunk;
                try
                {
                    chunk = await FillAsync(source, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    sourceFaulted = true;
                    throw;
                }

                if (chunk.Count == 0)
                {
                    break;
                }

                var index = _nextChunkIndex++;
                lock (_gate)
                {
                    // Recorded before it is sent, so a fault between the two re-sends it.
                    _unacknowledged.Enqueue((index, chunk));
                }

                await SendAsync(call.RequestStream, slots, index, chunk, attempt.Token).ConfigureAwait(false);
            }

            await call.RequestStream.CompleteAsync().ConfigureAwait(false);
            return await receive.ConfigureAwait(false);
        }
        catch (Exception sendFault)
        {
            await attempt.CancelAsync().ConfigureAwait(false);
            Exception? receiveFault = null;
            try
            {
                await receive.ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                receiveFault = exception;
            }

            // A fault of the input, or the caller's own cancellation, stands as it is. Any
            // other fault means the call broke, and the sender may only have noticed it (a
            // failed write, or a wait the abandoned attempt woke): the server's own status
            // says why when it sent one, and it decides whether the stream resumes.
            if (sourceFaulted || cancellationToken.IsCancellationRequested)
            {
                throw;
            }

            if (receiveFault is RpcException { StatusCode: not StatusCode.Cancelled } callFault)
            {
                ExceptionDispatchInfo.Throw(callFault);
            }

            if (sendFault is RpcException)
            {
                throw;
            }

            throw new RpcException(
                new Status(StatusCode.Unavailable, "The bulk-load stream broke before the server answered.", sendFault));
        }
    }

    private async Task SendAsync(
        IClientStreamWriter<TreeAdminBulkLoadStreamRequest> writer,
        SemaphoreSlim slots,
        long index,
        IReadOnlyList<DataEntry> chunk,
        CancellationToken cancellationToken)
    {
        await slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        await writer.WriteAsync(new TreeAdminBulkLoadStreamRequest
        {
            TreeId = _treeId,
            OperationId = _operationId,
            ChunkIndex = index,
            Entries = chunk,
        }).ConfigureAwait(false);
    }

    private async Task<List<DataEntry>> FillAsync(IAsyncEnumerator<DataEntry> source, CancellationToken cancellationToken)
    {
        var chunk = new List<DataEntry>(_options.ChunkSize);
        while (chunk.Count < _options.ChunkSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!await source.MoveNextAsync().ConfigureAwait(false))
            {
                _sourceDrained = true;
                break;
            }

            var entry = source.Current ?? throw new ArgumentException("A bulk-load stream entry is null.");
            if (_previousKey is not null && string.CompareOrdinal(entry.Key, _previousKey) <= 0)
            {
                throw new BulkLoadOrderException(_treeId, _nextChunkIndex, entry.Key, _previousKey);
            }

            _previousKey = entry.Key;
            chunk.Add(entry);
        }

        return chunk;
    }

    private async Task<TreeBulkLoadResult> ReceiveAsync(
        IAsyncStreamReader<TreeAdminBulkLoadStreamResponse> reader,
        SemaphoreSlim slots,
        CancellationTokenSource attempt)
    {
        try
        {
            while (await reader.MoveNext(attempt.Token).ConfigureAwait(false))
            {
                var frame = reader.Current;
                if (frame.Opened)
                {
                    _opened = true;
                }

                if (frame.Ack is { } ack)
                {
                    lock (_gate)
                    {
                        while (_unacknowledged.Count > 0 && _unacknowledged.Peek().Index <= ack.ChunkIndex)
                        {
                            _unacknowledged.Dequeue();
                        }
                    }

                    _progress?.Report(ack);
                    slots.Release();
                }

                if (frame.Result is { } result)
                {
                    return result;
                }
            }

            throw new RpcException(new Status(
                StatusCode.Unavailable,
                "The bulk-load stream ended before the server reported the load's result."));
        }
        catch (Exception)
        {
            // Wake a sender waiting for a slot, so the attempt ends with this fault.
            await attempt.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }
}
