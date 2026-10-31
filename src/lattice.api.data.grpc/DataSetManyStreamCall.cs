using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Drives one streamed write over the duplex <c>StreamSetMany</c> RPC for
/// <see cref="LatticeDataApiGrpcClient.SetManyStreamAsync"/>: it cuts the entries into
/// batches, keeps at most <see cref="MaxBatchesInFlight"/> of them unanswered, and reports
/// each batch's outcome as the server answers it. It holds the batch being filled and the
/// unanswered ones, never the input.
/// </summary>
internal static class DataSetManyStreamCall
{
    /// <summary>How many batches may be sent and not yet answered before the sender waits.</summary>
    internal const int MaxBatchesInFlight = 4;

    /// <summary>Runs the stream to its totals.</summary>
    /// <param name="invoker">The call invoker.</param>
    /// <param name="method">The <c>StreamSetMany</c> method.</param>
    /// <param name="treeId">The tree to write.</param>
    /// <param name="upserts">The entries.</param>
    /// <param name="progress">Receives each batch's outcome.</param>
    /// <param name="options">The validated options.</param>
    /// <param name="cancellationToken">Abandons the stream.</param>
    /// <returns>The stream's totals.</returns>
    public static async Task<DataSetManyStreamResult> RunAsync(
        CallInvoker invoker,
        Method<DataSetManyStreamRequest, DataSetManyStreamResponse> method,
        string treeId,
        IAsyncEnumerable<DataEntry> upserts,
        IProgress<DataSetManyStreamBatch>? progress,
        DataSetManyStreamOptions options,
        CancellationToken cancellationToken)
    {
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var call = invoker.AsyncDuplexStreamingCall(method, host: null, new CallOptions(cancellationToken: attempt.Token));
        using var slots = new SemaphoreSlim(MaxBatchesInFlight);
        var receive = ReceiveAsync(call.ResponseStream, slots, progress, options.FirstOrdinal, attempt);
        var sourceFaulted = false;
        try
        {
            var batch = new List<DataEntry>(options.BatchSize);
            var source = upserts.GetAsyncEnumerator(cancellationToken);
            await using (source.ConfigureAwait(false))
            {
                while (true)
                {
                    bool moved;
                    try
                    {
                        moved = await source.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        sourceFaulted = true;
                        throw;
                    }

                    if (moved)
                    {
                        batch.Add(source.Current ?? throw new ArgumentException("A set-many stream entry is null."));
                        if (batch.Count < options.BatchSize)
                        {
                            continue;
                        }
                    }

                    if (batch.Count > 0)
                    {
                        await slots.WaitAsync(attempt.Token).ConfigureAwait(false);
                        await call.RequestStream.WriteAsync(new DataSetManyStreamRequest { TreeId = treeId, Upserts = batch }).ConfigureAwait(false);
                        batch = new List<DataEntry>(options.BatchSize);
                    }

                    if (!moved)
                    {
                        break;
                    }
                }
            }

            await call.RequestStream.CompleteAsync().ConfigureAwait(false);
            return await receive.ConfigureAwait(false);
        }
        catch (Exception)
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

            // The server's own status says why a call it ended broke; a fault of the input or
            // the caller's cancellation stands as it is.
            if (!sourceFaulted && !cancellationToken.IsCancellationRequested
                && receiveFault is RpcException { StatusCode: not StatusCode.Cancelled } callFault)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(callFault);
            }

            throw;
        }
    }

    private static async Task<DataSetManyStreamResult> ReceiveAsync(
        IAsyncStreamReader<DataSetManyStreamResponse> reader,
        SemaphoreSlim slots,
        IProgress<DataSetManyStreamBatch>? progress,
        long firstOrdinal,
        CancellationTokenSource attempt)
    {
        try
        {
            while (await reader.MoveNext(attempt.Token).ConfigureAwait(false))
            {
                var frame = reader.Current;
                if (frame.Batch is { } batch)
                {
                    progress?.Report(Offset(batch, firstOrdinal));
                    slots.Release();
                }

                if (frame.Result is { } result)
                {
                    return result;
                }
            }

            throw new RpcException(new Status(StatusCode.Unavailable, "The set-many stream ended before the server reported its totals."));
        }
        catch (Exception)
        {
            await attempt.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Numbers a batch's ordinals from the caller's first ordinal rather than the stream's zero.</summary>
    /// <param name="batch">The batch as the server numbered it.</param>
    /// <param name="firstOrdinal">The caller's first ordinal.</param>
    internal static DataSetManyStreamBatch Offset(DataSetManyStreamBatch batch, long firstOrdinal) => firstOrdinal == 0
        ? batch
        : batch with
        {
            FirstOrdinal = batch.FirstOrdinal + firstOrdinal,
            Rejections = batch.Rejections.Select(rejection => rejection with { Ordinal = rejection.Ordinal + firstOrdinal }).ToArray(),
        };
}
