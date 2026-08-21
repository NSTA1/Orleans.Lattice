using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Data;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Unit coverage for the strongly-typed <see cref="LatticeDataApiGrpcClient"/>.
/// Drives every RPC wrapper over a deterministic in-memory
/// <see cref="CallInvoker"/> - no wire, no server - asserting that each method
/// forwards the caller's request onto its own gRPC method and returns the invoker's
/// response, that a null request is rejected before any call, and that a cancelled
/// token surfaces as a faulted call. Also covers the factory / constructor guards.
/// </summary>
[TestFixture]
public sealed class LatticeDataApiGrpcClientTests
{
    private static IServiceProvider SerializerProvider() =>
        new ServiceCollection().AddSerializer().BuildServiceProvider();

    private static LatticeDataApiGrpcClient CreateClient(CallInvoker invoker) =>
        LatticeDataApiGrpcClient.Create(invoker, SerializerProvider());

    [Test]
    public void Create_throws_on_null_call_invoker()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeDataApiGrpcClient.Create(null!, SerializerProvider()));
    }

    [Test]
    public void Create_throws_on_null_serializer_provider()
    {
        var invoker = new RecordingCallInvoker(_ => throw new InvalidOperationException("unused"));

        Assert.Throws<ArgumentNullException>(
            () => LatticeDataApiGrpcClient.Create(invoker, null!));
    }

    [Test]
    public async Task SetAsync_forwards_the_request_and_returns_the_response()
    {
        var response = new DataSetResponse();
        var invoker = new RecordingCallInvoker(_ => response);
        var client = CreateClient(invoker);
        var request = new DataSetRequest { TreeId = "t", Key = "k", Value = [1] };

        var result = await client.SetAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(response));
            Assert.That(invoker.LastRequest, Is.SameAs(request));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.SetMethodName));
        });
    }

    [Test]
    public async Task DeleteAsync_forwards_the_request_and_returns_the_response()
    {
        var response = new DataDeleteResponse { Removed = true };
        var invoker = new RecordingCallInvoker(_ => response);
        var client = CreateClient(invoker);

        var result = await client.DeleteAsync(new DataDeleteRequest { TreeId = "t", Key = "k" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Removed, Is.True);
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.DeleteMethodName));
        });
    }

    [Test]
    public async Task SetManyAtomicAsync_targets_the_atomic_method()
    {
        var invoker = new RecordingCallInvoker(_ => new DataAtomicResponse());
        var client = CreateClient(invoker);

        await client.SetManyAtomicAsync(new DataAtomicRequest { TreeId = "t", OperationId = "op" });

        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.SetManyAtomicMethodName));
    }

    [Test]
    public async Task SetManyAtomicCrossTreeAsync_targets_the_cross_tree_method()
    {
        var invoker = new RecordingCallInvoker(_ => new DataCrossTreeResponse());
        var client = CreateClient(invoker);

        await client.SetManyAtomicCrossTreeAsync(new DataCrossTreeRequest { OperationId = "xt" });

        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.SetManyAtomicCrossTreeMethodName));
    }

    [Test]
    public async Task GetAsync_targets_the_get_method_and_returns_the_result()
    {
        var response = new DataReadResult { TreeId = "t", Key = "k", Found = true, Value = [7] };
        var invoker = new RecordingCallInvoker(_ => response);
        var client = CreateClient(invoker);

        var result = await client.GetAsync(new DataGetRequest { TreeId = "t", Key = "k" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Found, Is.True);
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.GetMethodName));
        });
    }

    [Test]
    public async Task ReadRangeAsync_targets_the_read_range_method()
    {
        var invoker = new RecordingCallInvoker(_ => new DataRangePage { TreeId = "t" });
        var client = CreateClient(invoker);

        await client.ReadRangeAsync(new DataRangeRequest { TreeId = "t", PageSize = 10 });

        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.ReadRangeMethodName));
    }

    [Test]
    public async Task DeleteRangeAsync_targets_the_delete_range_method()
    {
        var invoker = new RecordingCallInvoker(_ => new DataRangeDeleteResult { TreeId = "t", DeletedCount = 0 });
        var client = CreateClient(invoker);

        await client.DeleteRangeAsync(new DataRangeDeleteRequest
        {
            TreeId = "t",
            StartInclusive = "a",
            EndExclusive = "z",
        });

        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.DeleteRangeMethodName));
    }

    [Test]
    public async Task SetManyAsync_targets_the_set_many_method()
    {
        var invoker = new RecordingCallInvoker(_ => new DataSetManyResponse());
        var client = CreateClient(invoker);

        await client.SetManyAsync(new DataSetManyRequest { TreeId = "t" });

        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.SetManyMethodName));
    }

    [Test]
    public async Task CrdtWriteAsync_targets_the_crdt_write_method()
    {
        var invoker = new RecordingCallInvoker(_ => new CrdtWriteResponse());
        var client = CreateClient(invoker);

        await client.CrdtWriteAsync(new CrdtWriteRequest
        {
            TreeId = "t",
            Key = "k",
            Op = CrdtWriteOp.CounterIncrement,
        });

        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.CrdtWriteMethodName));
    }

    [Test]
    public async Task CrdtReadAsync_targets_the_crdt_read_method()
    {
        var invoker = new RecordingCallInvoker(_ => new CrdtReadResponse());
        var client = CreateClient(invoker);

        await client.CrdtReadAsync(new CrdtReadRequest { TreeId = "t", Key = "k", Kind = CrdtKind.PnCounter });

        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeDataApiGrpcMethods.CrdtReadMethodName));
    }

    [Test]
    public void SetAsync_throws_on_null_request()
    {
        var invoker = new RecordingCallInvoker(_ => new DataSetResponse());
        var client = CreateClient(invoker);

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => client.SetAsync(null!));
            Assert.That(invoker.CallCount, Is.EqualTo(0), "a null request must be rejected before any call");
        });
    }

    [Test]
    public void GetAsync_propagates_a_cancelled_token_as_a_faulted_call()
    {
        var invoker = new RecordingCallInvoker(_ => new DataReadResult { TreeId = "t", Key = "k" });
        var client = CreateClient(invoker);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            () => client.GetAsync(new DataGetRequest { TreeId = "t", Key = "k" }, cts.Token));
    }

    /// <summary>
    /// Deterministic <see cref="CallInvoker"/> double: a unary call runs a supplied
    /// responder against the request and returns the produced response (faulting the
    /// call when the responder returns or throws an exception), a cancelled token
    /// faults the call, and every call's request and method name are captured. No
    /// network, no threads.
    /// </summary>
    private sealed class RecordingCallInvoker(Func<object, object> responder) : CallInvoker
    {
        public object? LastRequest { get; private set; }

        public string? LastMethodName { get; private set; }

        public int CallCount { get; private set; }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
        {
            CallCount++;
            LastRequest = request;
            LastMethodName = method.Name;

            Task<TResponse> responseAsync;
            if (options.CancellationToken.IsCancellationRequested)
            {
                responseAsync = Task.FromCanceled<TResponse>(options.CancellationToken);
            }
            else
            {
                var produced = responder(request!);
                responseAsync = produced is Exception ex
                    ? Task.FromException<TResponse>(ex)
                    : Task.FromResult((TResponse)produced);
            }

            return new AsyncUnaryCall<TResponse>(
                responseAsync,
                Task.FromResult(new global::Grpc.Core.Metadata()),
                static () => Status.DefaultSuccess,
                static () => new global::Grpc.Core.Metadata(),
                static () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
            => throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
            => throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
            => throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
            => throw new NotSupportedException();
    }
}
