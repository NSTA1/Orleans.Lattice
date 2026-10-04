using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Lattice.Explorer.Core.Connection;
using GrpcMetadata = Grpc.Core.Metadata;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// The sign-in on the wire: every call shape the Explorer's channels actually
/// use composes the circuit's <see cref="CallCredentials"/> onto its options,
/// forwards everything else about the call untouched, and leaves the credential
/// to be resolved by gRPC at call time rather than captured once.
/// </summary>
/// <remarks>
/// Each override is a one-line delegation, so the only thing it can get wrong is
/// which call it forwards to and what it forwards. A shared recording invoker at
/// the bottom of the pipeline is therefore driven once per shape and the
/// recorded method is what tells the three apart.
/// </remarks>
[TestFixture]
public sealed class CallCredentialsInterceptorTests
{
    private static readonly Method<string, string> Unary = Make(MethodType.Unary);
    private static readonly Method<string, string> ServerStreaming = Make(MethodType.ServerStreaming);

    [Test]
    public void The_constructor_rejects_a_missing_credential() =>
        Assert.That(() => new CallCredentialsInterceptor(null!), Throws.ArgumentNullException);

    [Test]
    public void A_blocking_unary_call_carries_the_credential()
    {
        var (invoker, recorded, credentials) = Pipeline();

        invoker.BlockingUnaryCall(Unary, null, default, "x");

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Options.Single().Credentials, Is.SameAs(credentials));
            Assert.That(recorded.Methods.Single(), Is.SameAs(Unary), "the blocking arm must forward its own method");
        });
    }

    [Test]
    public void An_async_unary_call_carries_the_credential()
    {
        var (invoker, recorded, credentials) = Pipeline();

        invoker.AsyncUnaryCall(Unary, null, default, "x");

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Options.Single().Credentials, Is.SameAs(credentials));
            Assert.That(recorded.Methods.Single(), Is.SameAs(Unary));
        });
    }

    [Test]
    public void A_server_streaming_call_carries_the_credential()
    {
        var (invoker, recorded, credentials) = Pipeline();

        invoker.AsyncServerStreamingCall(ServerStreaming, null, default, "x");

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Options.Single().Credentials, Is.SameAs(credentials));
            Assert.That(
                recorded.Methods.Single(),
                Is.SameAs(ServerStreaming),
                "the streaming arm must forward the streaming method, not a unary one");
        });
    }

    [Test]
    public void Each_shape_forwards_its_own_method_so_no_two_arms_are_interchangeable()
    {
        // Driving all three through one pipeline is what would catch two arms
        // wired to the same continuation: asserting each in isolation cannot.
        var (invoker, recorded, credentials) = Pipeline();

        invoker.BlockingUnaryCall(Unary, null, default, "blocking");
        invoker.AsyncUnaryCall(Unary, null, default, "async");
        invoker.AsyncServerStreamingCall(ServerStreaming, null, default, "streaming");

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Requests, Is.EqualTo(new object?[] { "blocking", "async", "streaming" }));
            Assert.That(recorded.Methods.Select(method => method.Type), Is.EqualTo(new[] { MethodType.Unary, MethodType.Unary, MethodType.ServerStreaming }));
            Assert.That(recorded.Options.Select(options => options.Credentials), Is.EqualTo(Enumerable.Repeat(credentials, 3)));
        });
    }

    [Test]
    public void The_rest_of_the_call_options_survive_the_composition()
    {
        var (invoker, recorded, credentials) = Pipeline();
        var headers = new GrpcMetadata { { "x-azure-fdid", "origin" } };
        var deadline = DateTime.UtcNow.AddMinutes(5);
        using var cancellation = new CancellationTokenSource();

        invoker.AsyncUnaryCall(
            Unary,
            "other-host",
            new CallOptions(headers, deadline, cancellation.Token),
            "x");

        var sent = recorded.Options.Single();
        Assert.Multiple(() =>
        {
            Assert.That(sent.Credentials, Is.SameAs(credentials));
            Assert.That(sent.Headers, Is.SameAs(headers), "attaching a credential must not drop the transport headers");
            Assert.That(sent.Deadline, Is.EqualTo(deadline));
            Assert.That(sent.CancellationToken, Is.EqualTo(cancellation.Token));
            Assert.That(recorded.Hosts.Single(), Is.EqualTo("other-host"), "the host the caller named must survive");
        });
    }

    [Test]
    public void A_credential_already_on_the_call_is_replaced_by_the_circuit_one()
    {
        var (invoker, recorded, credentials) = Pipeline();
        var stale = CallCredentials.FromInterceptor((_, _) => Task.CompletedTask);

        invoker.AsyncUnaryCall(Unary, null, new CallOptions().WithCredentials(stale), "x");

        Assert.That(recorded.Options.Single().Credentials, Is.SameAs(credentials));
    }

    [Test]
    public void The_credential_is_handed_down_unresolved_so_grpc_resolves_it_at_call_time()
    {
        // A bearer provider refreshes a near-expiry token while gRPC resolves the
        // credential as the call starts, so the interceptor must hand the provider
        // itself down the pipeline and never a header it read once and cached.
        var resolutions = 0;
        var credentials = CallCredentials.FromInterceptor((_, metadata) =>
        {
            resolutions++;
            metadata.Add("authorization", "Bearer token");
            return Task.CompletedTask;
        });
        var recorded = new RecordingCallInvoker();
        var invoker = recorded.Intercept(new CallCredentialsInterceptor(credentials));

        invoker.AsyncUnaryCall(Unary, null, default, "x");
        invoker.AsyncUnaryCall(Unary, null, default, "x");

        Assert.Multiple(() =>
        {
            Assert.That(resolutions, Is.Zero, "the interceptor must not resolve the credential itself");
            Assert.That(recorded.Headers, Is.All.Null, "no authorization header is written here");
            Assert.That(recorded.Options.Select(options => options.Credentials), Is.All.SameAs(credentials));
        });
    }

    private static (CallInvoker Invoker, RecordingCallInvoker Recorded, CallCredentials Credentials) Pipeline()
    {
        var recorded = new RecordingCallInvoker();
        var credentials = CallCredentials.FromInterceptor((_, _) => Task.CompletedTask);
        return (recorded.Intercept(new CallCredentialsInterceptor(credentials)), recorded, credentials);
    }

    private static Method<string, string> Make(MethodType type) =>
        new(type, "svc", type.ToString(), Marshallers.StringMarshaller, Marshallers.StringMarshaller);
}
