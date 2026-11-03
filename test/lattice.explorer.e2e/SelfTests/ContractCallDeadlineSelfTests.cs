using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// A contract's facade call is bounded (#5736): with no deadline of its own, each attempt
/// carries a 120-second deadline, so a call that never
/// returns fails instead of hanging the test host until its blame timeout.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class ContractCallDeadlineSelfTests
{
    /// <summary>The per-attempt deadline the interceptor applies.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(120);

    private static readonly Marshaller<string> Text = Marshallers.Create(
        value => System.Text.Encoding.UTF8.GetBytes(value),
        bytes => System.Text.Encoding.UTF8.GetString(bytes));

    /// <summary>A read and a mutation without a deadline each get one.</summary>
    [TestCase("GetGroup")]
    [TestCase("UpsertRule")]
    public async Task A_call_without_a_deadline_gets_one(string method)
    {
        var invoker = new CapturingInvoker();
        var before = DateTime.UtcNow;

        await Invoke(invoker, method, new CallOptions());

        Assert.That(invoker.Deadline, Is.Not.Null.And.InRange(before + Bound, DateTime.UtcNow + Bound));
    }

    /// <summary>A caller's own deadline is kept.</summary>
    [Test]
    public async Task A_callers_deadline_is_kept()
    {
        var invoker = new CapturingInvoker();
        var own = DateTime.UtcNow.AddSeconds(7);

        await Invoke(invoker, "ListRules", new CallOptions(deadline: own));

        Assert.That(invoker.Deadline, Is.EqualTo(own));
    }

    private static Task<string> Invoke(CallInvoker invoker, string method, CallOptions options) =>
        invoker.Intercept(TransientReadRetryInterceptor.Instance)
            .AsyncUnaryCall(new Method<string, string>(MethodType.Unary, "selftest", method, Text, Text), null, options, "request")
            .ResponseAsync;

    private sealed class CapturingInvoker : CallInvoker
    {
        public DateTime? Deadline { get; private set; }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            Deadline = options.Deadline;
            return new AsyncUnaryCall<TResponse>(
                Task.FromResult((TResponse)(object)"response"),
                Task.FromResult(new Grpc.Core.Metadata()),
                () => Status.DefaultSuccess,
                () => [],
                () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();
    }
}
