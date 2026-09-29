using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Lattice.Explorer.Core.Connection;
using GrpcMetadata = Grpc.Core.Metadata;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// The circuit's tenant assertion on the wire: every call shape carries the
/// provider's tenant in the cluster's header, read as the call starts, and a
/// call that asserts none carries no header at all.
/// </summary>
[TestFixture]
public sealed class ActiveTenantInterceptorTests
{
    private static readonly Method<string, string> Unary = Make(MethodType.Unary);
    private static readonly Method<string, string> ServerStreaming = Make(MethodType.ServerStreaming);
    private static readonly Method<string, string> ClientStreaming = Make(MethodType.ClientStreaming);
    private static readonly Method<string, string> Duplex = Make(MethodType.DuplexStreaming);

    [Test]
    public void The_constructor_rejects_a_missing_provider() =>
        Assert.That(() => new ActiveTenantInterceptor(null!), Throws.ArgumentNullException);

    [Test]
    public void The_header_is_the_one_the_cluster_reads() =>
        Assert.That(ActiveTenantInterceptor.HeaderName, Is.EqualTo(LatticeActiveTenantAssertion.DefaultHeaderName));

    [Test]
    public void Every_call_shape_asserts_the_tenant()
    {
        var (invoker, recorded, _) = Pipeline("acme");

        invoker.BlockingUnaryCall(Unary, null, default, "x");
        invoker.AsyncUnaryCall(Unary, null, default, "x");
        invoker.AsyncServerStreamingCall(ServerStreaming, null, default, "x");
        invoker.AsyncClientStreamingCall(ClientStreaming, null, default);
        invoker.AsyncDuplexStreamingCall(Duplex, null, default);

        Assert.That(recorded.Headers.Select(Asserted), Is.EqualTo(Enumerable.Repeat<string?>("acme", 5)));
    }

    [Test]
    public void The_cluster_resolves_the_asserted_tenant_from_the_metadata()
    {
        var (invoker, recorded, _) = Pipeline("acme");

        invoker.AsyncUnaryCall(Unary, null, default, "x");

        var resolved = LatticeActiveTenantAssertion.Resolve(
            recorded.Headers.Single()!,
            static (headers, name) => headers.GetValue(name),
            LatticeActiveTenantAssertion.DefaultHeaderName);
        Assert.That(resolved?.Value, Is.EqualTo("acme"));
    }

    [Test]
    public void A_call_that_asserts_no_tenant_carries_no_header_and_no_metadata()
    {
        var (invoker, recorded, _) = Pipeline(null);

        invoker.AsyncUnaryCall(Unary, null, default, "x");

        Assert.That(recorded.Headers.Single(), Is.Null, "no metadata is allocated for a call that asserts nothing");
    }

    [Test]
    public void The_tenant_is_read_as_each_call_starts()
    {
        var (invoker, recorded, provider) = Pipeline("acme");

        invoker.AsyncUnaryCall(Unary, null, default, "x");
        provider.Set("globex");
        invoker.AsyncUnaryCall(Unary, null, default, "x");
        provider.Set(null);
        invoker.AsyncUnaryCall(Unary, null, default, "x");

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Headers.Select(Asserted), Is.EqualTo(new[] { "acme", "globex", null }));
            Assert.That(provider.Reads, Is.EqualTo(3));
        });
    }

    [Test]
    public void A_value_for_the_header_from_elsewhere_is_replaced_and_other_headers_are_kept()
    {
        var (invoker, recorded, _) = Pipeline("acme");
        var headers = new GrpcMetadata
        {
            { LatticeActiveTenantAssertion.DefaultHeaderName, "globex" },
            { "x-azure-fdid", "origin" },
        };

        invoker.AsyncUnaryCall(Unary, null, new CallOptions(headers), "x");

        var sent = recorded.Headers.Single()!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.GetAll(LatticeActiveTenantAssertion.DefaultHeaderName).Select(entry => entry.Value), Is.EqualTo(new[] { "acme" }));
            Assert.That(sent.GetValue("x-azure-fdid"), Is.EqualTo("origin"));
        });
    }

    [Test]
    public void With_no_tenant_a_value_for_the_header_from_elsewhere_is_removed()
    {
        var (invoker, recorded, _) = Pipeline(null);
        var headers = new GrpcMetadata
        {
            { LatticeActiveTenantAssertion.DefaultHeaderName, "globex" },
            { "x-azure-fdid", "origin" },
        };

        invoker.AsyncUnaryCall(Unary, null, new CallOptions(headers), "x");

        var sent = recorded.Headers.Single()!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Get(LatticeActiveTenantAssertion.DefaultHeaderName), Is.Null);
            Assert.That(sent.GetValue("x-azure-fdid"), Is.EqualTo("origin"));
        });
    }

    [Test]
    public void An_unchanged_tenant_reuses_one_header_entry()
    {
        var (invoker, recorded, provider) = Pipeline("acme");

        invoker.AsyncUnaryCall(Unary, null, default, "x");
        invoker.AsyncUnaryCall(Unary, null, default, "x");
        provider.Set("globex");
        invoker.AsyncUnaryCall(Unary, null, default, "x");

        var entries = recorded.Headers.Select(headers => headers!.Get(LatticeActiveTenantAssertion.DefaultHeaderName)).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(entries[1], Is.SameAs(entries[0]));
            Assert.That(entries[2], Is.Not.SameAs(entries[0]));
            Assert.That(entries[2]!.Value, Is.EqualTo("globex"));
        });
    }

    private static (CallInvoker Invoker, RecordingCallInvoker Recorded, FakeActiveTenantProvider Provider) Pipeline(string? tenant)
    {
        var recorded = new RecordingCallInvoker();
        var provider = new FakeActiveTenantProvider(tenant);
        return (recorded.Intercept(new ActiveTenantInterceptor(provider)), recorded, provider);
    }

    private static string? Asserted(GrpcMetadata? headers) =>
        headers?.GetValue(LatticeActiveTenantAssertion.DefaultHeaderName);

    private static Method<string, string> Make(MethodType type) =>
        new(type, "svc", type.ToString(), Marshallers.StringMarshaller, Marshallers.StringMarshaller);
}
