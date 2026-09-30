using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// <see cref="LatticeGrpcChannelFactory.ApplyActiveTenant"/> and the settings
/// seam that carries the circuit's live tenant source to it.
/// </summary>
[TestFixture]
public sealed class LatticeGrpcChannelFactoryActiveTenantTests
{
    private static readonly Method<string, string> Unary =
        new(MethodType.Unary, "svc", "M", Marshallers.StringMarshaller, Marshallers.StringMarshaller);

    [Test]
    public void ApplyActiveTenant_returns_the_invoker_unchanged_without_a_provider()
    {
        var invoker = new RecordingCallInvoker();

        Assert.That(LatticeGrpcChannelFactory.ApplyActiveTenant(invoker, null), Is.SameAs(invoker));
    }

    [Test]
    public void ApplyActiveTenant_asserts_the_provider_tenant_on_each_call()
    {
        var recorded = new RecordingCallInvoker();
        var provider = new FakeActiveTenantProvider("acme");
        var invoker = LatticeGrpcChannelFactory.ApplyActiveTenant(recorded, provider);

        invoker.AsyncUnaryCall(Unary, null, default, "x");
        provider.Set("globex");
        invoker.AsyncUnaryCall(Unary, null, default, "x");

        Assert.That(
            recorded.Headers.Select(headers => headers!.GetValue(LatticeActiveTenantAssertion.DefaultHeaderName)),
            Is.EqualTo(new[] { "acme", "globex" }));
    }

    [Test]
    public void ApplyActiveTenant_rejects_a_null_invoker() =>
        Assert.That(
            () => LatticeGrpcChannelFactory.ApplyActiveTenant(null!, new FakeActiveTenantProvider()),
            Throws.ArgumentNullException);

    [Test]
    public void ActiveTenantProvider_defaults_to_none_and_survives_a_sign_in()
    {
        var provider = new FakeActiveTenantProvider("acme");
        var settings = new LatticeConnectionSettings { Address = "https://host:443" };
        var withTenant = settings with { ActiveTenantProvider = provider };
        var afterSignIn = withTenant with { Authentication = LatticeCallAuthentication.Basic("alice", "pw") };

        Assert.Multiple(() =>
        {
            Assert.That(settings.ActiveTenantProvider, Is.Null);
            Assert.That(afterSignIn.ActiveTenantProvider, Is.SameAs(provider));
            Assert.That(provider.Reads, Is.Zero, "the settings never read the tenant; only a call does");
        });
    }
}
