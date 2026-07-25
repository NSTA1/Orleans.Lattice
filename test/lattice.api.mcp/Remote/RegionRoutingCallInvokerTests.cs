using System.Collections.Frozen;
using Grpc.Core;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="RegionRoutingCallInvoker"/>: the per-group invoker
/// that dispatches each outbound gRPC call to the region selected by the ambient
/// <see cref="LatticeApiMcpRegionScope"/>. Proves the default-region fast path (no
/// selection), explicit region selection, and the defensive fallback for a
/// selection absent from the map - all without touching a network.
/// </summary>
[TestFixture]
public sealed class RegionRoutingCallInvokerTests
{
    private static readonly Method<string, string> Method = new(
        MethodType.Unary,
        "svc",
        "m",
        Marshallers.Create(static s => System.Text.Encoding.UTF8.GetBytes(s), static b => System.Text.Encoding.UTF8.GetString(b)),
        Marshallers.Create(static s => System.Text.Encoding.UTF8.GetBytes(s), static b => System.Text.Encoding.UTF8.GetString(b)));

    private static FakeCallInvoker Named(string name) => new(_ => name);

    private static RegionRoutingCallInvoker Build(
        FakeCallInvoker @default,
        params (string Region, FakeCallInvoker Invoker)[] peers)
    {
        var map = new Dictionary<string, CallInvoker>(StringComparer.Ordinal) { ["current"] = @default };
        foreach (var (region, invoker) in peers)
        {
            map[region] = invoker;
        }

        return new RegionRoutingCallInvoker(@default, map.ToFrozenDictionary(StringComparer.Ordinal));
    }

    private static string Invoke(RegionRoutingCallInvoker invoker)
        => invoker.AsyncUnaryCall(Method, host: null, new CallOptions(), "req").ResponseAsync.GetAwaiter().GetResult();

    [Test]
    public void No_region_selected_routes_to_the_default_invoker()
    {
        var routing = Build(Named("default"), ("peer", Named("peer")));

        Assert.That(Invoke(routing), Is.EqualTo("default"),
            "With no ambient region the call must take the default-region channel.");
    }

    [Test]
    public void Selected_region_routes_to_that_regions_invoker()
    {
        var routing = Build(Named("default"), ("peer", Named("peer")));

        using (LatticeApiMcpRegionScope.Enter("peer"))
        {
            Assert.That(Invoke(routing), Is.EqualTo("peer"));
        }
    }

    [Test]
    public void Selection_absent_from_the_map_falls_back_to_the_default_invoker()
    {
        var routing = Build(Named("default"), ("peer", Named("peer")));

        using (LatticeApiMcpRegionScope.Enter("unmapped"))
        {
            Assert.That(Invoke(routing), Is.EqualTo("default"),
                "A defensive miss must fall back to the default region rather than throw.");
        }
    }

    [Test]
    public void Null_default_invoker_throws()
        => Assert.That(
            () => new RegionRoutingCallInvoker(null!, FrozenDictionary<string, CallInvoker>.Empty),
            Throws.ArgumentNullException);

    [Test]
    public void Null_map_throws()
        => Assert.That(
            () => new RegionRoutingCallInvoker(Named("d"), null!),
            Throws.ArgumentNullException);
}
