using System.Collections.Generic;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeSchemaControl"/>, the remote-host adapter
/// that fronts <see cref="ILatticeSchemaControl"/> over the schema-API gRPC client.
/// Because the client already projects the wire messages back onto the abstractions
/// DTOs, the adapter is a pure pass-through: every test proves a member forwards its
/// request (carrying the tree id and any argument) and returns the client result
/// verbatim, plus the streaming drain and the constructor guard. Deterministic over
/// a <see cref="FakeCallInvoker"/> - no channel, no cluster.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeSchemaControlTests
{
    private static GrpcLatticeSchemaControl Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.SchemaClient(invoker));

    private static GrpcLatticeSchemaControl Adapter(
        Func<object, object> unary, Func<object, IEnumerable<object>>? stream = null)
        => Adapter(new FakeCallInvoker(unary, stream is null ? null : r => stream(r)));

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeSchemaControl(null!), Throws.ArgumentNullException);

    [Test]
    public async Task SetPolicyAsync_forwards_the_tree_id_and_policy()
    {
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        var invoker = new FakeCallInvoker(_ => new SchemaAckResponse());

        await Adapter(invoker).SetPolicyAsync("orders", policy);

        var sent = (SetPolicyRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Policy, Is.SameAs(policy));
        });
    }

    [Test]
    public async Task ClearPolicyAsync_forwards_and_unwraps_removed()
    {
        var invoker = new FakeCallInvoker(_ => new SchemaRemovedResponse { Removed = true });

        var removed = await Adapter(invoker).ClearPolicyAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(((SchemaTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task GetPolicyAsync_unwraps_a_found_policy()
    {
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Utf8() });
        var result = await Adapter(_ => new GetPolicyResponse { Found = true, Policy = policy })
            .GetPolicyAsync("orders");

        Assert.That(result, Is.SameAs(policy));
    }

    [Test]
    public async Task GetPolicyAsync_maps_not_found_to_null()
    {
        var result = await Adapter(_ => new GetPolicyResponse { Found = false })
            .GetPolicyAsync("orders");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task ListDeadLettersAsync_drains_the_server_stream()
    {
        var entries = new object[]
        {
            new LatticeSchemaDeadLetterEntry("k1", new byte[] { 1 }, 2, "bad", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UnixEpoch),
            new LatticeSchemaDeadLetterEntry("k2", new byte[] { 3 }, 9, "big", LatticeSchemaDeadLetterSource.Restore, DateTimeOffset.UnixEpoch),
        };
        var adapter = Adapter(_ => throw new InvalidOperationException("unary not used"), _ => entries);

        var keys = new List<string>();
        await foreach (var entry in adapter.ListDeadLettersAsync("orders"))
        {
            keys.Add(entry.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "k1", "k2" }));
    }

    [Test]
    public async Task ListDeadLettersAsync_drains_an_empty_stream()
    {
        var adapter = Adapter(_ => throw new InvalidOperationException("unary not used"), _ => Array.Empty<object>());

        var count = 0;
        await foreach (var _ in adapter.ListDeadLettersAsync("orders"))
        {
            count++;
        }

        Assert.That(count, Is.Zero);
    }

    [Test]
    public async Task CountDeadLettersAsync_unwraps_the_count()
    {
        var count = await Adapter(_ => new SchemaCountResponse { Count = 7 }).CountDeadLettersAsync("orders");

        Assert.That(count, Is.EqualTo(7));
    }

    [Test]
    public async Task SetVersionConfigAsync_forwards_the_tree_id_and_config()
    {
        var config = new LatticeSchemaVersionConfig(4, 2);
        var invoker = new FakeCallInvoker(_ => new SchemaAckResponse());

        await Adapter(invoker).SetVersionConfigAsync("orders", config);

        var sent = (SetVersionConfigRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Config, Is.EqualTo(config));
        });
    }

    [Test]
    public async Task GetVersionConfigAsync_unwraps_a_found_config()
    {
        var config = new LatticeSchemaVersionConfig(3, 5);
        var result = await Adapter(_ => new GetVersionConfigResponse { Found = true, Config = config })
            .GetVersionConfigAsync("orders");

        Assert.That(result, Is.EqualTo(config));
    }

    [Test]
    public async Task GetVersionConfigAsync_maps_not_found_to_null()
    {
        var result = await Adapter(_ => new GetVersionConfigResponse { Found = false, Config = new LatticeSchemaVersionConfig(1, 1) })
            .GetVersionConfigAsync("orders");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task AdvanceTargetVersionAsync_forwards_the_new_target_and_unwraps_config()
    {
        var config = new LatticeSchemaVersionConfig(1, 4);
        var invoker = new FakeCallInvoker(_ => new VersionConfigResponse { Config = config });

        var result = await Adapter(invoker).AdvanceTargetVersionAsync("orders", 4);

        var sent = (AdvanceVersionRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.NewTargetVersion, Is.EqualTo(4u));
            Assert.That(result, Is.EqualTo(config));
        });
    }

    [Test]
    public async Task AdvanceAndMigrateAsync_forwards_the_new_target_and_unwraps_report()
    {
        var report = LatticeSchemaRemediationReport.Completed(3, "orders#v4", "op-1");
        var invoker = new FakeCallInvoker(_ => new SchemaRemediationReportResponse { Report = report });

        var result = await Adapter(invoker).AdvanceAndMigrateAsync("orders", 4);

        Assert.Multiple(() =>
        {
            Assert.That(((AdvanceVersionRequest)invoker.LastRequest!).NewTargetVersion, Is.EqualTo(4u));
            Assert.That(result, Is.EqualTo(report));
        });
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_unwraps_the_report()
    {
        var report = LatticeSchemaRemediationReport.Completed(2, "orders#v2", "op-2");
        var result = await Adapter(_ => new SchemaRemediationReportResponse { Report = report })
            .MigrateToTargetVersionAsync("orders");

        Assert.That(result, Is.EqualTo(report));
    }

    [Test]
    public async Task ClearVersionConfigAsync_unwraps_removed()
    {
        var removed = await Adapter(_ => new SchemaRemovedResponse { Removed = false })
            .ClearVersionConfigAsync("orders");

        Assert.That(removed, Is.False);
    }

    [Test]
    public async Task RemediateAsync_forwards_transform_and_policy_and_unwraps_report()
    {
        var transform = LatticeValueTransform.Passthrough();
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        var report = LatticeSchemaRemediationReport.Completed(5, "orders#r1", "op-3");
        var invoker = new FakeCallInvoker(_ => new SchemaRemediationReportResponse { Report = report });

        var result = await Adapter(invoker).RemediateAsync("orders", transform, policy);

        var sent = (RemediateRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Transform, Is.EqualTo(transform));
            Assert.That(sent.TargetPolicy, Is.SameAs(policy));
            Assert.That(result, Is.EqualTo(report));
        });
    }

    [Test]
    public async Task GetRemediationStatusAsync_unwraps_the_report()
    {
        var report = LatticeSchemaRemediationReport.Idle;
        var result = await Adapter(_ => new SchemaRemediationReportResponse { Report = report })
            .GetRemediationStatusAsync("orders");

        Assert.That(result, Is.EqualTo(report));
    }

    [Test]
    public async Task ScanComplianceAsync_unwraps_the_report()
    {
        var report = LatticeSchemaComplianceReport.Ungoverned("orders");
        var result = await Adapter(_ => new SchemaComplianceReportResponse { Report = report })
            .ScanComplianceAsync("orders");

        Assert.That(result, Is.EqualTo(report));
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_forwards_request_and_returns_capabilities()
    {
        var caps = new LatticeSchemaCapabilities { TreeId = "orders", CanViewPolicy = true };
        var invoker = new FakeCallInvoker(_ => caps);

        var result = await Adapter(invoker).ProbeCapabilitiesAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((SchemaTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result, Is.SameAs(caps));
        });
    }
}
