using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Schema;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Direct unit tests for the typed <see cref="LatticeSchemaApiGrpcClient"/> that
/// drive every wrapper method over a <see cref="FakeCallInvoker"/> - no gRPC
/// server, channel, or cluster - proving each method builds the right request,
/// unwraps the right field from the canned response, and enforces its argument
/// contract. The live wire round trip is covered separately by the
/// integration-tagged E2E fixture; this fixture covers the request/response
/// shaping and validation branches cheaply and deterministically.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaApiGrpcClientUnitTests
{
    private const string Tree = "orders";

    private ServiceProvider _serializerProvider = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    private LatticeSchemaApiGrpcClient ClientReturning(object unaryResponse) =>
        LatticeSchemaApiGrpcClient.Create(FakeCallInvoker.ForUnary(unaryResponse), _serializerProvider);

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public void Create_with_null_call_invoker_throws()
    {
        Assert.That(
            () => LatticeSchemaApiGrpcClient.Create(null!, _serializerProvider),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Create_with_null_serializer_provider_throws()
    {
        Assert.That(
            () => LatticeSchemaApiGrpcClient.Create(FakeCallInvoker.ForUnary(new SchemaAckResponse()), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task SetPolicyAsync_sends_tree_and_policy()
    {
        var invoker = FakeCallInvoker.ForUnary(new SchemaAckResponse());
        var client = LatticeSchemaApiGrpcClient.Create(invoker, _serializerProvider);

        await client.SetPolicyAsync(Tree, JsonPolicy());

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeSchemaGrpcMethods.SetPolicyMethodName));
            Assert.That(invoker.LastRequest, Is.InstanceOf<SetPolicyRequest>());
            Assert.That(((SetPolicyRequest)invoker.LastRequest!).TreeId, Is.EqualTo(Tree));
        });
    }

    [Test]
    public void SetPolicyAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaAckResponse());
        Assert.That(async () => await client.SetPolicyAsync("", JsonPolicy()), Throws.ArgumentException);
    }

    [Test]
    public void SetPolicyAsync_with_null_policy_throws()
    {
        var client = ClientReturning(new SchemaAckResponse());
        Assert.That(async () => await client.SetPolicyAsync(Tree, null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ClearPolicyAsync_returns_the_removed_flag()
    {
        var client = ClientReturning(new SchemaRemovedResponse { Removed = true });

        Assert.That(await client.ClearPolicyAsync(Tree), Is.True);
    }

    [Test]
    public void ClearPolicyAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaRemovedResponse { Removed = false });
        Assert.That(async () => await client.ClearPolicyAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task GetPolicyAsync_returns_policy_when_found()
    {
        var client = ClientReturning(new GetPolicyResponse { Found = true, Policy = JsonPolicy() });

        var policy = await client.GetPolicyAsync(Tree);

        Assert.That(policy, Is.Not.Null);
        Assert.That(policy!.Rules, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task GetPolicyAsync_returns_null_when_not_found()
    {
        var client = ClientReturning(new GetPolicyResponse { Found = false });

        Assert.That(await client.GetPolicyAsync(Tree), Is.Null);
    }

    [Test]
    public void GetPolicyAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new GetPolicyResponse { Found = false });
        Assert.That(async () => await client.GetPolicyAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ListDeadLettersAsync_streams_every_entry()
    {
        var entries = new[]
        {
            new LatticeSchemaDeadLetterEntry("k1", Array.Empty<byte>(), 0, "bad", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UnixEpoch),
            new LatticeSchemaDeadLetterEntry("k2", Array.Empty<byte>(), 0, "bad", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UnixEpoch),
        };
        var invoker = FakeCallInvoker.ForStream(entries);
        var client = LatticeSchemaApiGrpcClient.Create(invoker, _serializerProvider);

        var seen = new List<string>();
        await foreach (var entry in client.ListDeadLettersAsync(Tree))
        {
            seen.Add(entry.Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName));
            Assert.That(seen, Is.EqualTo(new[] { "k1", "k2" }));
        });
    }

    [Test]
    public void ListDeadLettersAsync_with_empty_tree_throws()
    {
        var client = LatticeSchemaApiGrpcClient.Create(
            FakeCallInvoker.ForStream(Array.Empty<LatticeSchemaDeadLetterEntry>()), _serializerProvider);

        Assert.That(() => client.ListDeadLettersAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task CountDeadLettersAsync_returns_the_count()
    {
        var client = ClientReturning(new SchemaCountResponse { Count = 7 });

        Assert.That(await client.CountDeadLettersAsync(Tree), Is.EqualTo(7));
    }

    [Test]
    public void CountDeadLettersAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaCountResponse { Count = 0 });
        Assert.That(async () => await client.CountDeadLettersAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task SetVersionConfigAsync_sends_tree_and_config()
    {
        var invoker = FakeCallInvoker.ForUnary(new SchemaAckResponse());
        var client = LatticeSchemaApiGrpcClient.Create(invoker, _serializerProvider);

        await client.SetVersionConfigAsync(Tree, new LatticeSchemaVersionConfig(1, 2));

        Assert.That(((SetVersionConfigRequest)invoker.LastRequest!).TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public void SetVersionConfigAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaAckResponse());
        Assert.That(
            async () => await client.SetVersionConfigAsync("", new LatticeSchemaVersionConfig(1, 2)),
            Throws.ArgumentException);
    }

    [Test]
    public async Task GetVersionConfigAsync_returns_config_when_found()
    {
        var client = ClientReturning(new GetVersionConfigResponse { Found = true, Config = new LatticeSchemaVersionConfig(1, 5) });

        var config = await client.GetVersionConfigAsync(Tree);

        Assert.That(config, Is.Not.Null);
        Assert.That(config!.Value.TargetVersion, Is.EqualTo(5u));
    }

    [Test]
    public async Task GetVersionConfigAsync_returns_null_when_not_found()
    {
        var client = ClientReturning(new GetVersionConfigResponse { Found = false });

        Assert.That(await client.GetVersionConfigAsync(Tree), Is.Null);
    }

    [Test]
    public void GetVersionConfigAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new GetVersionConfigResponse { Found = false });
        Assert.That(async () => await client.GetVersionConfigAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task AdvanceTargetVersionAsync_returns_the_updated_config()
    {
        var client = ClientReturning(new VersionConfigResponse { Config = new LatticeSchemaVersionConfig(1, 9) });

        var config = await client.AdvanceTargetVersionAsync(Tree, 9);

        Assert.That(config.TargetVersion, Is.EqualTo(9u));
    }

    [Test]
    public void AdvanceTargetVersionAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new VersionConfigResponse { Config = new LatticeSchemaVersionConfig(1, 2) });
        Assert.That(async () => await client.AdvanceTargetVersionAsync("", 3), Throws.ArgumentException);
    }

    [Test]
    public async Task AdvanceAndMigrateAsync_returns_the_report()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });

        var report = await client.AdvanceAndMigrateAsync(Tree, 4);

        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public void AdvanceAndMigrateAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });
        Assert.That(async () => await client.AdvanceAndMigrateAsync("", 4), Throws.ArgumentException);
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_returns_the_report()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });

        var report = await client.MigrateToTargetVersionAsync(Tree);

        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public void MigrateToTargetVersionAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });
        Assert.That(async () => await client.MigrateToTargetVersionAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ClearVersionConfigAsync_returns_the_removed_flag()
    {
        var client = ClientReturning(new SchemaRemovedResponse { Removed = false });

        Assert.That(await client.ClearVersionConfigAsync(Tree), Is.False);
    }

    [Test]
    public void ClearVersionConfigAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaRemovedResponse { Removed = false });
        Assert.That(async () => await client.ClearVersionConfigAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task RemediateAsync_sends_transform_and_target_policy()
    {
        var invoker = FakeCallInvoker.ForUnary(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });
        var client = LatticeSchemaApiGrpcClient.Create(invoker, _serializerProvider);

        var report = await client.RemediateAsync(Tree, LatticeValueTransform.Passthrough(), JsonPolicy());

        Assert.Multiple(() =>
        {
            Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
            Assert.That(((RemediateRequest)invoker.LastRequest!).TreeId, Is.EqualTo(Tree));
        });
    }

    [Test]
    public void RemediateAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });
        Assert.That(
            async () => await client.RemediateAsync("", LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.ArgumentException);
    }

    [Test]
    public void RemediateAsync_with_null_target_policy_throws()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });
        Assert.That(
            async () => await client.RemediateAsync(Tree, LatticeValueTransform.Passthrough(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetRemediationStatusAsync_returns_the_report()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });

        var report = await client.GetRemediationStatusAsync(Tree);

        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public void GetRemediationStatusAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });
        Assert.That(async () => await client.GetRemediationStatusAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ScanComplianceAsync_returns_the_report()
    {
        var report = LatticeSchemaComplianceReport.Ungoverned(Tree) with { HasPolicy = true };
        var client = ClientReturning(new SchemaComplianceReportResponse { Report = report });

        var result = await client.ScanComplianceAsync(Tree);

        Assert.That(result.TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public void ScanComplianceAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new SchemaComplianceReportResponse
        {
            Report = LatticeSchemaComplianceReport.Ungoverned(Tree),
        });
        Assert.That(async () => await client.ScanComplianceAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_returns_the_capabilities()
    {
        var client = ClientReturning(new LatticeSchemaCapabilities { TreeId = Tree, CanViewPolicy = true });

        var capabilities = await client.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(capabilities.TreeId, Is.EqualTo(Tree));
            Assert.That(capabilities.CanViewPolicy, Is.True);
        });
    }

    [Test]
    public void ProbeCapabilitiesAsync_with_empty_tree_throws()
    {
        var client = ClientReturning(new LatticeSchemaCapabilities { TreeId = Tree });
        Assert.That(async () => await client.ProbeCapabilitiesAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task GetAuthSchemeAsync_returns_the_advertised_schemes()
    {
        var advertisement = new AuthSchemeAdvertisement
        {
            Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "entra" } },
        };
        var client = ClientReturning(advertisement);

        var schemes = await client.GetAuthSchemeAsync();

        Assert.That(schemes, Has.Count.EqualTo(1));
        Assert.That(schemes[0].SchemeId, Is.EqualTo("entra"));
    }
}
