using System.Collections.Generic;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminSchemaToolHandlers"/>, the thin adapter
/// methods behind the tree-administration schema-control tools. Every test drives a
/// handler with a substituted <see cref="ILatticeSchemaControl"/> facade and proves
/// the handler marshals the tool-call arguments into the facade's model types and
/// forwards the call verbatim - it re-implements no authorization, read, or write
/// logic. Covers every inspection read and management write, the dead-letter stream
/// drain into a named result, the version-config construction from scalar
/// arguments, and the null-facade guards. Deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class TreeAdminSchemaToolHandlersTests
{
    private static ILatticeSchemaControl Schema() => Substitute.For<ILatticeSchemaControl>();

    // ----- Inspection -----

    [Test]
    public async Task GetPolicyAsync_forwards_the_tree_id_and_returns_the_policy()
    {
        var schema = Schema();
        var expected = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Utf8() }, strictIngest: true);
        schema.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminSchemaToolHandlers.GetPolicyAsync(schema, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await schema.Received(1).GetPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListDeadLettersAsync_drains_the_stream_into_a_named_result()
    {
        var schema = Schema();
        var entries = new[]
        {
            new LatticeSchemaDeadLetterEntry("k1", new byte[] { 1, 2 }, 2, "bad", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UnixEpoch),
            new LatticeSchemaDeadLetterEntry("k2", new byte[] { 3 }, 9, "too big", LatticeSchemaDeadLetterSource.Restore, DateTimeOffset.UnixEpoch),
        };
        schema.ListDeadLettersAsync("orders", Arg.Any<CancellationToken>()).Returns(ToAsync(entries));

        var result = await TreeAdminSchemaToolHandlers.ListDeadLettersAsync(schema, "orders", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Entries.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2" }));
        });
    }

    [Test]
    public async Task ListDeadLettersAsync_drains_an_empty_stream_into_an_empty_result()
    {
        var schema = Schema();
        schema.ListDeadLettersAsync("orders", Arg.Any<CancellationToken>())
            .Returns(ToAsync(Array.Empty<LatticeSchemaDeadLetterEntry>()));

        var result = await TreeAdminSchemaToolHandlers.ListDeadLettersAsync(schema, "orders", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task CountDeadLettersAsync_returns_the_facade_count()
    {
        var schema = Schema();
        schema.CountDeadLettersAsync("orders", Arg.Any<CancellationToken>()).Returns(7);

        var count = await TreeAdminSchemaToolHandlers.CountDeadLettersAsync(schema, "orders", CancellationToken.None);

        Assert.That(count, Is.EqualTo(7));
    }

    [Test]
    public async Task GetVersionConfigAsync_returns_the_facade_config()
    {
        var schema = Schema();
        var config = new LatticeSchemaVersionConfig(3, 5);
        schema.GetVersionConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(config);

        var result = await TreeAdminSchemaToolHandlers.GetVersionConfigAsync(schema, "orders", CancellationToken.None);

        Assert.That(result, Is.EqualTo(config));
    }

    [Test]
    public async Task GetRemediationStatusAsync_forwards_to_the_facade()
    {
        var schema = Schema();
        var report = LatticeSchemaRemediationReport.Completed(10, "orders#v2", "op-1");
        schema.GetRemediationStatusAsync("orders", Arg.Any<CancellationToken>()).Returns(report);

        var result = await TreeAdminSchemaToolHandlers.GetRemediationStatusAsync(schema, "orders", CancellationToken.None);

        Assert.That(result, Is.EqualTo(report));
    }

    [Test]
    public async Task ScanComplianceAsync_forwards_to_the_facade()
    {
        var schema = Schema();
        var report = LatticeSchemaComplianceReport.Ungoverned("orders");
        schema.ScanComplianceAsync("orders", Arg.Any<CancellationToken>()).Returns(report);

        var result = await TreeAdminSchemaToolHandlers.ScanComplianceAsync(schema, "orders", CancellationToken.None);

        Assert.That(result, Is.EqualTo(report));
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_forwards_to_the_facade()
    {
        var schema = Schema();
        var capabilities = new LatticeSchemaCapabilities { TreeId = "orders", CanViewPolicy = true };
        schema.ProbeCapabilitiesAsync("orders", Arg.Any<CancellationToken>()).Returns(capabilities);

        var result = await TreeAdminSchemaToolHandlers.ProbeCapabilitiesAsync(schema, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(capabilities));
    }

    // ----- Management -----

    [Test]
    public async Task SetPolicyAsync_forwards_the_policy_and_echoes_it()
    {
        var schema = Schema();
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });

        var result = await TreeAdminSchemaToolHandlers.SetPolicyAsync(schema, "orders", policy, CancellationToken.None);

        Assert.That(result, Is.SameAs(policy));
        await schema.Received(1).SetPolicyAsync("orders", policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ClearPolicyAsync_returns_the_facade_result()
    {
        var schema = Schema();
        schema.ClearPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(true);

        var removed = await TreeAdminSchemaToolHandlers.ClearPolicyAsync(schema, "orders", CancellationToken.None);

        Assert.That(removed, Is.True);
    }

    [Test]
    public async Task SetVersionConfigAsync_builds_the_config_from_scalars_and_echoes_it()
    {
        var schema = Schema();

        var config = await TreeAdminSchemaToolHandlers.SetVersionConfigAsync(
            schema, "orders", schemaId: 4, targetVersion: 2, strictIngest: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(config.SchemaId, Is.EqualTo(4u));
            Assert.That(config.TargetVersion, Is.EqualTo(2u));
            Assert.That(config.StrictIngest, Is.True);
        });
        await schema.Received(1).SetVersionConfigAsync(
            "orders",
            Arg.Is<LatticeSchemaVersionConfig>(c => c.SchemaId == 4 && c.TargetVersion == 2 && c.StrictIngest),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetVersionConfigAsync_defaults_strict_ingest_to_false()
    {
        var schema = Schema();

        var config = await TreeAdminSchemaToolHandlers.SetVersionConfigAsync(
            schema, "orders", schemaId: 1, targetVersion: 1);

        Assert.That(config.StrictIngest, Is.False);
    }

    [Test]
    public async Task ClearVersionConfigAsync_returns_the_facade_result()
    {
        var schema = Schema();
        schema.ClearVersionConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(false);

        var removed = await TreeAdminSchemaToolHandlers.ClearVersionConfigAsync(schema, "orders", CancellationToken.None);

        Assert.That(removed, Is.False);
    }

    [Test]
    public async Task AdvanceTargetVersionAsync_forwards_the_new_target()
    {
        var schema = Schema();
        var config = new LatticeSchemaVersionConfig(1, 4);
        schema.AdvanceTargetVersionAsync("orders", 4, Arg.Any<CancellationToken>()).Returns(config);

        var result = await TreeAdminSchemaToolHandlers.AdvanceTargetVersionAsync(schema, "orders", 4, CancellationToken.None);

        Assert.That(result, Is.EqualTo(config));
        await schema.Received(1).AdvanceTargetVersionAsync("orders", 4, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AdvanceAndMigrateAsync_forwards_the_new_target()
    {
        var schema = Schema();
        var report = LatticeSchemaRemediationReport.Completed(3, "orders#v4", "op-2");
        schema.AdvanceAndMigrateAsync("orders", 4, Arg.Any<CancellationToken>()).Returns(report);

        var result = await TreeAdminSchemaToolHandlers.AdvanceAndMigrateAsync(schema, "orders", 4, CancellationToken.None);

        Assert.That(result, Is.EqualTo(report));
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_forwards_to_the_facade()
    {
        var schema = Schema();
        var report = LatticeSchemaRemediationReport.Idle;
        schema.MigrateToTargetVersionAsync("orders", Arg.Any<CancellationToken>()).Returns(report);

        var result = await TreeAdminSchemaToolHandlers.MigrateToTargetVersionAsync(schema, "orders", CancellationToken.None);

        Assert.That(result, Is.EqualTo(report));
    }

    [Test]
    public async Task RemediateAsync_forwards_the_transform_and_target_policy()
    {
        var schema = Schema();
        var transform = LatticeValueTransform.Passthrough(LatticeValueTransform.DropMember("legacy"));
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        var report = LatticeSchemaRemediationReport.Completed(5, "orders#r1", "op-3");
        schema.RemediateAsync("orders", transform, policy, Arg.Any<CancellationToken>()).Returns(report);

        var result = await TreeAdminSchemaToolHandlers.RemediateAsync(schema, "orders", transform, policy, CancellationToken.None);

        Assert.That(result, Is.EqualTo(report));
        await schema.Received(1).RemediateAsync("orders", transform, policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Handlers_reject_a_null_facade()
    {
        var transform = LatticeValueTransform.Passthrough();
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());

        Assert.Multiple(() =>
        {
            Assert.That(() => TreeAdminSchemaToolHandlers.GetPolicyAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.ListDeadLettersAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.CountDeadLettersAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.GetVersionConfigAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.GetRemediationStatusAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.ScanComplianceAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.ProbeCapabilitiesAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.SetPolicyAsync(null!, "t", policy), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.ClearPolicyAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.SetVersionConfigAsync(null!, "t", 1, 1), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.ClearVersionConfigAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.AdvanceTargetVersionAsync(null!, "t", 2), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.AdvanceAndMigrateAsync(null!, "t", 2), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.MigrateToTargetVersionAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminSchemaToolHandlers.RemediateAsync(null!, "t", transform, policy), Throws.ArgumentNullException);
        });
    }

    private static async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ToAsync(
        IEnumerable<LatticeSchemaDeadLetterEntry> entries)
    {
        foreach (var entry in entries)
        {
            yield return entry;
        }

        await Task.CompletedTask;
    }
}
