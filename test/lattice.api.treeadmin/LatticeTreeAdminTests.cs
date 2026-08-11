using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTreeAdmin"/>: the scaffolding foundation facade
/// owns no admin plane of its own and, following composition over absorption, wraps
/// the schema control facade (<see cref="ILatticeSchemaControl"/>) by delegation.
/// The only operation at this stage is the capability probe, which composes the
/// wrapped facade's own probe and reports whole-tree admin authority default-deny.
/// Driven purely with a substitute - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminTests
{
    private const string Tree = "orders";

    private static LatticeTreeAdmin Create(ILatticeSchemaControl schemaControl)
        => new(schemaControl, Options.Create(new LatticeApiTreeAdminOptions()));

    private static LatticeSchemaCapabilities SchemaCaps(string tree, bool granted) => new()
    {
        TreeId = tree,
        CanViewPolicy = granted,
        CanViewDeadLetters = granted,
        CanViewVersionConfig = granted,
        CanViewRemediationStatus = granted,
        CanScanCompliance = granted,
        CanManagePolicy = granted,
        CanManageVersion = granted,
        CanRemediate = granted,
    };

    [Test]
    public async Task ProbeCapabilitiesAsync_delegates_to_schema_control_and_composes_result()
    {
        var schemaControl = Substitute.For<ILatticeSchemaControl>();
        var schemaCaps = SchemaCaps(Tree, granted: true);
        schemaControl.ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>()).Returns(schemaCaps);
        var facade = Create(schemaControl);

        var caps = await facade.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.TreeId, Is.EqualTo(Tree));
            Assert.That(caps.Schema, Is.SameAs(schemaCaps));
            // Scaffolding stage: no whole-tree admin gate exists yet, so the flag is
            // reported default-deny regardless of the composed schema grant.
            Assert.That(caps.CanAdministerTree, Is.False);
        });
        await schemaControl.Received(1).ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_embeds_denied_schema_capabilities_without_throwing()
    {
        var schemaControl = Substitute.For<ILatticeSchemaControl>();
        schemaControl.ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(SchemaCaps(Tree, granted: false));
        var facade = Create(schemaControl);

        var caps = await facade.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.Schema.CanViewPolicy, Is.False);
            Assert.That(caps.Schema.CanManagePolicy, Is.False);
            Assert.That(caps.CanAdministerTree, Is.False);
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_flows_cancellation_token_to_schema_control()
    {
        var schemaControl = Substitute.For<ILatticeSchemaControl>();
        schemaControl.ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(SchemaCaps(Tree, granted: true));
        var facade = Create(schemaControl);
        using var cts = new CancellationTokenSource();

        await facade.ProbeCapabilitiesAsync(Tree, cts.Token);

        await schemaControl.Received(1).ProbeCapabilitiesAsync(Tree, cts.Token);
    }

    [Test]
    public void ProbeCapabilitiesAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.ProbeCapabilitiesAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ProbeCapabilitiesAsync(""), Throws.ArgumentException);
        });
    }

    [Test]
    public void Constructor_null_dependencies_throw()
    {
        var schemaControl = Substitute.For<ILatticeSchemaControl>();
        var options = Options.Create(new LatticeApiTreeAdminOptions());

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTreeAdmin(null!, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTreeAdmin(schemaControl, null!), Throws.ArgumentNullException);
        });
    }
}
