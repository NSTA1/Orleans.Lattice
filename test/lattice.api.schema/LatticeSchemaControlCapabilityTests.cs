using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Coverage for the read-only capability probe on the
/// <see cref="LatticeSchemaControl"/> facade: <c>ProbeCapabilitiesAsync</c>
/// evaluates the caller's read and schema-admin authority for a tree with no side
/// effects and never throws on a denial - a denied capability is reported as a
/// <c>false</c> flag (default-deny). The read-flag group reflects the read grant;
/// the manage-flag group reflects the schema-admin grant.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaControlCapabilityTests
{
    private const string Tree = "orders";

    private static LatticeSchemaControl Create(ILatticeAccessGate gate)
    {
        var services = new ServiceCollection().BuildServiceProvider();
        return new LatticeSchemaControl(
            Substitute.For<ILatticeSchemaAdmin>(),
            Substitute.For<ILatticeSchemaRemediationAdmin>(),
            Substitute.For<ILatticeSchemaComplianceAdmin>(),
            new SchemaAccessAuthorizer(gate),
            Options.Create(new LatticeApiSchemaOptions()),
            services,
            new DefaultTenantContextResolver());
    }

    [Test]
    public async Task ProbeCapabilities_all_granted_reports_every_capability()
    {
        var control = Create(RecordingAccessGate.Allow());

        var caps = await control.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.TreeId, Is.EqualTo(Tree));
            Assert.That(caps.CanViewPolicy, Is.True);
            Assert.That(caps.CanViewDeadLetters, Is.True);
            Assert.That(caps.CanViewVersionConfig, Is.True);
            Assert.That(caps.CanViewRemediationStatus, Is.True);
            Assert.That(caps.CanScanCompliance, Is.True);
            Assert.That(caps.CanManagePolicy, Is.True);
            Assert.That(caps.CanManageVersion, Is.True);
            Assert.That(caps.CanRemediate, Is.True);
        });
    }

    [Test]
    public async Task ProbeCapabilities_fully_denied_reports_no_capabilities_without_throwing()
    {
        var control = Create(RecordingAccessGate.Deny());

        var caps = await control.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.TreeId, Is.EqualTo(Tree));
            Assert.That(caps.CanViewPolicy, Is.False);
            Assert.That(caps.CanViewDeadLetters, Is.False);
            Assert.That(caps.CanViewVersionConfig, Is.False);
            Assert.That(caps.CanViewRemediationStatus, Is.False);
            Assert.That(caps.CanScanCompliance, Is.False);
            Assert.That(caps.CanManagePolicy, Is.False);
            Assert.That(caps.CanManageVersion, Is.False);
            Assert.That(caps.CanRemediate, Is.False);
        });
    }

    [Test]
    public async Task ProbeCapabilities_read_only_grant_reports_reads_true_and_mutations_false()
    {
        var control = Create(new OperationScopedGate(LatticeOperation.Read));

        var caps = await control.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.CanViewPolicy, Is.True);
            Assert.That(caps.CanViewDeadLetters, Is.True);
            Assert.That(caps.CanViewVersionConfig, Is.True);
            Assert.That(caps.CanViewRemediationStatus, Is.True);
            Assert.That(caps.CanScanCompliance, Is.True);
            Assert.That(caps.CanManagePolicy, Is.False);
            Assert.That(caps.CanManageVersion, Is.False);
            Assert.That(caps.CanRemediate, Is.False);
        });
    }

    [Test]
    public async Task ProbeCapabilities_manage_only_grant_reports_mutations_true_and_reads_false()
    {
        var control = Create(new OperationScopedGate(LatticeOperation.SchemaAdmin));

        var caps = await control.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.CanViewPolicy, Is.False);
            Assert.That(caps.CanScanCompliance, Is.False);
            Assert.That(caps.CanManagePolicy, Is.True);
            Assert.That(caps.CanManageVersion, Is.True);
            Assert.That(caps.CanRemediate, Is.True);
        });
    }

    [Test]
    public void ProbeCapabilities_null_or_empty_tree_id_throws()
    {
        var control = Create(RecordingAccessGate.Allow());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await control.ProbeCapabilitiesAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await control.ProbeCapabilitiesAsync(""), Throws.ArgumentException);
        });
    }
}
