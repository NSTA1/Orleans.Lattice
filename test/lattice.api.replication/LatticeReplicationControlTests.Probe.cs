using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Replication.Tests.PeerStatus;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests;

/// <summary>
/// <see cref="LatticeReplicationControl.ProbeCapabilitiesAsync"/> (issue #5478): it
/// reports the Replication capability through the same gate enable and disable use,
/// turns a denial into a <see langword="false"/> flag, never consults the engine, and
/// probes the same tenant-scoped tree an enable of that name would act on.
/// </summary>
public sealed partial class LatticeReplicationControlTests
{
    [Test]
    public async Task ProbeCapabilitiesAsync_reports_an_authorized_caller_without_touching_the_engine()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        var gate = new AllowingAccessGate();
        var control = CreateControl(authority, gate);

        var capabilities = await control.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(capabilities.TreeId, Is.EqualTo(Tree));
            Assert.That(capabilities.CanConfigure, Is.True);
            Assert.That(gate.AuthorizedTrees, Is.EqualTo(new[] { Tree }));
            Assert.That(authority.ReceivedCalls().Any(), Is.False, "a probe has no side effects");
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_reports_a_denied_caller_as_false_rather_than_throwing()
    {
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        var control = CreateControl(authority, new DenyingAccessGate("no grant"));

        var capabilities = await control.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(capabilities.CanConfigure, Is.False);
            Assert.That(authority.ReceivedCalls().Any(), Is.False);
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_denies_an_anonymous_caller_by_default()
    {
        var control = CreateControl(Substitute.For<ILatticeReplicationConfigAuthority>(), new AnonymousDenyingAccessGate());

        var capabilities = await control.ProbeCapabilitiesAsync(Tree);

        Assert.That(capabilities.CanConfigure, Is.False);
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_probes_the_callers_tenant_scoped_tree()
    {
        var gate = new AllowingAccessGate();
        var control = new LatticeReplicationControl(
            Substitute.For<ILatticeReplicationConfigAuthority>(),
            new ReplicationAccessAuthorizer(gate, membership: null),
            FixedTenantContextResolver.For("acme"));

        var capabilities = await control.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(capabilities.TreeId, Is.EqualTo("t/acme/" + Tree));
            Assert.That(gate.AuthorizedTrees, Is.EqualTo(new[] { "t/acme/" + Tree }));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void ProbeCapabilitiesAsync_rejects_a_missing_tree_id(string? treeId)
    {
        var control = CreateControl(Substitute.For<ILatticeReplicationConfigAuthority>(), new AllowingAccessGate());

        Assert.That(async () => await control.ProbeCapabilitiesAsync(treeId!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReplicationCapabilities_requires_a_tree_id_and_keeps_its_flag()
    {
        var capabilities = new ReplicationCapabilities(Tree, canConfigure: true);

        Assert.Multiple(() =>
        {
            Assert.That(() => new ReplicationCapabilities(null!, canConfigure: false), Throws.ArgumentNullException);
            Assert.That(capabilities.TreeId, Is.EqualTo(Tree));
            Assert.That(capabilities.CanConfigure, Is.True);
            Assert.That(capabilities with { CanConfigure = false }, Is.Not.EqualTo(capabilities));
        });
    }
}
