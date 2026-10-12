using System.Collections.Immutable;
using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Views;
using Orleans.Metadata;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Serialization.TypeSystem;

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class ClusterManifestViewJointRepairAdmissionGateTests
{
    private static readonly SiloAddress Current = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 1);
    private static readonly SiloAddress Peer = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11112), 1);

    private static ClusterManifest Manifest(
        GrainInterfaceType marker,
        long minorVersion,
        params (SiloAddress Address, bool Capable)[] silos) => new(
            new MajorMinorVersion(1, minorVersion),
            silos.ToImmutableDictionary(silo => silo.Address, silo => new GrainManifest(
                ImmutableDictionary<GrainType, GrainProperties>.Empty,
                silo.Capable
                    ? ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty.Add(
                        marker, new GrainInterfaceProperties(ImmutableDictionary.Create<string, string>(StringComparer.Ordinal)))
                    : ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty)));

    private static ClusterMembershipSnapshot Members(
        long version,
        params (SiloAddress Address, SiloStatus Status)[] members) => new(
            members.ToImmutableDictionary(member => member.Address,
                member => new ClusterMember(member.Address, member.Status, member.Address.ToString())),
            new MembershipVersion(version));

    private static (ClusterManifestViewJointRepairAdmissionGate Gate, GrainInterfaceType Marker) CreateGate(
        IClusterManifestProvider manifests,
        IClusterMembershipService membership)
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var resolver = new GrainInterfaceTypeResolver(
            Array.Empty<IGrainInterfaceTypeProvider>(), services.GetRequiredService<TypeConverter>());
        return (new ClusterManifestViewJointRepairAdmissionGate(
            manifests, membership, resolver, NullLogger<ClusterManifestViewJointRepairAdmissionGate>.Instance),
            resolver.GetGrainInterfaceType(typeof(IViewJointRepairCapable)));
    }

    [TestCase(SiloStatus.Active)]
    [TestCase(SiloStatus.Joining)]
    [TestCase(SiloStatus.ShuttingDown)]
    [TestCase(SiloStatus.Stopping)]
    public async Task IsOpenAsync_legacy_member_remains_closed_until_its_activation_lifetime_ends(SiloStatus status)
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var (gate, marker) = CreateGate(manifests, membership);
        manifests.Current.Returns(Manifest(marker, 1, (Current, true), (Peer, false)));
        membership.CurrentSnapshot.Returns(Members(1, (Current, SiloStatus.Active), (Peer, status)));

        Assert.That(await gate.IsOpenAsync(), Is.False);

        membership.CurrentSnapshot.Returns(Members(2, (Current, SiloStatus.Active), (Peer, SiloStatus.Dead)));
        Assert.That(await gate.IsOpenAsync(), Is.True,
            "Membership change must invalidate the closed verdict without manual migration or a host restart.");
    }

    [Test]
    public async Task IsOpenAsync_missing_manifest_fails_closed_and_manifest_arrival_opens_automatically()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var (gate, marker) = CreateGate(manifests, membership);
        membership.CurrentSnapshot.Returns(Members(1, (Current, SiloStatus.Active), (Peer, SiloStatus.Active)));
        manifests.Current.Returns(Manifest(marker, 1, (Current, true)));
        Assert.That(await gate.IsOpenAsync(), Is.False,
            "Active membership can arrive before its manifest; absence is not capability.");

        manifests.Current.Returns(Manifest(marker, 2, (Current, true), (Peer, true)));
        Assert.That(await gate.IsOpenAsync(), Is.True);
    }

    [Test]
    public async Task IsOpenAsync_old_member_join_closes_admission_even_with_unchanged_manifest_version()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var (gate, marker) = CreateGate(manifests, membership);
        manifests.Current.Returns(Manifest(marker, 1, (Current, true), (Peer, false)));
        membership.CurrentSnapshot.Returns(Members(1, (Current, SiloStatus.Active)));
        Assert.That(await gate.IsOpenAsync(), Is.True);

        membership.CurrentSnapshot.Returns(Members(2, (Current, SiloStatus.Active), (Peer, SiloStatus.Joining)));
        Assert.That(await gate.IsOpenAsync(), Is.False);
    }

    [Test]
    public async Task IsOpenAsync_recreated_gate_rederives_closed_membership_instead_of_remembering_prior_admission()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var (gate, marker) = CreateGate(manifests, membership);
        manifests.Current.Returns(Manifest(marker, 1, (Current, true), (Peer, true)));
        membership.CurrentSnapshot.Returns(Members(1, (Current, SiloStatus.Active), (Peer, SiloStatus.Active)));
        Assert.That(await gate.IsOpenAsync(), Is.True);

        manifests.Current.Returns(Manifest(marker, 2, (Current, true), (Peer, false)));
        var (restarted, _) = CreateGate(manifests, membership);
        Assert.That(await restarted.IsOpenAsync(), Is.False);
    }

    [Test]
    public async Task IsOpenAsync_no_active_member_is_not_a_vacuous_upgrade_success()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var (gate, marker) = CreateGate(manifests, membership);
        manifests.Current.Returns(Manifest(marker, 1, (Current, true)));
        membership.CurrentSnapshot.Returns(Members(1, (Current, SiloStatus.Joining)));

        Assert.That(await gate.IsOpenAsync(), Is.False);
    }

    [Test]
    public void IsOpenAsync_cancelled_request_does_not_issue_an_admission_verdict()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var (gate, _) = CreateGate(manifests, membership);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await gate.IsOpenAsync(cancellation.Token));
        Assert.That(manifests.ReceivedCalls(), Is.Empty);
        Assert.That(membership.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void AddLatticeViews_registers_required_cluster_admission_gate()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeViews();
        builder.AddLatticeViews();

        var registration = services.Single(descriptor => descriptor.ServiceType == typeof(IViewJointRepairAdmissionGate));
        Assert.Multiple(() =>
        {
            Assert.That(registration.ImplementationType, Is.EqualTo(typeof(ClusterManifestViewJointRepairAdmissionGate)));
            Assert.That(registration.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
        });
    }
}
