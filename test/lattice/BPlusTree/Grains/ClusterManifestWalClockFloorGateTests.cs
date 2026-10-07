using System.Collections.Immutable;
using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Metadata;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Serialization.TypeSystem;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The WAL clock-floor capability gate (issue #4586): open only while every
/// active silo's grain manifest advertises <see cref="IWalClockFloorCapable"/>.
/// </summary>
[TestFixture]
public sealed class ClusterManifestWalClockFloorGateTests
{
    private static readonly GrainInterfaceType Marker = GrainInterfaceType.Create("ol.wfc");
    private static readonly SiloAddress SiloA = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 1);
    private static readonly SiloAddress SiloB = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11112), 1);

    private static GrainManifest Manifest(bool capable) => new(
        ImmutableDictionary<GrainType, GrainProperties>.Empty,
        capable
            ? ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty.Add(
                Marker, new GrainInterfaceProperties(ImmutableDictionary.Create<string, string>(StringComparer.Ordinal)))
            : ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty);

    private static ClusterManifest Cluster(params (SiloAddress Silo, bool Capable)[] silos) => new(
        new MajorMinorVersion(1, 1),
        silos.ToImmutableDictionary(s => s.Silo, s => Manifest(s.Capable)));

    private static ClusterMembershipSnapshot Members(params (SiloAddress Silo, SiloStatus Status)[] members) => new(
        members.ToImmutableDictionary(m => m.Silo, m => new ClusterMember(m.Silo, m.Status, m.Silo.ToString())),
        new MembershipVersion(1));

    [Test]
    public void Opens_when_every_active_silo_is_capable()
    {
        Assert.That(ClusterManifestWalClockFloorGate.Evaluate(
            Cluster((SiloA, true), (SiloB, true)),
            Members((SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)),
            Marker), Is.True);
    }

    [Test]
    public void Stays_closed_while_any_active_silo_runs_an_older_build()
    {
        Assert.That(ClusterManifestWalClockFloorGate.Evaluate(
            Cluster((SiloA, true), (SiloB, false)),
            Members((SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)),
            Marker), Is.False);
    }

    [Test]
    public void Stays_closed_while_an_active_silo_manifest_has_not_arrived()
    {
        Assert.That(ClusterManifestWalClockFloorGate.Evaluate(
            Cluster((SiloA, true)),
            Members((SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)),
            Marker), Is.False);
    }

    [Test]
    public void Ignores_members_that_are_not_active()
    {
        Assert.That(ClusterManifestWalClockFloorGate.Evaluate(
            Cluster((SiloA, true), (SiloB, false)),
            Members((SiloA, SiloStatus.Active), (SiloB, SiloStatus.Dead)),
            Marker), Is.True);
    }

    [Test]
    public void Stays_closed_with_no_active_member()
    {
        Assert.That(ClusterManifestWalClockFloorGate.Evaluate(
            Cluster((SiloA, true)),
            Members((SiloA, SiloStatus.Joining)),
            Marker), Is.False);
    }

    /// <summary>
    /// Builds the production gate over substitute manifest and membership views,
    /// and returns the marker interface type the gate resolves, so the manifests
    /// can advertise exactly what it looks for.
    /// </summary>
    private static ClusterManifestWalClockFloorGate Gate(
        IClusterManifestProvider manifests,
        IClusterMembershipService membership,
        out GrainInterfaceType marker)
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var resolver = new GrainInterfaceTypeResolver(
            Array.Empty<IGrainInterfaceTypeProvider>(),
            services.GetRequiredService<TypeConverter>());
        marker = resolver.GetGrainInterfaceType(typeof(IWalClockFloorCapable));
        return new ClusterManifestWalClockFloorGate(
            manifests, membership, resolver, NullLogger<ClusterManifestWalClockFloorGate>.Instance);
    }

    private static ClusterManifest Cluster(MajorMinorVersion version, GrainInterfaceType marker, params (SiloAddress Silo, bool Capable)[] silos) => new(
        version,
        silos.ToImmutableDictionary(
            s => s.Silo,
            s => new GrainManifest(
                ImmutableDictionary<GrainType, GrainProperties>.Empty,
                s.Capable
                    ? ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty.Add(
                        marker, new GrainInterfaceProperties(ImmutableDictionary.Create<string, string>(StringComparer.Ordinal)))
                    : ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty)));

    [Test]
    public void IsOpen_stays_closed_while_any_active_silo_runs_an_older_build()
    {
        // The gate's production consumer reads IsOpen, not Evaluate: a gate that
        // answered open without consulting the manifests would let a WAL
        // partition advance a clock floor an older-build silo cannot honour.
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var gate = Gate(manifests, membership, out var marker);
        manifests.Current.Returns(Cluster(new MajorMinorVersion(1, 1), marker, (SiloA, true), (SiloB, false)));
        membership.CurrentSnapshot.Returns(Members((SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)));

        Assert.That(gate.IsOpen, Is.False);
    }

    [Test]
    public void IsOpen_opens_once_the_last_older_build_silo_advertises_the_marker()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var gate = Gate(manifests, membership, out var marker);
        membership.CurrentSnapshot.Returns(Members((SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)));
        manifests.Current.Returns(Cluster(new MajorMinorVersion(1, 1), marker, (SiloA, true), (SiloB, false)));
        var before = gate.IsOpen;

        // A newer manifest version invalidates the cached verdict.
        manifests.Current.Returns(Cluster(new MajorMinorVersion(1, 2), marker, (SiloA, true), (SiloB, true)));

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.False, "precondition: closed while SiloB runs the older build");
            Assert.That(gate.IsOpen, Is.True, "the verdict is re-evaluated against the newer manifest");
        });
    }
}