using System.Collections.Immutable;
using System.Net;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Metadata;
using Orleans.Runtime;

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
}
