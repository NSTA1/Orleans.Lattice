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
/// The WAL append-fence capability gate (epic #5605): open only while every
/// active silo advertises <see cref="IWalAppendFenceCapable"/>, and not
/// ratcheted - it closes again by itself when a silo running an older build
/// joins, and reopens once it leaves.
/// </summary>
[TestFixture]
public sealed class ClusterManifestWalAppendFenceGateTests
{
    private static readonly SiloAddress SiloA = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 1);
    private static readonly SiloAddress SiloB = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11112), 1);
    private static readonly SiloAddress OldSilo = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11113), 1);

    private static ClusterManifestWalAppendFenceGate Gate(
        IClusterManifestProvider manifests,
        IClusterMembershipService membership,
        out GrainInterfaceType marker)
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var resolver = new GrainInterfaceTypeResolver(
            Array.Empty<IGrainInterfaceTypeProvider>(),
            services.GetRequiredService<TypeConverter>());
        marker = resolver.GetGrainInterfaceType(typeof(IWalAppendFenceCapable));
        return new ClusterManifestWalAppendFenceGate(
            manifests, membership, resolver, NullLogger<ClusterManifestWalAppendFenceGate>.Instance);
    }

    private static ClusterManifest Cluster(int version, GrainInterfaceType marker, params (SiloAddress Silo, bool Capable)[] silos) => new(
        new MajorMinorVersion(1, version),
        silos.ToImmutableDictionary(
            s => s.Silo,
            s => new GrainManifest(
                ImmutableDictionary<GrainType, GrainProperties>.Empty,
                s.Capable
                    ? ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty.Add(
                        marker, new GrainInterfaceProperties(ImmutableDictionary.Create<string, string>(StringComparer.Ordinal)))
                    : ImmutableDictionary<GrainInterfaceType, GrainInterfaceProperties>.Empty)));

    private static ClusterMembershipSnapshot Members(int version, params (SiloAddress Silo, SiloStatus Status)[] members) => new(
        members.ToImmutableDictionary(m => m.Silo, m => new ClusterMember(m.Silo, m.Status, m.Silo.ToString())),
        new MembershipVersion(version));

    [Test]
    public void The_marker_is_a_distinct_grain_interface_from_the_clock_floor_marker()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var resolver = new GrainInterfaceTypeResolver(
            Array.Empty<IGrainInterfaceTypeProvider>(),
            services.GetRequiredService<TypeConverter>());
        _ = Gate(Substitute.For<IClusterManifestProvider>(), Substitute.For<IClusterMembershipService>(), out var marker);

        Assert.That(marker, Is.Not.EqualTo(resolver.GetGrainInterfaceType(typeof(IWalClockFloorCapable))),
            "the append fence must not open on silos that only advertise the clock-floor capability");
    }

    [Test]
    public void A_cached_verdict_is_read_without_the_gate_lock()
    {
        // #5605 review, S3: every leaf append on the silo reads the gate, so a cached
        // verdict must not take the silo-wide lock. Hold it from another thread; a
        // read whose versions have not moved must still answer.
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var gate = Gate(manifests, membership, out var marker);
        manifests.Current.Returns(Cluster(1, marker, (SiloA, true)));
        membership.CurrentSnapshot.Returns(Members(1, (SiloA, SiloStatus.Active)));
        Assert.That(gate.IsOpen, Is.True, "precondition: the verdict is evaluated and cached");

        var lockObject = typeof(ClusterManifestWalAppendFenceGate)
            .GetField("_gate", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(gate)!;
        using var held = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var holder = Task.Factory.StartNew(() =>
        {
            lock (lockObject)
            {
                held.Set();
                release.Wait();
            }
        }, TaskCreationOptions.LongRunning);
        held.Wait();
        try
        {
            var read = Task.Run(() => gate.IsOpen);
            Assert.That(read.Wait(TimeSpan.FromSeconds(10)), Is.True, "a cached read must not wait for the lock");
            Assert.That(read.Result, Is.True);
        }
        finally
        {
            release.Set();
            holder.Wait();
        }
    }

    [Test]
    public void Opens_when_every_active_silo_advertises_the_marker()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var gate = Gate(manifests, membership, out var marker);
        manifests.Current.Returns(Cluster(1, marker, (SiloA, true), (SiloB, true)));
        membership.CurrentSnapshot.Returns(Members(1, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)));

        Assert.That(gate.IsOpen, Is.True);
    }

    [Test]
    public void Stays_closed_while_an_active_silo_manifest_has_not_arrived()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var gate = Gate(manifests, membership, out var marker);
        manifests.Current.Returns(Cluster(1, marker, (SiloA, true)));
        membership.CurrentSnapshot.Returns(Members(1, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)));

        Assert.That(gate.IsOpen, Is.False, "a member whose manifest has not arrived counts as not capable");
    }

    [Test]
    public void Closes_again_when_an_older_build_silo_joins_and_reopens_when_it_leaves()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var gate = Gate(manifests, membership, out var marker);
        manifests.Current.Returns(Cluster(1, marker, (SiloA, true), (SiloB, true)));
        membership.CurrentSnapshot.Returns(Members(1, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)));
        var openBefore = gate.IsOpen;

        // A 10.2.0 silo joins (or rejoins in a rollback): it advertises no marker.
        manifests.Current.Returns(Cluster(2, marker, (SiloA, true), (SiloB, true), (OldSilo, false)));
        membership.CurrentSnapshot.Returns(Members(2, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active), (OldSilo, SiloStatus.Active)));
        var openWithOldSilo = gate.IsOpen;

        // It leaves again.
        membership.CurrentSnapshot.Returns(Members(3, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active), (OldSilo, SiloStatus.Dead)));
        var openAfterItLeft = gate.IsOpen;

        Assert.Multiple(() =>
        {
            Assert.That(openBefore, Is.True, "precondition: every silo was capable");
            Assert.That(openWithOldSilo, Is.False, "the gate is not ratcheted: an older silo joining closes it");
            Assert.That(openAfterItLeft, Is.True, "the gate reopens by itself once the older silo has left");
        });
    }

    [Test]
    public void The_pre_arm_class_opens_only_when_every_active_silo_also_advertises_its_marker_and_closes_again()
    {
        // #5833: the pre-arm class closes again if a silo with the fence but without the
        // pre-arm class joins, so no shard is sent a stamp it reads as an unloaded entry.
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var resolver = new GrainInterfaceTypeResolver(
            Array.Empty<IGrainInterfaceTypeProvider>(),
            services.GetRequiredService<TypeConverter>());
        var fence = resolver.GetGrainInterfaceType(typeof(IWalAppendFenceCapable));
        var preArm = resolver.GetGrainInterfaceType(typeof(IWalAppendFencePreArmCapable));
        var gate = new ClusterManifestWalAppendFenceGate(
            manifests, membership, resolver, NullLogger<ClusterManifestWalAppendFenceGate>.Instance);
        static GrainManifest Advertising(params GrainInterfaceType[] markers) => new(
            ImmutableDictionary<GrainType, GrainProperties>.Empty,
            markers.ToImmutableDictionary(
                m => m,
                _ => new GrainInterfaceProperties(ImmutableDictionary.Create<string, string>(StringComparer.Ordinal))));

        manifests.Current.Returns(new ClusterManifest(
            new MajorMinorVersion(1, 1),
            ImmutableDictionary<SiloAddress, GrainManifest>.Empty.Add(SiloA, Advertising(fence, preArm)).Add(SiloB, Advertising(fence, preArm))));
        membership.CurrentSnapshot.Returns(Members(1, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)));
        var openBoth = (gate.IsOpen, gate.IsPreArmOpen);

        manifests.Current.Returns(new ClusterManifest(
            new MajorMinorVersion(1, 2),
            ImmutableDictionary<SiloAddress, GrainManifest>.Empty.Add(SiloA, Advertising(fence, preArm)).Add(SiloB, Advertising(fence, preArm)).Add(OldSilo, Advertising(fence))));
        membership.CurrentSnapshot.Returns(Members(2, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active), (OldSilo, SiloStatus.Active)));
        var withFenceOnlySilo = (gate.IsOpen, gate.IsPreArmOpen);

        manifests.Current.Returns(new ClusterManifest(
            new MajorMinorVersion(1, 3),
            ImmutableDictionary<SiloAddress, GrainManifest>.Empty.Add(SiloA, Advertising(preArm)).Add(SiloB, Advertising(fence, preArm))));
        membership.CurrentSnapshot.Returns(Members(3, (SiloA, SiloStatus.Active), (SiloB, SiloStatus.Active)));
        var preArmWithoutFence = (gate.IsOpen, gate.IsPreArmOpen);

        Assert.Multiple(() =>
        {
            Assert.That(openBoth, Is.EqualTo((true, true)));
            Assert.That(withFenceOnlySilo, Is.EqualTo((true, false)), "a silo without the pre-arm class did not close it");
            Assert.That(preArmWithoutFence, Is.EqualTo((false, false)), "the pre-arm class opened without the fence itself");
        });
    }

    [Test]
    public void Closes_when_a_joining_silo_is_active_before_its_manifest_arrives()
    {
        var manifests = Substitute.For<IClusterManifestProvider>();
        var membership = Substitute.For<IClusterMembershipService>();
        var gate = Gate(manifests, membership, out var marker);
        manifests.Current.Returns(Cluster(1, marker, (SiloA, true)));
        membership.CurrentSnapshot.Returns(Members(1, (SiloA, SiloStatus.Active)));
        var before = gate.IsOpen;

        membership.CurrentSnapshot.Returns(Members(2, (SiloA, SiloStatus.Active), (OldSilo, SiloStatus.Active)));

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.True);
            Assert.That(gate.IsOpen, Is.False, "a membership change alone invalidates the cached verdict");
        });
    }
}
