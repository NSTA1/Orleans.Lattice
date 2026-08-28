using System.Reflection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Concrete buffer-ownership guard for the core <c>Orleans.Lattice</c> assembly.
/// Registers one specimen per CRDT primitive so
/// <see cref="CrdtBufferOwnershipContractTestsBase"/> can assert every leg of the
/// contract documented on <see cref="ICrdt{TSelf}"/> structurally, by walking the
/// real object graph and comparing <c>byte[]</c> instances by reference.
/// <para>
/// The point of registering <em>every</em> primitive - including the ones that
/// hold no <c>byte[]</c> at all - is that the base fails when a declared CRDT type
/// has no specimen, and when a payload-free claim stops being true. The set
/// primitives are payload-free by design (they encode elements as base64 strings
/// and never retain a caller array); pinning that here means a future change to
/// raw <c>byte[]</c> storage cannot silently inherit the aliasing bugs the
/// register and sequence primitives each had to be fixed for.
/// </para>
/// </summary>
[TestFixture]
public sealed class CrdtBufferOwnershipContractTests : CrdtBufferOwnershipContractTestsBase
{
    private static byte[] Bytes(params byte[] values) => values;

    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(Rga).Assembly;

    /// <inheritdoc />
    protected override Type CrdtInterfaceType => typeof(ICrdt<>);

    /// <inheritdoc />
    protected override IReadOnlyList<CrdtOwnershipSpecimen> Specimens { get; } =
    [
        new(
            typeof(Rga),
            CreatePopulated: static () =>
            {
                var rga = new Rga();
                var first = rga.InsertAfter(Rga.Root, "A", Bytes(1, 2, 3));
                rga.InsertAfter(first, "A", Bytes(4, 5, 6));
                return rga;
            },
            CreateEmpty: static () => new Rga(),
            Projections:
            [
                new(nameof(Rga.ToList), static state => ((Rga)state).ToList().Select(static e => e.Value)),
            ],
            CreateDeltaFrom: static state =>
            {
                var rga = (Rga)state;
                var node = rga.Nodes[0];
                return new RgaDelta
                {
                    Inserts = [new RgaDeltaNode
                    {
                        ReplicaId = node.ReplicaId,
                        Counter = node.Counter,
                        ParentDot = node.ParentDot,
                        Value = node.Value,
                    }],
                    Tombstones = Array.Empty<OrSetDot>(),
                };
            },
            ApplyDelta: static (receiver, delta) => ((Rga)receiver).MergeDelta((RgaDelta)delta)),

        new(
            typeof(MvRegister),
            CreatePopulated: static () =>
            {
                var register = new MvRegister();
                register.Set("A", Bytes(1, 2, 3));
                return register;
            },
            CreateEmpty: static () => new MvRegister(),
            Projections:
            [
                new(nameof(MvRegister.Values), static state => ((MvRegister)state).Values()),
            ],
            CreateDeltaFrom: static state =>
            {
                var register = (MvRegister)state;
                return new MvRegisterDelta
                {
                    Entries = register.Entries.ToList(),
                    Context = new Dictionary<string, long>(register.Context),
                };
            },
            ApplyDelta: static (receiver, delta) => ((MvRegister)receiver).MergeDelta((MvRegisterDelta)delta)),

        new(
            typeof(BoundedRegister),
            CreatePopulated: static () =>
            {
                var register = BoundedRegister.CreateEmpty(isMin: false);
                register.Set(Bytes(1, 2, 3), Bytes(9));
                return register;
            },
            CreateEmpty: static () => BoundedRegister.CreateEmpty(isMin: false),
            Projections: [],
            CreateDeltaFrom: static state =>
            {
                var register = (BoundedRegister)state;
                return new BoundedRegisterDelta
                {
                    Value = register.Value,
                    OrderKey = register.OrderKey,
                    HasValue = true,
                };
            },
            ApplyDelta: static (receiver, delta) => ((BoundedRegister)receiver).MergeDelta((BoundedRegisterDelta)delta)),

        // A composite over a byte[]-carrying value: the leg that has drawn blood
        // twice (OrMap.Clone, OrMap.Get). One contributor exercises Get's
        // single-entry Clone fast path; the multi-contributor specimen below
        // exercises the fold path that seeds from a clone and merges the rest.
        new(
            typeof(OrMap<string, Rga>),
            CreatePopulated: static () =>
            {
                var map = new OrMap<string, Rga>();
                var value = new Rga();
                value.InsertAfter(Rga.Root, "A", Bytes(1, 2, 3));
                map.Set("k", "A", value);
                return map;
            },
            CreateEmpty: static () => new OrMap<string, Rga>(),
            Projections:
            [
                new("Get", static state => ((OrMap<string, Rga>)state).Get("k")!.Nodes.Select(static n => n.Value)),
            ],
            CreateDeltaFrom: static state =>
            {
                var value = ((OrMap<string, Rga>)state).Get("k")!;
                return new OrMapDelta<string, Rga>
                {
                    Adds = [new OrMapDeltaEntry<string, Rga> { Key = "k", ReplicaId = "A", Counter = 1, Value = value }],
                    Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
                };
            },
            ApplyDelta: static (receiver, delta) =>
                ((OrMap<string, Rga>)receiver).MergeDelta((OrMapDelta<string, Rga>)delta),
            Label: "single contributor"),

        new(
            typeof(OrMap<string, MvRegister>),
            CreatePopulated: static () =>
            {
                var map = new OrMap<string, MvRegister>();
                var a = new MvRegister();
                a.Set("A", Bytes(1, 2, 3));
                map.Set("k", "A", a);

                var peer = new OrMap<string, MvRegister>();
                var b = new MvRegister();
                b.Set("B", Bytes(4, 5, 6));
                peer.Set("k", "B", b);

                map.MergeFrom(peer);
                return map;
            },
            CreateEmpty: static () => new OrMap<string, MvRegister>(),
            Projections:
            [
                new("Get", static state => ((OrMap<string, MvRegister>)state).Get("k")!.Values()),
            ],
            Label: "multiple contributors"),

        // The set primitives retain no caller byte[]: an element is base64-encoded
        // into a string key on the way in and decoded fresh on the way out.
        new(
            typeof(GSet),
            CreatePopulated: static () =>
            {
                var set = new GSet();
                set.Add(Bytes(1, 2, 3));
                return set;
            },
            CreateEmpty: static () => new GSet(),
            Projections:
            [
                new(nameof(GSet.Values), static state => ((GSet)state).Values()),
            ],
            CreateDeltaFrom: static _ => new GSetDelta { Adds = [Bytes(7, 8, 9)] },
            ApplyDelta: static (receiver, delta) => ((GSet)receiver).MergeDelta((GSetDelta)delta),
            PayloadFree: true),

        new(
            typeof(OrSet),
            CreatePopulated: static () =>
            {
                var set = new OrSet();
                set.Add(Bytes(1, 2, 3), "A", 1);
                return set;
            },
            CreateEmpty: static () => new OrSet(),
            Projections:
            [
                new(nameof(OrSet.Elements), static state => ((OrSet)state).Elements()),
            ],
            CreateDeltaFrom: static _ => new OrSetDelta
            {
                Adds = [new OrSetDeltaDot { Element = Bytes(7, 8, 9), ReplicaId = "P", Counter = 1 }],
                Removes = Array.Empty<OrSetDeltaDot>(),
            },
            ApplyDelta: static (receiver, delta) => ((OrSet)receiver).MergeDelta((OrSetDelta)delta),
            PayloadFree: true),

        new(
            typeof(RwSet),
            CreatePopulated: static () =>
            {
                var set = new RwSet();
                set.Add(Bytes(1, 2, 3), "A", 1);
                return set;
            },
            CreateEmpty: static () => new RwSet(),
            Projections:
            [
                new(nameof(RwSet.Elements), static state => ((RwSet)state).Elements()),
            ],
            CreateDeltaFrom: static _ => new RwSetDelta
            {
                Adds = [new OrSetDeltaDot { Element = Bytes(7, 8, 9), ReplicaId = "P", Counter = 1 }],
                Removes = Array.Empty<OrSetDeltaDot>(),
                Tombstones = Array.Empty<OrSetDeltaDot>(),
            },
            ApplyDelta: static (receiver, delta) => ((RwSet)receiver).MergeDelta((RwSetDelta)delta),
            PayloadFree: true),

        // Counters, flags and the frontier carry no opaque payload at all.
        new(
            typeof(GCounter),
            CreatePopulated: static () =>
            {
                var counter = new GCounter();
                counter.Increment("A");
                return counter;
            },
            CreateEmpty: static () => new GCounter(),
            Projections: [],
            CreateDeltaFrom: static _ => new GCounterDelta { Increments = new Dictionary<string, long> { ["P"] = 1 } },
            ApplyDelta: static (receiver, delta) => ((GCounter)receiver).MergeDelta((GCounterDelta)delta),
            PayloadFree: true),

        new(
            typeof(PnCounter),
            CreatePopulated: static () =>
            {
                var counter = new PnCounter();
                counter.Increment("A");
                counter.Decrement("B");
                return counter;
            },
            CreateEmpty: static () => new PnCounter(),
            Projections: [],
            CreateDeltaFrom: static _ => new PnCounterDelta
            {
                Increments = new Dictionary<string, long> { ["P"] = 1 },
                Decrements = new Dictionary<string, long> { ["P"] = 1 },
            },
            ApplyDelta: static (receiver, delta) => ((PnCounter)receiver).MergeDelta((PnCounterDelta)delta),
            PayloadFree: true),

        new(
            typeof(OrFlag),
            CreatePopulated: static () =>
            {
                var flag = new OrFlag();
                flag.Enable("A", 1);
                return flag;
            },
            CreateEmpty: static () => new OrFlag(),
            Projections: [],
            CreateDeltaFrom: static _ => new OrFlagDelta
            {
                Enables = [new OrSetDot { ReplicaId = "P", Counter = 1 }],
                Disables = Array.Empty<OrSetDot>(),
            },
            ApplyDelta: static (receiver, delta) => ((OrFlag)receiver).MergeDelta((OrFlagDelta)delta),
            PayloadFree: true),

        new(
            typeof(RwFlag),
            CreatePopulated: static () =>
            {
                var flag = new RwFlag();
                flag.Enable("A", 1);
                return flag;
            },
            CreateEmpty: static () => new RwFlag(),
            Projections: [],
            CreateDeltaFrom: static _ => new RwFlagDelta
            {
                Enables = [new OrSetDot { ReplicaId = "P", Counter = 1 }],
                Disables = Array.Empty<OrSetDot>(),
                Tombstones = Array.Empty<OrSetDot>(),
            },
            ApplyDelta: static (receiver, delta) => ((RwFlag)receiver).MergeDelta((RwFlagDelta)delta),
            PayloadFree: true),

        new(
            typeof(VersionVector),
            CreatePopulated: static () =>
            {
                var vector = new VersionVector();
                vector.Tick("A");
                return vector;
            },
            CreateEmpty: static () => new VersionVector(),
            Projections: [],
            CreateDeltaFrom: static state => new VersionVectorDelta
            {
                Entries = new Dictionary<string, HybridLogicalClock>(((VersionVector)state).Entries),
            },
            ApplyDelta: static (receiver, delta) => ((VersionVector)receiver).MergeDelta((VersionVectorDelta)delta),
            PayloadFree: true),
    ];
}
