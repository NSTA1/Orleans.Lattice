using Grpc.Core;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the typed-CRDT half of <see cref="GrpcLatticeDataApi"/>: the
/// remote-host adapter that collapses every strongly-typed CRDT facade member
/// onto the two unified wire RPCs - <c>CrdtWrite</c>, discriminated by
/// <see cref="CrdtWriteOp"/>, and <c>CrdtRead</c>, discriminated by
/// <see cref="CrdtKind"/> - and re-projects the single carry-all read response
/// back to each member's explicit CLR shape.
/// </summary>
/// <remarks>
/// Because every write funnels through one request type, the discriminator and
/// the payload field each member populates are the whole contract: a member that
/// sent the wrong <see cref="CrdtWriteOp"/>, or put a value in
/// <c>Element</c> when the server reads <c>Amount</c>, would corrupt data while
/// still "succeeding". The write table below asserts the exact request each
/// member emits, and the read tests assert the projection out of the carry-all
/// response. Deterministic over a <see cref="FakeCallInvoker"/> - no wire.
/// </remarks>
[TestFixture]
public sealed class GrpcLatticeDataApiCrdtTests
{
    private const string Tree = "tree";
    private const string Key = "k";
    private const string Replica = "replica-a";

    private static readonly byte[] Payload = [1, 2, 3];

    private static GrpcLatticeDataApi Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.DataClient(invoker));

    private static FakeCallInvoker WriteInvoker()
        => new(_ => new CrdtWriteResponse());

    private static FakeCallInvoker ReadInvoker(CrdtReadResponse response)
        => new(_ => response);

    /// <summary>
    /// One case per typed-CRDT write member: the call to make, the wire op it
    /// must select, and an assertion over the request it emitted.
    /// </summary>
    private static IEnumerable<TestCaseData> Writes()
    {
        yield return Write("CounterIncrement", CrdtWriteOp.CounterIncrement,
            a => a.CounterIncrementAsync(Tree, Key, Replica, 7),
            r => Assert.Multiple(() =>
            {
                Assert.That(r.ReplicaId, Is.EqualTo(Replica));
                Assert.That(r.Amount, Is.EqualTo(7));
            }));
        yield return Write("CounterDecrement", CrdtWriteOp.CounterDecrement,
            a => a.CounterDecrementAsync(Tree, Key, Replica, 4),
            r => Assert.That(r.Amount, Is.EqualTo(4)));
        yield return Write("GCounterIncrement", CrdtWriteOp.GCounterIncrement,
            a => a.GCounterIncrementAsync(Tree, Key, Replica, 9),
            r => Assert.That(r.Amount, Is.EqualTo(9)));
        yield return Write("SetAdd", CrdtWriteOp.SetAdd,
            a => a.SetAddAsync(Tree, Key, Payload, Replica),
            r => Assert.That(r.Element, Is.EqualTo(Payload)));
        yield return Write("SetRemove", CrdtWriteOp.SetRemove,
            a => a.SetRemoveAsync(Tree, Key, Payload),
            r => Assert.That(r.Element, Is.EqualTo(Payload)));
        yield return Write("OrFlagEnable", CrdtWriteOp.OrFlagEnable,
            a => a.OrFlagEnableAsync(Tree, Key, Replica),
            r => Assert.That(r.ReplicaId, Is.EqualTo(Replica)));
        yield return Write("OrFlagDisable", CrdtWriteOp.OrFlagDisable,
            a => a.OrFlagDisableAsync(Tree, Key),
            r => Assert.That(r.Key, Is.EqualTo(Key)));
        yield return Write("RwFlagEnable", CrdtWriteOp.RwFlagEnable,
            a => a.RwFlagEnableAsync(Tree, Key, Replica),
            r => Assert.That(r.ReplicaId, Is.EqualTo(Replica)));
        yield return Write("RwFlagDisable", CrdtWriteOp.RwFlagDisable,
            a => a.RwFlagDisableAsync(Tree, Key, Replica),
            r => Assert.That(r.ReplicaId, Is.EqualTo(Replica)));
        yield return Write("RwSetAdd", CrdtWriteOp.RwSetAdd,
            a => a.RwSetAddAsync(Tree, Key, Payload, Replica),
            r => Assert.That(r.Element, Is.EqualTo(Payload)));
        yield return Write("RwSetRemove", CrdtWriteOp.RwSetRemove,
            a => a.RwSetRemoveAsync(Tree, Key, Payload, Replica),
            r => Assert.That(r.Element, Is.EqualTo(Payload)));
        yield return Write("VersionVectorTick", CrdtWriteOp.VersionVectorTick,
            a => a.VersionVectorTickAsync(Tree, Key, Replica),
            r => Assert.That(r.ReplicaId, Is.EqualTo(Replica)));
        yield return Write("RegisterSet", CrdtWriteOp.RegisterSet,
            a => a.RegisterSetAsync(Tree, Key, Replica, Payload),
            r => Assert.Multiple(() =>
            {
                Assert.That(r.ReplicaId, Is.EqualTo(Replica));
                Assert.That(r.Element, Is.EqualTo(Payload));
            }));
        yield return Write("MaxRegisterSet", CrdtWriteOp.MaxRegisterSet,
            a => a.MaxRegisterSetAsync(Tree, Key, Payload),
            r => Assert.That(r.Element, Is.EqualTo(Payload)));
        yield return Write("MinRegisterSet", CrdtWriteOp.MinRegisterSet,
            a => a.MinRegisterSetAsync(Tree, Key, Payload),
            r => Assert.That(r.Element, Is.EqualTo(Payload)));
        yield return Write("SequenceInsertAt", CrdtWriteOp.SequenceInsertAt,
            a => a.SequenceInsertAtAsync(Tree, Key, 2, Replica, Payload),
            r => Assert.Multiple(() =>
            {
                Assert.That(r.Index, Is.EqualTo(2));
                Assert.That(r.Element, Is.EqualTo(Payload));
            }));
        yield return Write("SequenceRemoveAt", CrdtWriteOp.SequenceRemoveAt,
            a => a.SequenceRemoveAtAsync(Tree, Key, 5),
            r => Assert.That(r.Index, Is.EqualTo(5)));
        yield return Write("MapSet", CrdtWriteOp.MapSet,
            a => a.MapSetAsync(Tree, Key, "colour", Replica, Payload),
            r => Assert.Multiple(() =>
            {
                Assert.That(r.Field, Is.EqualTo("colour"));
                Assert.That(r.Element, Is.EqualTo(Payload));
            }));
        yield return Write("MapRemove", CrdtWriteOp.MapRemove,
            a => a.MapRemoveAsync(Tree, Key, "colour"),
            r => Assert.That(r.Field, Is.EqualTo("colour")));
        yield return Write("GSetAdd", CrdtWriteOp.GSetAdd,
            a => a.GSetAddAsync(Tree, Key, Payload),
            r => Assert.That(r.Element, Is.EqualTo(Payload)));

        static TestCaseData Write(
            string name,
            CrdtWriteOp op,
            Func<ILatticeDataApi, Task> call,
            Action<CrdtWriteRequest> assert)
            => new TestCaseData(op, call, assert).SetArgDisplayNames(name);
    }

    [TestCaseSource(nameof(Writes))]
    public async Task Crdt_write_member_selects_its_wire_op_and_populates_its_payload(
        CrdtWriteOp expectedOp,
        Func<ILatticeDataApi, Task> call,
        Action<CrdtWriteRequest> assertRequest)
    {
        var invoker = WriteInvoker();

        await call(Adapter(invoker));

        var sent = (CrdtWriteRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo(Tree));
            Assert.That(sent.Key, Is.EqualTo(Key));
            Assert.That(sent.Op, Is.EqualTo(expectedOp),
                "Every typed write shares one request type, so the op discriminator is the whole contract.");
        });
        assertRequest(sent);
    }

    [Test]
    public void Every_crdt_write_op_is_exercised_by_the_write_table()
    {
        var exercised = Writes().Select(c => (CrdtWriteOp)c.Arguments[0]!).ToHashSet();

        Assert.That(exercised, Is.EquivalentTo(Enum.GetValues<CrdtWriteOp>()),
            "Every wire write op must be reachable from a typed facade member, so a newly added op "
            + "cannot ship without an adapter member that emits it.");
    }

    [Test]
    public void Crdt_write_translates_permission_denied()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(
                    _ => new RpcException(new Status(StatusCode.PermissionDenied, "denied"))))
                .GSetAddAsync(Tree, Key, Payload),
            Throws.TypeOf<LatticeAuthorizationDeniedException>().With.InnerException.TypeOf<RpcException>(),
            "A denied CRDT write must surface as the facade's own denial, with the transport fault preserved.");

    [Test]
    public void Crdt_write_leaves_a_non_permission_fault_unchanged()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(
                    _ => new RpcException(new Status(StatusCode.Unavailable, "down"))))
                .GSetAddAsync(Tree, Key, Payload),
            Throws.TypeOf<RpcException>(),
            "Only PermissionDenied is translated; every other transport fault is surfaced as-is.");

    // ---- reads: projection out of the carry-all response --------------------

    [Test]
    public async Task Counter_reads_select_their_kind_and_project_the_counter_value()
    {
        var pn = ReadInvoker(new CrdtReadResponse { CounterValue = 11 });
        var pnValue = await Adapter(pn).CounterGetAsync(Tree, Key);
        var pnRequest = (CrdtReadRequest)pn.LastRequest!;

        var g = ReadInvoker(new CrdtReadResponse { CounterValue = 12 });
        var gValue = await Adapter(g).GCounterGetAsync(Tree, Key);
        var gRequest = (CrdtReadRequest)g.LastRequest!;

        Assert.Multiple(() =>
        {
            Assert.That(pnValue, Is.EqualTo(11));
            Assert.That(pnRequest.Kind, Is.EqualTo(CrdtKind.PnCounter));
            Assert.That(gValue, Is.EqualTo(12));
            Assert.That(gRequest.Kind, Is.EqualTo(CrdtKind.GCounter));
        });
    }

    [Test]
    public async Task Flag_reads_select_their_kind_and_project_the_flag_value()
    {
        var or = ReadInvoker(new CrdtReadResponse { FlagValue = true });
        var orValue = await Adapter(or).OrFlagGetAsync(Tree, Key);

        var rw = ReadInvoker(new CrdtReadResponse { FlagValue = false });
        var rwValue = await Adapter(rw).RwFlagGetAsync(Tree, Key);

        Assert.Multiple(() =>
        {
            Assert.That(orValue, Is.True);
            Assert.That(((CrdtReadRequest)or.LastRequest!).Kind, Is.EqualTo(CrdtKind.OrFlag));
            Assert.That(rwValue, Is.False);
            Assert.That(((CrdtReadRequest)rw.LastRequest!).Kind, Is.EqualTo(CrdtKind.RwFlag));
        });
    }

    [TestCase(CrdtKind.OrSet)]
    [TestCase(CrdtKind.RwSet)]
    [TestCase(CrdtKind.MvRegister)]
    [TestCase(CrdtKind.Sequence)]
    [TestCase(CrdtKind.GSet)]
    public async Task Element_reads_select_their_kind_and_project_the_element_list(CrdtKind kind)
    {
        var invoker = ReadInvoker(new CrdtReadResponse { Elements = [Payload] });
        var adapter = Adapter(invoker);

        IReadOnlyList<byte[]> elements = kind switch
        {
            CrdtKind.OrSet => await adapter.SetGetAsync(Tree, Key),
            CrdtKind.RwSet => await adapter.RwSetGetAsync(Tree, Key),
            CrdtKind.MvRegister => await adapter.RegisterGetAsync(Tree, Key),
            CrdtKind.Sequence => await adapter.SequenceGetAsync(Tree, Key),
            _ => await adapter.GSetGetAsync(Tree, Key),
        };

        Assert.Multiple(() =>
        {
            Assert.That(elements, Is.EqualTo(new[] { Payload }));
            Assert.That(((CrdtReadRequest)invoker.LastRequest!).Kind, Is.EqualTo(kind));
        });
    }

    [Test]
    public async Task Version_vector_read_rebuilds_the_replica_to_clock_map()
    {
        var invoker = ReadInvoker(new CrdtReadResponse
        {
            Vector =
            [
                new CrdtVectorEntry { ReplicaId = "a", Clock = "1" },
                new CrdtVectorEntry { ReplicaId = "b", Clock = "2" },
            ],
        });

        var vector = await Adapter(invoker).VersionVectorGetAsync(Tree, Key);

        Assert.Multiple(() =>
        {
            Assert.That(vector, Has.Count.EqualTo(2));
            Assert.That(vector["a"], Is.EqualTo("1"));
            Assert.That(vector["b"], Is.EqualTo("2"));
            Assert.That(((CrdtReadRequest)invoker.LastRequest!).Kind, Is.EqualTo(CrdtKind.VersionVector));
        });
    }

    [Test]
    public async Task Version_vector_read_of_an_absent_key_yields_an_empty_map()
    {
        var vector = await Adapter(ReadInvoker(new CrdtReadResponse())).VersionVectorGetAsync(Tree, Key);

        Assert.That(vector, Is.Empty, "An absent version vector is an empty map, never a fault.");
    }

    [Test]
    public async Task Map_read_rebuilds_the_field_to_values_map()
    {
        var invoker = ReadInvoker(new CrdtReadResponse
        {
            Map =
            [
                new CrdtMapField { Field = "colour", Values = [Payload] },
                new CrdtMapField { Field = "size", Values = [] },
            ],
        });

        var map = await Adapter(invoker).MapGetAsync(Tree, Key);

        Assert.Multiple(() =>
        {
            Assert.That(map, Has.Count.EqualTo(2));
            Assert.That(map["colour"], Is.EqualTo(new[] { Payload }));
            Assert.That(map["size"], Is.Empty, "A field with no live value is still reported.");
            Assert.That(((CrdtReadRequest)invoker.LastRequest!).Kind, Is.EqualTo(CrdtKind.OrMap));
        });
    }

    [Test]
    public async Task Max_and_min_register_reads_project_the_first_element_or_null()
    {
        var present = ReadInvoker(new CrdtReadResponse { Elements = [Payload] });
        var max = await Adapter(present).MaxRegisterGetAsync(Tree, Key);
        var maxKind = ((CrdtReadRequest)present.LastRequest!).Kind;

        var absent = ReadInvoker(new CrdtReadResponse());
        var min = await Adapter(absent).MinRegisterGetAsync(Tree, Key);
        var minKind = ((CrdtReadRequest)absent.LastRequest!).Kind;

        Assert.Multiple(() =>
        {
            Assert.That(max, Is.EqualTo(Payload));
            Assert.That(maxKind, Is.EqualTo(CrdtKind.MaxRegister));
            Assert.That(min, Is.Null, "An absent single-valued register reads as null, never a fault.");
            Assert.That(minKind, Is.EqualTo(CrdtKind.MinRegister));
        });
    }

    [Test]
    public async Task Min_register_read_projects_the_first_element_when_present()
    {
        var min = await Adapter(ReadInvoker(new CrdtReadResponse { Elements = [Payload] }))
            .MinRegisterGetAsync(Tree, Key);

        Assert.That(min, Is.EqualTo(Payload));
    }

    [Test]
    public async Task Max_register_read_of_an_absent_key_yields_null()
    {
        var max = await Adapter(ReadInvoker(new CrdtReadResponse())).MaxRegisterGetAsync(Tree, Key);

        Assert.That(max, Is.Null);
    }
}
