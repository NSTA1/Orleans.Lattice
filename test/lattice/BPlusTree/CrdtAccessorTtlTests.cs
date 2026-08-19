using System.Text;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage that every CRDT accessor's primary mutating write exposes a
/// per-entry TTL overload and threads the <see cref="System.TimeSpan"/> through
/// to the TTL-carrying
/// <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], System.TimeSpan, System.Threading.CancellationToken)"/>
/// seam (rather than the durable no-TTL overload). Each test drives the accessor
/// against a substituted <see cref="ILattice"/> and asserts the ttl-carrying
/// call was received with the accessor's own merge mode, so a regression that
/// drops the ttl on any single accessor fails in isolation.
/// </summary>
[TestFixture]
public class CrdtAccessorTtlTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(5);
    private static byte[] Elem => Encoding.UTF8.GetBytes("e");

    private static ILattice NewLattice()
    {
        var lattice = Substitute.For<ILattice>();
        // NSubstitute auto-returns an empty array for byte[]-returning members;
        // stub the read seam to null so accessors that read-modify-write decode
        // an empty CRDT rather than failing to deserialize zero-length JSON.
        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        return lattice;
    }

    [Test]
    public async Task GCounter_IncrementAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.GCounter("k").IncrementAsync("r", 1, Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GSet_AddAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.GSet("k").AddAsync(Elem, Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.GSet, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MaxRegister_SetAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.MaxRegister<byte[]>("k", v => v).SetAsync(Elem, Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.MaxRegister, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MinRegister_SetAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.MinRegister<byte[]>("k", v => v).SetAsync(Elem, Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.MinRegister, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MvRegister_SetAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.MvRegister<string>("k").SetAsync("r", "v", Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.MvRegister, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OrFlag_EnableAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.OrFlag("k").EnableAsync("r", Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrFlag, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OrMap_SetAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.OrMap<string, PnCounter>("k").SetAsync("mk", "r", new PnCounter(), Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrMap, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OrSet_AddAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.OrSet("k").AddAsync(Elem, "r", Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PnCounter_IncrementAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.PnCounter("k").IncrementAsync("r", 1, Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.PnCounter, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Sequence_InsertAtAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.Sequence<string>("k").InsertAtAsync(0, "r", "v", Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.Sequence, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RwFlag_EnableAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.RwFlag("k").EnableAsync("r", Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.RwFlag, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RwSet_AddAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.RwSet("k").AddAsync(Elem, "r", Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.RwSet, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task VersionVector_TickAsync_ttl_routes_through_ttl_seam()
    {
        var lattice = NewLattice();
        await lattice.VersionVector("k").TickAsync("r", Ttl);
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.VersionVector, Arg.Any<byte[]>(), Ttl, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Durable_write_does_not_route_through_ttl_seam()
    {
        // The non-TTL primary write must never invoke the ttl-carrying
        // overload - a durable CRDT write leaves expiry untouched.
        var lattice = NewLattice();
        await lattice.OrSet("k").AddAsync(Elem, "r");
        await lattice.DidNotReceive().ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, Arg.Any<byte[]>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>());
        await lattice.Received(1).ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }
}
