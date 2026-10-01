using System.Text;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage that every CRDT accessor's TTL overload rejects a zero or
/// negative <see cref="TimeSpan"/> the way the
/// <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], TimeSpan, CancellationToken)"/>
/// seam does. Eleven of the thirteen overloads used to route a non-positive TTL
/// to the durable no-TTL write instead, so a caller whose computed remaining life
/// had already run out got an entry that never expires. Each case drives the
/// accessor against a substituted <see cref="ILattice"/> and asserts both the
/// rejection and that nothing was read or written.
/// </summary>
[TestFixture]
public class CrdtAccessorTtlValidationTests
{
    private static byte[] Elem => Encoding.UTF8.GetBytes("e");

    private static readonly TimeSpan[] NonPositiveTtls = [TimeSpan.Zero, TimeSpan.FromSeconds(-1)];

    private static IEnumerable<TestCaseData> TtlOverloads()
    {
        var overloads = new (string Name, Func<ILattice, TimeSpan, Task> Write)[]
        {
            ("GCounter.IncrementAsync", static (l, ttl) => l.GCounter("k").IncrementAsync("r", 1, ttl)),
            ("GSet.AddAsync", static (l, ttl) => l.GSet("k").AddAsync(Elem, ttl)),
            ("MaxRegister.SetAsync", static (l, ttl) => l.MaxRegister<byte[]>("k", v => v).SetAsync(Elem, ttl)),
            ("MinRegister.SetAsync", static (l, ttl) => l.MinRegister<byte[]>("k", v => v).SetAsync(Elem, ttl)),
            ("MvRegister.SetAsync", static (l, ttl) => l.MvRegister<string>("k").SetAsync("r", "v", ttl)),
            ("OrFlag.EnableAsync", static (l, ttl) => l.OrFlag("k").EnableAsync("r", ttl)),
            ("OrMap.SetAsync", static (l, ttl) => l.OrMap<string, PnCounter>("k").SetAsync("mk", "r", new PnCounter(), ttl)),
            ("OrSet.AddAsync", static (l, ttl) => l.OrSet("k").AddAsync(Elem, "r", ttl)),
            ("PnCounter.IncrementAsync", static (l, ttl) => l.PnCounter("k").IncrementAsync("r", 1, ttl)),
            ("Sequence.InsertAtAsync", static (l, ttl) => l.Sequence<string>("k").InsertAtAsync(0, "r", "v", ttl)),
            ("RwFlag.EnableAsync", static (l, ttl) => l.RwFlag("k").EnableAsync("r", ttl)),
            ("RwSet.AddAsync", static (l, ttl) => l.RwSet("k").AddAsync(Elem, "r", ttl)),
            ("VersionVector.TickAsync", static (l, ttl) => l.VersionVector("k").TickAsync("r", ttl)),
        };

        foreach (var (name, write) in overloads)
        {
            foreach (var ttl in NonPositiveTtls)
            {
                yield return new TestCaseData(write, ttl).SetArgDisplayNames(name, ttl.ToString());
            }
        }
    }

    [TestCaseSource(nameof(TtlOverloads))]
    public void Ttl_overload_rejects_a_non_positive_ttl_without_reading_or_writing(
        Func<ILattice, TimeSpan, Task> write,
        TimeSpan ttl)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        lattice.ClearReceivedCalls();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => write(lattice, ttl),
                Throws.TypeOf<ArgumentOutOfRangeException>().With.Property(nameof(ArgumentException.ParamName)).EqualTo("ttl"));
            Assert.That(
                lattice.ReceivedCalls(),
                Is.Empty,
                "a rejected TTL must neither read the entry nor write it - least of all as a durable entry");
        });
    }
}
