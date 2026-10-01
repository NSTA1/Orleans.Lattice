using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage that the batched OR-Flag enable helpers
/// (<see cref="CrdtLatticeExtensions.EnableManyAsync"/> and
/// <see cref="CrdtLatticeExtensions.StageEnableManyAsync"/>) reject an empty
/// replica id, as the per-key <see cref="OrFlagAccessor.EnableAsync(string, CancellationToken, int)"/>
/// and the <see cref="OrFlag.Enable(string, long)"/> primitive already do.
/// <para>
/// They used to check only for <see langword="null"/>, so an empty id minted
/// enable dots in a replica namespace every such caller shares. OR-Flag
/// cancellation is coverage-based per replica, so one writer's disable - which
/// tombstones the dots it observed - would also cancel another writer's
/// concurrent, unobserved enable under the same empty id, breaking add-wins.
/// </para>
/// </summary>
[TestFixture]
public class CrdtLatticeExtensionsEnableManyReplicaIdTests
{
    private static ILattice NewLattice()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new Dictionary<string, byte[]>()));
        lattice.ClearReceivedCalls();
        return lattice;
    }

    [Test]
    public void EnableManyAsync_rejects_an_empty_replica_id_without_reading_or_writing()
    {
        var lattice = NewLattice();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await lattice.EnableManyAsync(new[] { "k1", "k2" }, string.Empty),
                Throws.TypeOf<ArgumentException>().With.Property(nameof(ArgumentException.ParamName)).EqualTo("replicaId"));
            Assert.That(lattice.ReceivedCalls(), Is.Empty, "a rejected batch must neither read nor apply");
        });
    }

    [Test]
    public void StageEnableManyAsync_rejects_an_empty_replica_id_without_reading()
    {
        var lattice = NewLattice();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await lattice.StageEnableManyAsync(new[] { "k1", "k2" }, string.Empty),
                Throws.TypeOf<ArgumentException>().With.Property(nameof(ArgumentException.ParamName)).EqualTo("replicaId"));
            Assert.That(lattice.ReceivedCalls(), Is.Empty, "a rejected staging call must not read");
        });
    }

    [Test]
    public void Both_helpers_reject_an_empty_replica_id_even_for_an_empty_key_set()
    {
        var lattice = NewLattice();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await lattice.EnableManyAsync(Array.Empty<string>(), string.Empty),
                Throws.TypeOf<ArgumentException>());
            Assert.That(
                async () => await lattice.StageEnableManyAsync(Array.Empty<string>(), string.Empty),
                Throws.TypeOf<ArgumentException>());
        });
    }

    [Test]
    public void Both_helpers_still_reject_a_null_replica_id_with_ArgumentNullException()
    {
        var lattice = NewLattice();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await lattice.EnableManyAsync(new[] { "k" }, null!),
                Throws.TypeOf<ArgumentNullException>());
            Assert.That(
                async () => await lattice.StageEnableManyAsync(new[] { "k" }, null!),
                Throws.TypeOf<ArgumentNullException>());
        });
    }
}
