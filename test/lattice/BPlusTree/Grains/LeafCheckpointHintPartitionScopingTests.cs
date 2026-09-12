using System.Reflection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guards the partition scoping of the leaf checkpoint-hint seam (issue #2699).
/// <para>
/// A checkpoint hint is only meaningful against a specific WAL partition, and a
/// grain method cannot recover that partition from ambient state: the apply-offset
/// scope is an <c>AsyncLocal</c>, and an <c>AsyncLocal</c> does not flow across an
/// Orleans grain call. A hint seam that takes only an offset therefore resolves
/// its partition from an <em>always absent</em> context and lands every write on
/// partition 0, whatever the caller meant - silently, with no error, under a name
/// that reads as partition-agnostic.
/// </para>
/// <para>
/// The removed <c>SetCheckpointOffsetHintAsync(long)</c> had exactly that shape
/// and was harmless only because nothing in <c>src/</c> called it. This fixture
/// stops it (or an equivalent) coming back: the partition must travel in the
/// argument, so a hint seam must not take a bare scalar offset.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafCheckpointHintPartitionScopingTests
{
    /// <summary>
    /// The methods on <see cref="IBPlusLeafGrain"/> that hint a projection
    /// checkpoint offset. Matched on the naming stem rather than an exact name so
    /// a re-introduced singular, or a differently-named sibling, is still caught.
    /// </summary>
    private static MethodInfo[] HintSeams() =>
        [.. typeof(IBPlusLeafGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => m.Name.Contains("CheckpointOffsetHint", StringComparison.Ordinal))
            .OrderBy(m => m.Name, StringComparer.Ordinal)];

    [Test]
    public void No_checkpoint_hint_seam_takes_a_bare_scalar_offset()
    {
        var seams = HintSeams();

        // Without this the fixture passes vacuously if the seam is renamed out
        // from under the stem: nothing to inspect reads exactly like nothing
        // wrong, which is the failure mode a scoping guard is least able to
        // notice.
        Assert.That(seams, Is.Not.Empty,
            "no checkpoint-hint seam was found on IBPlusLeafGrain - the guard cannot pass "
            + "vacuously, so either the seam was renamed or the naming stem is stale");

        var unscoped = seams
            .Where(m =>
            {
                var parameters = m.GetParameters();
                return parameters.Length == 1 && parameters[0].ParameterType == typeof(long);
            })
            .Select(m => m.Name)
            .ToArray();

        Assert.That(unscoped, Is.Empty,
            "A checkpoint-hint seam taking only a scalar offset has no way to learn its "
            + "target partition: LatticeApplyOffsetContext is an AsyncLocal and does not flow "
            + "across an Orleans grain call, so the scope is always absent at the callee and "
            + "every hint lands on partition 0 whatever the caller meant. Carry the partition "
            + "in the argument (see SetCheckpointOffsetHintsAsync) instead."
            + Environment.NewLine
            + string.Join(Environment.NewLine, unscoped));
    }

    [Test]
    public void The_partition_scoped_hint_seam_carries_offsets_indexed_by_partition()
    {
        // The positive half: the guard above only forbids a shape, and a seam
        // surface that forbade the wrong shape while offering no right one
        // would satisfy it just as well as a correct surface does.
        var plural = typeof(IBPlusLeafGrain).GetMethod(
            nameof(IBPlusLeafGrain.SetCheckpointOffsetHintsAsync),
            BindingFlags.Public | BindingFlags.Instance);

        Assert.That(plural, Is.Not.Null,
            "the partition-scoped hint seam must exist - it is what callers are directed to");

        var parameters = plural!.GetParameters();
        Assert.Multiple(() =>
        {
            Assert.That(parameters, Has.Length.EqualTo(1));
            Assert.That(parameters[0].ParameterType, Is.EqualTo(typeof(long[])),
                "offsets must arrive indexed by partition ordinal, which is how the partition "
                + "travels in the argument rather than in an AsyncLocal that cannot flow");
        });
    }
}
