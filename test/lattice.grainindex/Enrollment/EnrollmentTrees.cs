using NSubstitute;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Substituted <see cref="ILattice"/> trees for the enrolment tests: one that
/// accepts every atomic batch and records it, and one that rejects every batch
/// so an index-write fault can be induced deliberately.
/// </summary>
internal static class EnrollmentTrees
{
    /// <summary>The message an induced index-write fault carries.</summary>
    internal const string InducedFaultMessage = "Induced index-write fault.";

    /// <summary>A tree whose atomic batches always fail.</summary>
    /// <returns>The substituted tree.</returns>
    internal static ILattice Faulting()
    {
        var tree = Substitute.For<ILattice>();

        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException(InducedFaultMessage)));

        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException(InducedFaultMessage)));

        return tree;
    }

    /// <summary>A tree that accepts every atomic batch.</summary>
    /// <returns>The substituted tree.</returns>
    internal static ILattice Accepting()
    {
        var tree = Substitute.For<ILattice>();

        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        return tree;
    }
}
