using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Shared construction helpers for the enrolment unit tests, so each fixture
/// states only what it is actually varying.
/// </summary>
internal static class EnrollmentTestIndex
{
    /// <summary>The index name the enrolment unit tests use.</summary>
    internal const string IndexName = "Subjects";

    /// <summary>An enroller over the two-property test definition.</summary>
    /// <param name="store">The bookkeeping store.</param>
    /// <param name="tree">The index tree, defaulting to one that accepts every batch.</param>
    /// <param name="mode">The projection mode.</param>
    /// <returns>The enroller.</returns>
    internal static TypedGrainIndexEnroller<ITestStringKeyedGrain, IndexedTestState> Enroller(
        IGrainIndexEnrollmentStore store,
        ILattice? tree = null,
        GrainIndexProjectionMode mode = GrainIndexProjectionMode.Synchronous) =>
        new(Definition(), tree ?? EnrollmentTrees.Accepting(), store, mode);

    /// <summary>A definition over two ordered properties of the test state.</summary>
    /// <param name="name">The index name.</param>
    /// <returns>The definition.</returns>
    internal static GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> Definition(
        string name = IndexName) =>
        new(
            name,
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [
                new TypedGrainIndexProperty<IndexedTestState, int>("Age", static s => s.Age),
                new TypedGrainIndexProperty<IndexedTestState, string>("Country", static s => s.Country),
            ]);

    /// <summary>A substituted grain implementing the indexed grain interface.</summary>
    /// <returns>The substitute.</returns>
    internal static ITestStringKeyedGrain GrainInstance() => Substitute.For<ITestStringKeyedGrain>();

    /// <summary>The identity the test grain key maps to.</summary>
    /// <param name="key">The primary key.</param>
    /// <returns>The grain identity.</returns>
    internal static GrainId Identity(string key) => GrainId.Create("subject", key);

    /// <summary>The projection of a state, as the index would compute it.</summary>
    /// <param name="grainKey">The encoded grain key.</param>
    /// <param name="state">The state to project.</param>
    /// <returns>The projection.</returns>
    internal static GrainIndexProjection Project(string grainKey, IndexedTestState state) =>
        new GrainIndexProjector<ITestStringKeyedGrain, IndexedTestState>(Definition())
            .Project(grainKey, state);
}
