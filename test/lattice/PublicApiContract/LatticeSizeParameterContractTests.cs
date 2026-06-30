using System.Reflection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Binds the reusable <see cref="PublicApiSizeContractTestsBase"/> guard to the
/// <see cref="ILattice"/> public surface. The base discovers every public
/// <see cref="ILattice"/> method with a size/limit <see cref="int"/> parameter
/// (today: <see cref="ILattice.NextKeysAsync"/> /
/// <see cref="ILattice.NextEntriesAsync"/> <c>pageSize</c>,
/// <see cref="ILattice.DeleteRangeStepAsync"/> <c>maxToDelete</c>, and
/// <see cref="ILattice.ScanEntryHistoryAsync"/> <c>limit</c>) and exercises each
/// with <see cref="int.MaxValue"/> / <see cref="int.MinValue"/> / <c>0</c> /
/// <c>-1</c>, asserting none of them faults the silo with
/// <see cref="OutOfMemoryException"/>.
/// <para>
/// This subclass is intentionally thin: it only seeds a live tree and supplies
/// the meaningful non-size arguments (a matching live cursor id, an existing
/// key). Adding a new size parameter to <see cref="ILattice"/> needs no change
/// here - the reflection-built table picks it up automatically.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("API")]
public sealed class LatticeSizeParameterContractTests
    : PublicApiSizeContractTestsBase<LatticeSizeParameterContractTests>
{
    private PublicApiContractClusterFixture _fixture = null!;
    private int _treeCounter;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new PublicApiContractClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    /// <inheritdoc />
    protected override IReadOnlyCollection<Type> ApiTypes => [typeof(ILattice)];

    /// <summary>
    /// Hands back a freshly-seeded, uniquely-named tree per test case so a
    /// pathological delete-range step in one case cannot perturb another.
    /// </summary>
    protected override async ValueTask<object> ResolveInstanceAsync(Type apiType)
    {
        var treeId = $"pac-size-contract-{Interlocked.Increment(ref _treeCounter)}";
        var tree = _fixture.GetTree(treeId);
        await tree.SetManyAsync(
        [
            new("a", "1"u8.ToArray()),
            new("b", "2"u8.ToArray()),
            new("c", "3"u8.ToArray()),
            new("d", "4"u8.ToArray()),
            new("e", "5"u8.ToArray()),
        ]);
        return tree;
    }

    /// <summary>
    /// Supplies a live cursor id matching the audited cursor method, or an
    /// existing key for the history scan; every other parameter (range bounds,
    /// HLC bounds, continuation token, cancellation token) is left at its
    /// default.
    /// </summary>
    protected override async ValueTask<object?> ResolveArgumentAsync(
        SizeParameterTarget target,
        ParameterInfo parameter,
        object instance)
    {
        var tree = (ILattice)instance;

        return parameter.Name switch
        {
            "cursorId" => await OpenCursorForAsync(tree, target.Method.Name),
            "key" => "a",
            _ => ContractArgument.UseDefault,
        };
    }

    private static async Task<string> OpenCursorForAsync(ILattice tree, string methodName) =>
        methodName switch
        {
            nameof(ILattice.NextKeysAsync) => await tree.OpenKeyCursorAsync(),
            nameof(ILattice.NextEntriesAsync) => await tree.OpenEntryCursorAsync(),
            nameof(ILattice.DeleteRangeStepAsync) => await tree.OpenDeleteRangeCursorAsync("a", "z"),
            // Fallback: a key cursor is a valid handle for any unrecognised
            // future cursor-stepping method; the contract assertion (no OOM)
            // still holds even if the kind mismatches and the call rejects it.
            _ => await tree.OpenKeyCursorAsync(),
        };
}
