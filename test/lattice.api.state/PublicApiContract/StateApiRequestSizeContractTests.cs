using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.State.Tests.PublicApiContract;

/// <summary>
/// Binds the reusable <see cref="RequestSizeContractTestsBase{TSelf}"/> guard to
/// the State-API read facade (<see cref="ILatticeStateQuery"/>), whose
/// caller-influenced sizes live on request records rather than as bare method
/// parameters - so the method-parameter guard (bound to <c>ILattice</c>) does
/// not see them. The base discovers, by reflection, every request-DTO
/// size/limit <see cref="int"/> property reachable through the facade (today:
/// <see cref="CatalogRequest.PageSize"/>, <see cref="EntryScanRequest.PageSize"/>
/// / <see cref="EntryScanRequest.ValuePreviewBudget"/>,
/// <see cref="EntryHistoryRequest.Limit"/> /
/// <see cref="EntryHistoryRequest.ValuePreviewBudget"/>, and
/// <see cref="StructureRequest.MaxNodes"/>) and exercises each with
/// <see cref="int.MaxValue"/> / <see cref="int.MinValue"/> / <c>0</c> / <c>-1</c>,
/// asserting none faults the silo with <see cref="OutOfMemoryException"/>.
/// <para>
/// This subclass is intentionally thin: it seeds a live tree and builds one valid
/// baseline request per request type (with required fields pointing at the seeded
/// tree / key) so the call reaches its size-sensitive path. Adding a new size
/// property to an existing request, a new request type, or a new facade method
/// needs no change here - the reflection-built table picks it up automatically;
/// only a brand-new request type needs a baseline entry, and an unconfigured one
/// fails loudly rather than slipping past the guard.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("API")]
public sealed class StateApiRequestSizeContractTests
    : RequestSizeContractTestsBase<StateApiRequestSizeContractTests>
{
    private const string TreeId = "state-size-contract";
    private const string KnownKey = "key-00000";

    private StateQueryClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new StateQueryClusterFixture();
        await _fixture.InitializeAsync();
        await _fixture.CreatePopulatedTreeAsync(TreeId, keyCount: 8);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    /// <inheritdoc />
    protected override IReadOnlyCollection<Type> ApiTypes => [typeof(ILatticeStateQuery)];

    /// <inheritdoc />
    protected override ValueTask<object> ResolveServiceAsync(Type apiType) => new(_fixture.Query);

    /// <summary>
    /// Runs the reflective facade call from this test assembly, which holds
    /// <c>InternalsVisibleTo</c> access to the internal
    /// <see cref="ILatticeStateQuery"/> - a cross-assembly invoke from the shared
    /// testing library would otherwise raise <see cref="MethodAccessException"/>.
    /// </summary>
    protected override object? Invoke(MethodInfo method, object service, object?[] arguments) =>
        method.Invoke(service, arguments);

    /// <summary>
    /// Builds a valid baseline request per request type, with size properties at
    /// their safe defaults and required fields pointing at the live seeded tree /
    /// key so the call reaches its size-sensitive allocation. The base then sets
    /// the discovered size property to the pathological boundary by reflection.
    /// </summary>
    protected override ValueTask<object> BuildBaselineRequestAsync(Type requestType)
    {
        object request = requestType switch
        {
            _ when requestType == typeof(CatalogRequest) =>
                new CatalogRequest { SourceTreeId = TreeId, IndexName = "size-contract-index" },
            _ when requestType == typeof(EntryScanRequest) =>
                new EntryScanRequest { TreeId = TreeId },
            _ when requestType == typeof(EntryHistoryRequest) =>
                new EntryHistoryRequest { TreeId = TreeId, Key = KnownKey },
            _ when requestType == typeof(StructureRequest) =>
                new StructureRequest { TreeId = TreeId },
            _ when requestType == typeof(TagMemberScanRequest) =>
                new TagMemberScanRequest { IndexName = "size-contract-index", Tag = "size-contract-tag" },
            _ => throw new NotSupportedException(
                $"No baseline request is configured for '{requestType}'. Add one so the "
                + "size-contract guard exercises its caller-influenced size properties."),
        };

        return new ValueTask<object>(request);
    }
}
