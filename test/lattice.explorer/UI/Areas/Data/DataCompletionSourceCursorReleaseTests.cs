using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Data area's key completions when releasing the scan cursor they opened
/// fails: the release is best effort, so the failure is swallowed and the
/// completions are answered regardless.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataCompletionSourceCursorReleaseTests : DataTestContext
{
    private readonly IDataReader _reader = Substitute.For<IDataReader>();

    /// <summary>Registers the substitute reader over the real one.</summary>
    public DataCompletionSourceCursorReleaseTests()
    {
        Services.AddSingleton(_reader);
    }

    [Test]
    public async Task A_failed_cursor_release_does_not_withhold_the_completions()
    {
        Client.WithTree("orders");
        _reader.ScanAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<TagFilter?>(), Arg.Any<string?>(), Arg.Any<EntryScanMode>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DataPage
            {
                Entries = [new DataEntry { Key = "k1" }],
                ContinuationToken = "cursor-1",
            }));
        var attempted = new TaskCompletionSource();
        _reader.CancelScanAsync(Arg.Any<string>(), "cursor-1", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                attempted.TrySetResult();
                return Task.FromException(new InvalidOperationException("the cursor was already reaped"));
            });

        var results = await Area.Completions!.CompleteAsync(
            new AddressQuery("k", AddressQueryMode.Search, ExplorerAddress.ForTree("data", "orders")),
            CancellationToken.None);

        Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "k1" }));
        await TestPoll.UntilAsync(() => attempted.Task.IsCompleted, "the release was attempted");
    }
}
