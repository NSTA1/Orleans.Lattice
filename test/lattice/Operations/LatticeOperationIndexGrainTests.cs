using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Unit tests for <see cref="LatticeOperationIndexGrain"/>: newest-first ordering,
/// cursor paging, kind-prefix filtering, malformed-token refusal, retention
/// pruning and the size cap.
/// </summary>
[TestFixture]
public sealed class LatticeOperationIndexGrainTests
{
    private const string Tenant = "acme";

    private ManualTimeProvider _clock = null!;
    private FakePersistentState<LatticeOperationIndexState> _state = null!;
    private IGrainFactory _factory = null!;
    private LatticeOperationOptions _options = null!;

    [SetUp]
    public void SetUp()
    {
        _clock = new ManualTimeProvider(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
        _state = new FakePersistentState<LatticeOperationIndexState>();
        _factory = Substitute.For<IGrainFactory>();
        _factory.GetGrain<ILatticeOperationGrain>(Arg.Any<string>(), null).Returns(_ => Substitute.For<ILatticeOperationGrain>());
        _options = new LatticeOperationOptions { Retention = TimeSpan.FromHours(1), MaxIndexedOperations = 100 };
    }

    private LatticeOperationIndexGrain CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("latticeoperationindex", LatticeOperationKey.ForIndex(Tenant)));
        return new LatticeOperationIndexGrain(context, _state, _factory, Options.Create(_options)) { Clock = _clock };
    }

    private DateTimeOffset At(int minutes) => new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(minutes);

    [Test]
    public void GrainContext_returns_the_injected_context()
    {
        Assert.That(CreateGrain().GrainContext, Is.Not.Null);
    }

    [Test]
    public async Task Entries_list_newest_first_whatever_the_insertion_order()
    {
        var grain = CreateGrain();
        await grain.AddAsync("b", "backup.capture", At(2));
        await grain.AddAsync("a", "backup.capture", At(1));
        await grain.AddAsync("c", "backup.capture", At(3));
        await grain.AddAsync("c", "backup.capture", At(3));

        var page = await grain.ListAsync(null, null, 10);

        Assert.Multiple(() =>
        {
            Assert.That(page.OperationIds, Is.EqualTo(new[] { "c", "b", "a" }), "Newest first; a repeated add is a no-op.");
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task Ties_on_start_time_order_by_id()
    {
        var grain = CreateGrain();
        await grain.AddAsync("z", "k", At(1));
        await grain.AddAsync("a", "k", At(1));

        Assert.That((await grain.ListAsync(null, null, 10)).OperationIds, Is.EqualTo(new[] { "a", "z" }));
    }

    [Test]
    public async Task Paging_walks_every_entry_exactly_once()
    {
        var grain = CreateGrain();
        for (var i = 0; i < 5; i++)
        {
            await grain.AddAsync($"op-{i}", "k", At(i));
        }

        var first = await grain.ListAsync(null, null, 2);
        var second = await grain.ListAsync(null, first.NextPageToken, 2);
        var third = await grain.ListAsync(null, second.NextPageToken, 2);

        Assert.Multiple(() =>
        {
            Assert.That(first.OperationIds, Is.EqualTo(new[] { "op-4", "op-3" }));
            Assert.That(second.OperationIds, Is.EqualTo(new[] { "op-2", "op-1" }));
            Assert.That(third.OperationIds, Is.EqualTo(new[] { "op-0" }));
            Assert.That(third.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task A_page_that_ends_exactly_on_the_last_entry_has_no_next_token()
    {
        var grain = CreateGrain();
        await grain.AddAsync("a", "k", At(1));
        await grain.AddAsync("b", "k", At(2));

        Assert.That((await grain.ListAsync(null, null, 2)).NextPageToken, Is.Null);
    }

    [Test]
    public async Task Kind_prefix_filters_the_listing()
    {
        var grain = CreateGrain();
        await grain.AddAsync("bk", "backup.capture", At(1));
        await grain.AddAsync("view", "view.rebuild", At(2));

        var page = await grain.ListAsync("backup.", null, 10);

        Assert.That(page.OperationIds, Is.EqualTo(new[] { "bk" }));
    }

    [TestCase("garbage")]
    [TestCase("12:bad/id")]
    [TestCase(":id")]
    public void A_malformed_page_token_is_refused(string token)
    {
        Assert.That(async () => await CreateGrain().ListAsync(null, token, 10), Throws.ArgumentException);
    }

    [Test]
    public void A_non_positive_page_size_is_refused()
    {
        Assert.That(async () => await CreateGrain().ListAsync(null, null, 0), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task Finished_entries_are_pruned_once_their_retention_elapses()
    {
        var grain = CreateGrain();
        await grain.AddAsync("old", "k", At(0));
        await grain.AddAsync("running", "k", At(1));
        await grain.MarkFinishedAsync("old", _clock.GetUtcNow());
        _clock.Advance(_options.Retention);

        var page = await grain.ListAsync(null, null, 10);

        Assert.That(page.OperationIds, Is.EqualTo(new[] { "running" }), "A running entry is never pruned by age.");
        _factory.Received().GetGrain<ILatticeOperationGrain>(LatticeOperationKey.For(Tenant, "old"), null);
    }

    [Test]
    public async Task Past_the_cap_the_oldest_finished_entry_goes_first()
    {
        _options.MaxIndexedOperations = 2;
        var grain = CreateGrain();
        await grain.AddAsync("finished-old", "k", At(0));
        await grain.AddAsync("running-old", "k", At(1));
        await grain.MarkFinishedAsync("finished-old", At(2));

        await grain.AddAsync("new", "k", At(3));

        Assert.That((await grain.ListAsync(null, null, 10)).OperationIds, Is.EqualTo(new[] { "new", "running-old" }));
    }

    [Test]
    public async Task Remove_and_mark_on_an_unknown_id_are_no_ops()
    {
        var grain = CreateGrain();
        await grain.AddAsync("a", "k", At(0));
        var writes = _state.WriteCount;

        await grain.RemoveAsync("absent");
        await grain.MarkFinishedAsync("absent", At(1));
        await grain.RemoveAsync("a");

        Assert.Multiple(() =>
        {
            Assert.That(_state.WriteCount, Is.EqualTo(writes + 1));
            Assert.That(_state.State.Entries, Is.Empty);
        });
    }
}
