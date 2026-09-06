using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Direct unit coverage for <see cref="LeafReplayCoordinatorGrain"/>, the
/// per-<c>{treeId}/{shardIndex}</c> commit-log slice coordinator that leaves share
/// when they activate against the same shard. The grain is constructed directly
/// (rather than through a cluster) so the key-shape guards, the argument guards,
/// the slice cache, and the read-failure arms can each be driven in isolation.
/// </summary>
[TestFixture]
public class LeafReplayCoordinatorGrainTests
{
    private const string TreeId = "orders";

    private static LatticeMutation Mutation(string key) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
    };

    /// <summary>
    /// Builds the grain over a substituted <see cref="ICommitLogReader"/> resolved
    /// from the activation service provider, exactly as production DI supplies it.
    /// </summary>
    private static (LeafReplayCoordinatorGrain Grain, ICommitLogReader Reader) CreateGrain(
        string key = TreeId + "/3",
        ICommitLogReader? reader = null)
    {
        reader ??= Substitute.For<ICommitLogReader>();

        var services = new ServiceCollection();
        services.AddSingleton(reader);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leafreplaycoordinator", key));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        return (new LeafReplayCoordinatorGrain(context, NullLogger<LeafReplayCoordinatorGrain>.Instance), reader);
    }

    /// <summary>
    /// Async feed helper: yields the supplied offsets in ascending order, recording
    /// how many entries the coordinator actually pulled so early-exit behaviour
    /// (budget reached, past the inclusive ceiling) is observable rather than
    /// inferred from the returned slice alone.
    /// </summary>
    private static async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> Feed(
        List<long> pulled,
        params long[] offsets)
    {
        foreach (var offset in offsets)
        {
            pulled.Add(offset);
            yield return (offset, Mutation($"k{offset}"));
            await Task.Yield();
        }
    }

    private static void StubRead(
        ICommitLogReader reader,
        List<long> pulled,
        params long[] offsets) =>
        reader.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ => Feed(pulled, offsets));

    // ---------------------------------------------------------------- key shape

    [Test]
    public void ReadSlice_rejects_a_key_with_no_shard_separator()
    {
        var (grain, _) = CreateGrain(key: "orders");

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("{treeId}/{shardIndex}"),
            "A key with no '/' cannot be split into a tree id and a shard index.");
    }

    [Test]
    public void ReadSlice_rejects_a_key_whose_separator_is_leading()
    {
        // sep == 0 means there is no tree-id segment at all.
        var (grain, _) = CreateGrain(key: "/3");

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("{treeId}/{shardIndex}"));
    }

    [Test]
    public void ReadSlice_rejects_a_key_whose_separator_is_trailing()
    {
        // sep == key.Length - 1 means there is no shard segment at all.
        var (grain, _) = CreateGrain(key: "orders/");

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("{treeId}/{shardIndex}"));
    }

    [Test]
    public void ReadSlice_rejects_a_non_integer_shard_segment()
    {
        var (grain, _) = CreateGrain(key: "orders/abc");

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("non-integer or negative shard segment"));
    }

    [Test]
    public void ReadSlice_rejects_a_negative_shard_segment()
    {
        var (grain, _) = CreateGrain(key: "orders/-2");

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("non-integer or negative shard segment"));
    }

    [Test]
    public async Task A_multi_segment_tree_id_binds_on_the_LAST_separator()
    {
        // Tenanted tree ids embed '/', so the shard split must be the last
        // separator rather than the first.
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain(key: "tenant-a/orders/7");
        StubRead(reader, pulled, 0);

        await grain.ReadSliceAsync(-1, 10, budget: 8);

        reader.Received(1).ReadAsync("tenant-a/orders", 7, -1, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Bindings_are_parsed_once_and_reused_across_calls()
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        StubRead(reader, pulled, 0);
        reader.GetHeadOffsetAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(9L);

        await grain.ReadSliceAsync(-1, 10, budget: 8);
        var head = await grain.GetHeadOffsetAsync();

        Assert.That(head, Is.EqualTo(9L));
        await reader.Received(1).GetHeadOffsetAsync(TreeId, 3, Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------ arg guards

    [TestCase(0)]
    [TestCase(-1)]
    public void ReadSlice_rejects_a_non_positive_budget(int budget)
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget),
            Throws.InstanceOf<ArgumentOutOfRangeException>()
                .With.Property("ParamName").EqualTo("budget"));
    }

    [Test]
    public void ReadSlice_rejects_an_offset_below_minus_one()
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.ReadSliceAsync(-2, 10, budget: 8),
            Throws.InstanceOf<ArgumentOutOfRangeException>()
                .With.Property("ParamName").EqualTo("fromOffsetExclusive"));
    }

    [Test]
    public async Task An_inverted_range_returns_an_empty_slice_without_reading()
    {
        var (grain, reader) = CreateGrain();

        var slice = await grain.ReadSliceAsync(fromOffsetExclusive: 10, toOffsetInclusive: 5, budget: 8);

        Assert.That(slice, Is.Empty);
        reader.DidNotReceive().ReadAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ReadSlice_observes_an_already_cancelled_token_before_any_guard()
    {
        // Cancellation is checked first, so it wins even over an invalid budget.
        var (grain, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // ------------------------------------------------------------------ reads

    [Test]
    public async Task A_slice_yields_every_entry_within_the_inclusive_ceiling()
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        StubRead(reader, pulled, 0, 1, 2);

        var slice = await grain.ReadSliceAsync(-1, 2, budget: 8);

        Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
        Assert.That(slice[0].Mutation.Key, Is.EqualTo("k0"));
    }

    [Test]
    public async Task An_entry_past_the_inclusive_ceiling_stops_the_walk()
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        StubRead(reader, pulled, 0, 1, 2, 3, 4);

        var slice = await grain.ReadSliceAsync(-1, toOffsetInclusive: 1, budget: 99);

        Assert.Multiple(() =>
        {
            Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }),
                "Entries beyond the inclusive ceiling must be excluded.");
            Assert.That(pulled, Is.EqualTo(new[] { 0L, 1L, 2L }),
                "The walk must break on the first over-ceiling entry rather than draining the feed.");
        });
    }

    [Test]
    public async Task The_budget_caps_the_slice_and_stops_the_walk()
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        StubRead(reader, pulled, 0, 1, 2, 3, 4);

        var slice = await grain.ReadSliceAsync(-1, 99, budget: 2);

        Assert.Multiple(() =>
        {
            Assert.That(slice, Has.Count.EqualTo(2));
            Assert.That(pulled, Is.EqualTo(new[] { 0L, 1L }),
                "Reaching the budget must break the walk, not merely truncate the result.");
        });
    }

    [Test]
    public async Task An_empty_feed_yields_an_empty_slice()
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        StubRead(reader, pulled);

        Assert.That(await grain.ReadSliceAsync(-1, 10, budget: 8), Is.Empty);
    }

    // ------------------------------------------------------------- read faults

    [Test]
    public void A_cancelled_read_rethrows_without_being_reclassified()
    {
        var (grain, reader) = CreateGrain();
        reader.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ => Throwing(new OperationCanceledException()));

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<OperationCanceledException>(),
            "The dedicated OperationCanceledException arm must rethrow, not fall into the general catch.");
    }

    [Test]
    public void A_failed_read_is_logged_and_rethrown()
    {
        var (grain, reader) = CreateGrain();
        reader.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ => Throwing(new InvalidOperationException("wal unavailable")));

        Assert.That(
            async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("wal unavailable"));
    }

    [Test]
    public void A_failed_read_is_not_cached_so_the_next_call_retries()
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        var firstCall = true;
        reader.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (firstCall)
                {
                    firstCall = false;
                    return Throwing(new InvalidOperationException("transient"));
                }
                return Feed(pulled, 0, 1);
            });

        Assert.That(async () => await grain.ReadSliceAsync(-1, 10, budget: 8),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(async () => (await grain.ReadSliceAsync(-1, 10, budget: 8)).Count,
            Is.EqualTo(2), "A faulted read must leave no cache entry behind.");
    }

    private static async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> Throwing(Exception ex)
    {
        await Task.Yield();
        throw ex;
#pragma warning disable CS0162 // Unreachable - required to make this a valid iterator.
        yield break;
#pragma warning restore CS0162
    }

    // ------------------------------------------------------------- slice cache

    [Test]
    public async Task An_identical_range_is_served_from_the_slice_cache()
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        StubRead(reader, pulled, 0, 1);

        var first = await grain.ReadSliceAsync(-1, 10, budget: 8);
        var second = await grain.ReadSliceAsync(-1, 10, budget: 8);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first), "The cached slice instance must be handed back verbatim.");
            Assert.That(pulled, Has.Count.EqualTo(2), "The second call must not re-read the commit log.");
        });
        reader.Received(1).ReadAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [TestCase(0, 10, TestName = "A_different_from_offset_bypasses_the_slice_cache")]
    [TestCase(-1, 11, TestName = "A_different_to_offset_bypasses_the_slice_cache")]
    public async Task A_deviating_range_bypasses_the_slice_cache(long from, long to)
    {
        var pulled = new List<long>();
        var (grain, reader) = CreateGrain();
        StubRead(reader, pulled, 0, 1);

        await grain.ReadSliceAsync(-1, 10, budget: 8);
        await grain.ReadSliceAsync(from, to, budget: 8);

        reader.Received(2).ReadAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    // ---------------------------------------------------------------- offsets

    [Test]
    public async Task GetHeadOffset_forwards_the_parsed_bindings_to_the_reader()
    {
        var (grain, reader) = CreateGrain(key: "orders/11");
        reader.GetHeadOffsetAsync("orders", 11, Arg.Any<CancellationToken>()).Returns(42L);

        Assert.That(await grain.GetHeadOffsetAsync(), Is.EqualTo(42L));
    }

    [Test]
    public async Task GetTailOffset_forwards_the_parsed_bindings_to_the_reader()
    {
        var (grain, reader) = CreateGrain(key: "orders/11");
        reader.GetTailOffsetAsync("orders", 11, Arg.Any<CancellationToken>()).Returns(7L);

        Assert.That(await grain.GetTailOffsetAsync(), Is.EqualTo(7L));
    }

    [Test]
    public void GetHeadOffset_observes_an_already_cancelled_token()
    {
        var (grain, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(async () => await grain.GetHeadOffsetAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void GetTailOffset_observes_an_already_cancelled_token()
    {
        var (grain, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(async () => await grain.GetTailOffsetAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void GetHeadOffset_rejects_a_malformed_key()
    {
        var (grain, _) = CreateGrain(key: "orders");

        Assert.That(async () => await grain.GetHeadOffsetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetTailOffset_rejects_a_malformed_key()
    {
        var (grain, _) = CreateGrain(key: "orders");

        Assert.That(async () => await grain.GetTailOffsetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ------------------------------------------------------------ grain context

    [Test]
    public void GrainContext_exposes_the_injected_context()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ICommitLogReader>());
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leafreplaycoordinator", TreeId + "/0"));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grain = new LeafReplayCoordinatorGrain(context, NullLogger<LeafReplayCoordinatorGrain>.Instance);

        Assert.That(((IGrainBase)grain).GrainContext, Is.SameAs(context));
    }
}
