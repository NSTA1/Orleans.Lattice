using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins that every atomic-write entry point on <see cref="LatticeGrain"/> calls
/// its saga under a decide-by deadline derived from the silo's response
/// timeout (<see cref="LatticeSagaDecisionDeadlineContext"/>), the deadline the
/// saga refuses to commit past. Without it a saga the routing tier re-issued
/// after a transient fault committed after its caller had timed out and moved
/// on, so the caller's next batch read back at this one's older values.
/// </summary>
[TestFixture]
public class LatticeGrainSagaDecisionDeadlineTests
{
    private const string TreeId = "orders";
    private static readonly TimeSpan ResponseTimeout = TimeSpan.FromSeconds(30);

    private static (LatticeGrain Grain, IAtomicWriteGrain Saga) CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.RouteEntriesThroughAliasAndMapStubs(_ => new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 });

        var saga = Substitute.For<IAtomicWriteGrain>();
        grainFactory.GetGrain<IAtomicWriteGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(saga);

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(IOptions<SiloMessagingOptions>))
            .Returns(Options.Create(new SiloMessagingOptions { ResponseTimeout = ResponseTimeout }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, saga);
    }

    private static void AssertDeadlineFromResponseTimeout(long? observed, DateTime before, DateTime after)
    {
        Assert.That(observed, Is.Not.Null, "The saga call must carry a decide-by deadline.");
        var deadline = new DateTime(observed!.Value, DateTimeKind.Utc);
        Assert.That(deadline, Is.InRange(before + ResponseTimeout - TimeSpan.FromSeconds(3), after + ResponseTimeout - TimeSpan.FromSeconds(3)),
            "The deadline is the response timeout less a tenth of it.");
        Assert.That(LatticeSagaDecisionDeadlineContext.Current, Is.Null,
            "The ambient deadline must not outlive the saga call.");
    }

    [Test]
    public async Task SetManyAtomicAsync_calls_the_saga_under_a_deadline_from_the_response_timeout()
    {
        var (grain, saga) = CreateGrain();
        long? observed = null;
        saga.ExecuteAsync(Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<List<bool>?>())
            .Returns(_ =>
            {
                observed = LatticeSagaDecisionDeadlineContext.Current;
                return Task.CompletedTask;
            });

        var before = DateTime.UtcNow;
        await grain.SetManyAtomicAsync([new("k", [1])]);
        var after = DateTime.UtcNow;

        AssertDeadlineFromResponseTimeout(observed, before, after);
    }

    [Test]
    public async Task SetManyAtomicAsync_with_operation_id_calls_the_saga_under_a_deadline()
    {
        var (grain, saga) = CreateGrain();
        long? observed = null;
        saga.ExecuteAsync(Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<List<bool>?>())
            .Returns(_ =>
            {
                observed = LatticeSagaDecisionDeadlineContext.Current;
                return Task.CompletedTask;
            });

        var before = DateTime.UtcNow;
        await grain.SetManyAtomicAsync([new("k", [1])], "op-1");
        var after = DateTime.UtcNow;

        AssertDeadlineFromResponseTimeout(observed, before, after);
    }

    [Test]
    public async Task SetManyAtomicWhereAsync_calls_the_guarded_saga_under_a_deadline()
    {
        var (grain, saga) = CreateGrain();
        long? observed = null;
        saga.ExecuteGuardedAsync(Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<LatticePredicateNode>())
            .Returns(_ =>
            {
                observed = LatticeSagaDecisionDeadlineContext.Current;
                return Task.FromResult(AtomicWriteOutcome.Committed);
            });

        var before = DateTime.UtcNow;
        await grain.SetManyAtomicWhereAsync([new("k", [1])], LatticePredicateNode.Member("Score"));
        var after = DateTime.UtcNow;

        AssertDeadlineFromResponseTimeout(observed, before, after);
    }
}
