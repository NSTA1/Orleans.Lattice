using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

public sealed partial class RepoContextAnnIndexBuildGrainTests
{
    [Test]
    public async Task Phase_tick_unknown_count_keeps_retry_armed_until_completed_count_then_stands_down()
    {
        var clock = new AdvanceableTimeProvider();
        var durable = new Durable { Plane = PlaneOptions(clock) };
        durable.SeedRing(16);
        using (var first = durable.Start())
        {
            await first.Grain.EnsureBuildingAsync(Space);
            Assert.That(await PumpAsync(first), Is.LessThan(MaxTicks));
        }
        var source = durable.Backing.For(RepoId, Space).Source;
        var vector = new float[Space.Dimension];
        vector[2] = 1f;
        source.Set("vec-999999", RepoContextKeys.File(RepoId, "src/Late.cs"), vector);
        source.FailNextCounts(2, static () =>
            new RepoContextCountBudgetExceededException(RepoId, 12, TimeSpan.FromSeconds(10)));
        var counts = source.CountCalls;
        var enumerations = source.FullEnumerations;
        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);
        var tick = CapturedTick(process.Timers);
        var ct = TestContext.CurrentContext.CancellationToken;
        await tick(ct);
        Assert.That(source.CountCalls, Is.EqualTo(++counts));
        for (var i = 0; i < 10; i++)
            await tick(ct);
        Assert.That(source.CountCalls, Is.EqualTo(counts));

        clock.Advance(TimeSpan.FromMinutes(1));
        await tick(ct);
        Assert.That(source.CountCalls, Is.EqualTo(++counts),
            "Ready must not retire the real phase timer while count verification is pending.");
        clock.Advance(TimeSpan.FromMinutes(2) - TimeSpan.FromSeconds(1));
        await tick(ct);
        Assert.That(source.CountCalls, Is.EqualTo(counts));
        Assert.That(source.FullEnumerations, Is.EqualTo(enumerations));
        clock.Advance(TimeSpan.FromSeconds(1));
        await tick(ct);
        Assert.Multiple(() =>
        {
            Assert.That(source.CountCalls, Is.EqualTo(++counts));
            Assert.That(source.FullEnumerations, Is.EqualTo(enumerations + 1));
            Assert.That(VectorsIndexed(process), Is.EqualTo(17));
        });
        for (var i = 0; i < 10; i++)
            await tick(ct);
        Assert.That(source.CountCalls, Is.EqualTo(counts),
            "a completed count must restore the existing Ready stand-down behavior");
    }
}
