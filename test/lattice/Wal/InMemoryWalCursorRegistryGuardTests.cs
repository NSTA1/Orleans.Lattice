using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Branch-coverage tests for <see cref="InMemoryWalCursorRegistry"/> guard
/// clauses and vector-merge / causal-stable-meet paths that the invariant
/// suite does not reach: argument validation on the report overloads, the
/// blocked-floor overload's relaxed cursor precondition, per-consumer vector
/// coalescing, and the two-consumer pointwise-min causal-stable intersection.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class InMemoryWalCursorRegistryGuardTests
{
    private const string Tree = "tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static VersionVector Vector(string origin, HybridLogicalClock clock)
    {
        var v = new VersionVector();
        v.Entries[origin] = clock;
        return v;
    }

    [Test]
    public void ReportCursorAsync_null_vector_with_blocked_floor_throws()
    {
        var registry = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await registry.ReportCursorAsync(
                Tree, "peer", Hlc(10), (VersionVector)null!, blockedAtHlc: Hlc(5), CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReportCursorAsync_negative_cursor_throws()
    {
        var registry = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await registry.ReportCursorAsync(
                Tree, "peer", Hlc(-1), blockedAtHlc: Hlc(5), CancellationToken.None),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReportCursorAsync_zero_cursor_without_blocked_floor_throws()
    {
        var registry = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await registry.ReportCursorAsync(
                Tree, "peer", HybridLogicalClock.Zero, CancellationToken.None),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReportCursorAsync_negative_blocked_floor_throws()
    {
        var registry = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await registry.ReportCursorAsync(
                Tree, "peer", Hlc(10), blockedAtHlc: Hlc(-1), CancellationToken.None),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReportCursorAsync_vector_after_vectorless_report_adopts_the_vector()
    {
        var registry = new InMemoryWalCursorRegistry();
        // First report carries no vector, so the consumer's cached vector is null.
        await registry.ReportCursorAsync(Tree, "peer", Hlc(10), CancellationToken.None);
        // Second report carries a vector: the null cached vector must adopt the clone.
        await registry.ReportCursorAsync(Tree, "peer", Hlc(20), Vector("origin-a", Hlc(100)), CancellationToken.None);

        var stable = await registry.GetCausalStableAsync(Tree, CancellationToken.None);
        Assert.That(stable, Is.Not.Null);
        Assert.That(stable!.Entries["origin-a"], Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task ReportCursorAsync_second_vector_merges_pointwise_max()
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer", Hlc(10), Vector("origin-a", Hlc(100)), CancellationToken.None);
        // Re-report with a vector touching a new origin and a higher clock on the
        // existing one: the cached vector must coalesce pointwise-max.
        var second = new VersionVector();
        second.Entries["origin-a"] = Hlc(150);
        second.Entries["origin-b"] = Hlc(50);
        await registry.ReportCursorAsync(Tree, "peer", Hlc(20), second, CancellationToken.None);

        var stable = await registry.GetCausalStableAsync(Tree, CancellationToken.None);
        Assert.That(stable, Is.Not.Null);
        Assert.That(stable!.Entries["origin-a"], Is.EqualTo(Hlc(150)));
        Assert.That(stable.Entries["origin-b"], Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task GetCausalStableAsync_two_consumers_returns_pointwise_min_intersection()
    {
        var registry = new InMemoryWalCursorRegistry();

        var a = new VersionVector();
        a.Entries["shared"] = Hlc(100);
        a.Entries["only-a"] = Hlc(10);
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(10), a, CancellationToken.None);

        var b = new VersionVector();
        b.Entries["shared"] = Hlc(60);
        b.Entries["only-b"] = Hlc(20);
        await registry.ReportCursorAsync(Tree, "peer-B", Hlc(10), b, CancellationToken.None);

        var stable = await registry.GetCausalStableAsync(Tree, CancellationToken.None);
        Assert.That(stable, Is.Not.Null);
        // Only the shared origin survives, at the smaller of the two clocks.
        Assert.That(stable!.Entries["shared"], Is.EqualTo(Hlc(60)));
        Assert.That(stable.Entries.ContainsKey("only-a"), Is.False);
        Assert.That(stable.Entries.ContainsKey("only-b"), Is.False);
    }
}
