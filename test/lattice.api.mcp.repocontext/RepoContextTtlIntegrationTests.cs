using System.Text;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Behavioural tests that let a repository-context memory entry opt into the
/// per-entry TTL that Orleans.Lattice core already provides, exercised over the
/// real in-memory cluster stood up by <see cref="RepoContextMcpHarness"/> via
/// <see cref="ILattice.SetAsync(string, byte[], TimeSpan, System.Threading.CancellationToken)"/>.
/// <para>
/// These assert the surfacing contract this issue delivers - a TTL'd memory entry
/// disappears from every read once it expires, a fresh TTL extends life (the
/// later HLC wins under last-writer-wins), a structural entry written without a
/// TTL never expires, and <see cref="RepoContextRemainingLife"/> reads the stored
/// expiry back correctly - not a new expiry mechanism (that is core's).
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextTtlIntegrationTests
{
    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static ILattice MemoryTree(RepoContextMcpHarness harness) =>
        harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);

    private static ILattice StructuralTree(RepoContextMcpHarness harness) =>
        harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);

    [Test]
    public async Task Memory_entry_with_a_ttl_becomes_invisible_after_expiry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var tree = MemoryTree(harness);
        var repoId = $"repo-{Guid.NewGuid():N}";
        var key = RepoContextKeys.Memory(repoId, "scratch", "note-1");

        await tree.SetAsync(key, Bytes("ephemeral"), TimeSpan.FromSeconds(2));

        // Live immediately after the write.
        Assert.That(await tree.GetAsync(key), Is.Not.Null,
            "A TTL'd memory entry must be readable before its TTL elapses.");

        await Task.Delay(TimeSpan.FromMilliseconds(2500));

        // Gone from every read surface once the TTL has elapsed (and thus reaped
        // by core's background tombstone compaction after the grace period).
        var afterGet = await tree.GetAsync(key);
        var afterExists = await tree.ExistsAsync(key);
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync().WithCancellation(TestContext.CurrentContext.CancellationToken))
        {
            keys.Add(k);
        }

        Assert.Multiple(() =>
        {
            Assert.That(afterGet, Is.Null, "GetAsync must hide an expired entry.");
            Assert.That(afterExists, Is.False, "ExistsAsync must hide an expired entry.");
            Assert.That(keys, Does.Not.Contain(key), "KeysAsync must omit an expired entry.");
        });
    }

    [Test]
    public async Task Re_setting_with_a_fresh_ttl_extends_the_entry_life()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var tree = MemoryTree(harness);
        var repoId = $"repo-{Guid.NewGuid():N}";
        var key = RepoContextKeys.Memory(repoId, "scratch", "renewed");

        // Short TTL, then renewed with a long TTL before the first one elapses.
        await tree.SetAsync(key, Bytes("v1"), TimeSpan.FromMilliseconds(300));
        await tree.SetAsync(key, Bytes("v2"), TimeSpan.FromHours(1));

        // Wait well past the original short TTL: the later HLC (the renewal) wins
        // under LWW, so the entry is still live and holds the renewed value.
        await Task.Delay(TimeSpan.FromMilliseconds(700));

        var value = await tree.GetAsync(key);
        Assert.Multiple(() =>
        {
            Assert.That(value, Is.Not.Null, "A renewed entry must outlive its original short TTL.");
            Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("v2"),
                "The renewal's value must win under last-writer-wins.");
        });
    }

    [Test]
    public async Task Structural_entry_written_without_a_ttl_never_expires()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var tree = StructuralTree(harness);
        var repoId = $"repo-{Guid.NewGuid():N}";
        var key = RepoContextKeys.Repo(repoId);

        await tree.SetAsync(key, Bytes("durable"));

        // Wait past the lifetime a short-lived memory entry would have had; a
        // structural entry carries no expiry, so it remains live.
        await Task.Delay(TimeSpan.FromMilliseconds(700));

        var value = await tree.GetAsync(key);
        Assert.That(value, Is.Not.Null, "A structural entry written without a TTL must never expire.");
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("durable"));
    }

    [Test]
    public async Task Remaining_life_helper_reports_life_from_the_stored_expiry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var tree = MemoryTree(harness);
        var repoId = $"repo-{Guid.NewGuid():N}";
        var ttlKey = RepoContextKeys.Memory(repoId, "scratch", "ttl");
        var durableKey = RepoContextKeys.Memory(repoId, "scratch", "durable");

        await tree.SetAsync(ttlKey, Bytes("expiring"), TimeSpan.FromHours(1));
        await tree.SetAsync(durableKey, Bytes("permanent"));

        var ttlValue = await tree.GetWithVersionAsync(ttlKey);
        var durableValue = await tree.GetWithVersionAsync(durableKey);

        var now = DateTime.UtcNow;
        var ttlLife = RepoContextRemainingLife.FromVersionedValue(ttlValue, now);
        var durableLife = RepoContextRemainingLife.FromVersionedValue(durableValue, now);

        Assert.Multiple(() =>
        {
            Assert.That(ttlLife.Expires, Is.True, "A TTL'd entry projects a finite expiry.");
            Assert.That(ttlLife.HasExpired, Is.False);
            // The absolute expiry was stamped ~1h out at write time; allow slack
            // for the write/read round-trip.
            Assert.That(ttlLife.Remaining, Is.GreaterThan(TimeSpan.FromMinutes(55)));
            Assert.That(ttlLife.Remaining, Is.LessThanOrEqualTo(TimeSpan.FromHours(1)));
            Assert.That(ttlLife.ExpiresAtUtc, Is.Not.Null);

            Assert.That(durableLife.Expires, Is.False, "An entry written without a TTL never expires.");
            Assert.That(durableLife, Is.EqualTo(RepoContextRemainingLife.NeverExpires));
        });
    }
}
