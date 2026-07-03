using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Shared helpers for reading the durable <c>sys-auth-audit</c> trail back through
/// a bootstrap administrator (which bypasses the enforcement gate).
/// </summary>
internal static class AuditTrailReader
{
    internal static async Task<List<string>> ReadKeysAsync(ILattice auditTree)
    {
        var keys = new List<string>();
        try
        {
            await foreach (var key in auditTree.KeysAsync())
            {
                keys.Add(key);
            }
        }
        catch (Orleans.Runtime.EnumerationAbortedException)
        {
            // A concurrent background audit write can invalidate a streaming
            // enumerator mid-scan; the caller polls, so surface what we have and
            // let the next poll iteration re-read a settled tree.
        }

        return keys;
    }

    internal static async Task<List<string>> PollForKeysAsync(
        Func<ILattice> auditTreeFactory,
        int minimum,
        int timeoutMs = 5000)
    {
        var start = Environment.TickCount64;
        List<string> keys;
        do
        {
            keys = await ReadKeysAsync(auditTreeFactory());
            if (keys.Count >= minimum)
            {
                return keys;
            }

            await Task.Delay(50);
        }
        while (Environment.TickCount64 - start < timeoutMs);

        return keys;
    }
}

/// <summary>
/// End-to-end coverage for the optional durable audit trail: a gated decision is
/// appended to the reserved <c>sys-auth-audit</c> tree as a well-formed
/// <see cref="LatticeAuthDecisionEvent"/> when the trail is enabled.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DurableAuditTrailIntegrationTests
{
    private AuditClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuditClusterFixture(trailTimeToLive: TimeSpan.FromHours(1));
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private ILattice AuditTree()
    {
        // Read the reserved audit tree as the bootstrap admin (gate bypass).
        using (AuditClusterFixture.AsSubject(AuditClusterFixture.BootstrapAdmin))
        {
            return _fixture.Lattice("sys-auth-audit");
        }
    }

    [Test]
    public async Task A_denied_decision_is_appended_to_the_durable_trail()
    {
        var tree = $"trail-deny-{Guid.NewGuid():N}";

        using (AuditClusterFixture.AsSubject("intruder"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("secret", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        }

        List<string> keys;
        LatticeAuthDecisionEvent? found = null;
        using (AuditClusterFixture.AsSubject(AuditClusterFixture.BootstrapAdmin))
        {
            keys = await AuditTrailReader.PollForKeysAsync(() => _fixture.Lattice("sys-auth-audit"), minimum: 1);
            foreach (var key in keys)
            {
                var evt = await _fixture.Lattice("sys-auth-audit").GetAsync<LatticeAuthDecisionEvent>(key);
                if (evt is { } value && value.SubjectId == "intruder")
                {
                    found = value;
                    break;
                }
            }
        }

        Assert.That(found, Is.Not.Null, "the denied decision must be recorded in the durable audit trail");
        Assert.Multiple(() =>
        {
            Assert.That(found!.Value.Effect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(found!.Value.TreeId, Is.EqualTo(tree));
            Assert.That(found!.Value.Operation, Is.EqualTo(LatticeOperation.Write));
        });
    }
}

/// <summary>
/// Coverage that the durable trail honours a configured time-to-live: with a short
/// TTL the appended rows expire and drop out of the tree on read.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DurableAuditTrailTtlIntegrationTests
{
    private AuditClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuditClusterFixture(trailTimeToLive: TimeSpan.FromMilliseconds(500));
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task Durable_trail_rows_expire_after_the_configured_ttl()
    {
        var tree = $"trail-ttl-{Guid.NewGuid():N}";

        using (AuditClusterFixture.AsSubject("ttl-intruder"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("k", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        }

        using (AuditClusterFixture.AsSubject(AuditClusterFixture.BootstrapAdmin))
        {
            var keys = await AuditTrailReader.PollForKeysAsync(() => _fixture.Lattice("sys-auth-audit"), minimum: 1);
            Assert.That(keys, Is.Not.Empty, "the row must be present before its TTL elapses");

            // Wait past the short TTL and confirm the rows are reaped on read.
            await Task.Delay(TimeSpan.FromMilliseconds(900));
            var afterExpiry = await AuditTrailReader.ReadKeysAsync(_fixture.Lattice("sys-auth-audit"));
            Assert.That(afterExpiry, Is.Empty, "durable audit rows must expire once their TTL has elapsed");
        }
    }
}

/// <summary>
/// Coverage that the durable trail is off by default and costs nothing: with the
/// default options (audit sink and durable trail disabled) a gated decision writes
/// no row to the <c>sys-auth-audit</c> tree.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DurableAuditTrailDisabledIntegrationTests
{
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task With_audit_disabled_no_row_is_written_to_the_audit_tree()
    {
        var tree = $"trail-off-{Guid.NewGuid():N}";

        using (AuthClusterFixture.AsSubject("nobody"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("k", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        }

        // Give any (erroneously scheduled) background dispatch time to land.
        await Task.Delay(300);

        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var keys = new List<string>();
            await foreach (var key in _fixture.Lattice("sys-auth-audit").KeysAsync())
            {
                keys.Add(key);
            }

            Assert.That(keys, Is.Empty, "the durable trail is opt-in: nothing must be written when it is disabled");
        }
    }
}
