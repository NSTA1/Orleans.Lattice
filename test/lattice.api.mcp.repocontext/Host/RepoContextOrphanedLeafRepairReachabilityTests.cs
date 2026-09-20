using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Proves the orphaned-leaf repair is <b>invocable</b> on this container, not merely
/// advertised by it.
/// </summary>
/// <remarks>
/// <para>
/// Advertisement and invocation are separate gates and this epic has already paid for
/// conflating them. Advertisement is decided by the MCP discovery core from the
/// coarse group mask; invocation is decided again, per tree, by the facade's own
/// fail-closed access gate -
/// <c>TreeAdminAccessAuthorizer.AuthorizeTreeLifecycleAsync</c> - reading the rule
/// warmup actually seeded into the live policy tree. A tool can therefore be listed
/// and still refuse every call, which is exactly the state #3289 declined to ship.
/// <see cref="RepoContextTreeAdminToolRegistrationTests"/> covers the first gate; this
/// fixture covers the second, against the real silo, the real default-deny gate, and
/// the real seeded grant rather than a stub authorizer.
/// </para>
/// <para>
/// The repair runs here against a freshly created temporary tree with no orphans, so
/// it is a structural no-op: what is under test is the authorization verdict and the
/// reserved-namespace guard, not the unsplice itself, which
/// <c>ShardRootGrainOrphanRepairTests</c> covers.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class RepoContextOrphanedLeafRepairReachabilityTests
{
    private string _dataRoot = null!;

    private static CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [SetUp]
    public void SetUp()
        => _dataRoot = Path.Combine(Path.GetTempPath(), "repocontext-repair-" + Guid.NewGuid().ToString("N"));

    [TearDown]
    public void TearDown()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(_dataRoot))
        {
            try
            {
                Directory.Delete(_dataRoot, recursive: true);
            }
            catch (IOException)
            {
                // A background flush may briefly hold the WAL file; best-effort cleanup.
            }
        }
    }

    private WebApplication BuildHost()
    {
        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _dataRoot,
                    [RepoContextHostConfiguration.ClusterIdKey] = "repocontext-repair",
                    [RepoContextHostConfiguration.ServiceIdKey] = "repocontext-repair",
                })
                .Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        return RepoContextHostBuilder.Build(builder, config);
    }

    /// <summary>
    /// Waits on the host's real readiness contract, whose lifecycle half flips only
    /// once <c>RepoContextStartupService</c> has seeded the access grant. That is the
    /// precondition every test here needs, and polling the endpoint gets it without
    /// reaching for the hosted service instance, which is registered only as an
    /// <see cref="IHostedService"/> and is not resolvable by its own type.
    /// </summary>
    private static async Task WaitForReadyAsync(WebApplication app)
    {
        using var client = app.GetTestServer().CreateClient();
        var deadline = DateTime.UtcNow.AddSeconds(60);

        HttpStatusCode status;
        do
        {
            status = (await client.GetAsync(RepoContextHostBuilder.ReadinessPath, Ct)).StatusCode;
            if (status == HttpStatusCode.OK)
            {
                return;
            }

            await Task.Delay(100, Ct);
        }
        while (DateTime.UtcNow < deadline);

        Assert.Fail($"The host did not report ready within the timeout; last status was {status}.");
    }

    private static IDisposable AsLocalAgent()
        => LatticeCredentialContext.Use(LocalTrustedAgent.SubjectId, scheme: LocalTrustedAgent.Scheme);

    [Test]
    public async Task The_seeded_grant_authorizes_the_repair_on_a_repository_context_tree()
    {
        var app = BuildHost();
        await app.StartAsync(Ct);
        try
        {
            await WaitForReadyAsync(app);

            var admin = app.Services.GetRequiredService<ILatticeTreeAdmin>();

            using (AsLocalAgent())
            {
                // The vector-membership tree is one of the two the live audit found
                // repairable orphans on, so it is a tree the operator will name.
                var audit = await admin.AuditOrphanedLeavesAsync(RepoContextHostTrees.VectorMembership, cancellationToken: Ct);
                var repair = await admin.RepairOrphanedLeavesAsync(RepoContextHostTrees.VectorMembership, cancellationToken: Ct);

                Assert.Multiple(() =>
                {
                    Assert.That(audit.DryRun, Is.True, "The audit must not mutate.");
                    Assert.That(
                        repair.DryRun,
                        Is.False,
                        "Reaching a non-dry-run report is the proof the lifecycle gate admitted the call rather than refusing it.");
                    Assert.That(repair.TreeId, Is.EqualTo(RepoContextHostTrees.VectorMembership));
                });
            }
        }
        finally
        {
            await app.StopAsync(Ct);
            await app.DisposeAsync();
        }
    }

    [Test]
    public async Task An_ungranted_subject_is_still_refused_the_repair()
    {
        // The complement, so the test above cannot pass merely because the gate is
        // off. The container runs default-deny and seeds a rule for exactly one
        // subject; any other caller must be refused on the same tree and same verb.
        var app = BuildHost();
        await app.StartAsync(Ct);
        try
        {
            await WaitForReadyAsync(app);

            var admin = app.Services.GetRequiredService<ILatticeTreeAdmin>();

            using (LatticeCredentialContext.Use("not-the-local-agent", scheme: LocalTrustedAgent.Scheme))
            {
                Assert.That(
                    async () => await admin.RepairOrphanedLeavesAsync(RepoContextHostTrees.VectorMembership, cancellationToken: Ct),
                    Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
                    "The grant is scoped to one subject; the gate must still be the enforcement seam.");
            }
        }
        finally
        {
            await app.StopAsync(Ct);
            await app.DisposeAsync();
        }
    }
}
