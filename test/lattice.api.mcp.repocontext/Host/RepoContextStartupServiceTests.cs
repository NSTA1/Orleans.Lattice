using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextGrant"/> and the seed step of
/// <see cref="RepoContextStartupService"/>: the granted operation mask matches the
/// repository-context tool surface, and warmup seeds exactly one Allow rule per
/// tree scoped to that tree for the local agent.
/// </summary>
[TestFixture]
public sealed class RepoContextStartupServiceTests
{
    [Test]
    public void Grant_covers_the_full_repository_context_data_plane_mask()
    {
        const LatticeOperation expected =
            LatticeOperation.Read
            | LatticeOperation.Write
            | LatticeOperation.Delete
            | LatticeOperation.RangeRead
            | LatticeOperation.RangeDelete
            | LatticeOperation.CrdtApply
            | LatticeOperation.AtomicWrite
            | LatticeOperation.BulkLoad;

        Assert.That(RepoContextGrant.Operations, Is.EqualTo(expected));
    }

    [Test]
    public async Task SeedAccessAsync_puts_one_allow_rule_per_tree_for_the_local_agent()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var captured = new List<LatticeAuthorizationRule>();
        await store.PutRuleAsync(
            Arg.Do<LatticeAuthorizationRule>(captured.Add),
            Arg.Any<CancellationToken>());

        var service = CreateService(store);

        await service.SeedAccessAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(captured, Has.Count.EqualTo(RepoContextHostTrees.All.Count));
            Assert.That(
                captured.Select(r => r.Scope.TreeId),
                Is.EquivalentTo(RepoContextHostTrees.All));
            Assert.That(captured, Has.All.Property(nameof(LatticeAuthorizationRule.Effect)).EqualTo(LatticeEffect.Allow));
            Assert.That(
                captured,
                Has.All.Property(nameof(LatticeAuthorizationRule.Operations)).EqualTo(RepoContextGrant.Operations));
            Assert.That(
                captured,
                Has.All.Property(nameof(LatticeAuthorizationRule.Subject))
                    .Property(nameof(LatticeSubjectSelector.Id)).EqualTo(LocalTrustedAgent.SubjectId));
        });
    }

    [Test]
    public async Task WarmupAsync_marks_ready_after_a_successful_seed()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var readiness = new RepoContextReadinessState();
        var service = CreateService(store, readiness);

        await service.WarmupAsync(CancellationToken.None);

        Assert.That(readiness.IsReady, Is.True);
    }

    [Test]
    public async Task SeedAccessAsync_opts_the_symbol_tree_in_to_schema_versioning_when_unversioned()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        admin.GetVersionConfigAsync(RepoContextHostTrees.Symbol, Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);

        var service = CreateService(store, admin: admin);
        await service.SeedAccessAsync(CancellationToken.None);

        await admin.Received(1).SetVersionConfigAsync(
            RepoContextHostTrees.Symbol,
            Arg.Is<LatticeSchemaVersionConfig>(c =>
                c.SchemaId == RepoContextHostTrees.SymbolSchemaId
                && c.TargetVersion == RepoContextHostTrees.SymbolSchemaVersion),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SeedAccessAsync_leaves_an_already_versioned_symbol_tree_untouched()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        admin.GetVersionConfigAsync(RepoContextHostTrees.Symbol, Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaVersionConfig(
                RepoContextHostTrees.SymbolSchemaId, RepoContextHostTrees.SymbolSchemaVersion));

        var service = CreateService(store, admin: admin);
        await service.SeedAccessAsync(CancellationToken.None);

        await admin.DidNotReceive().SetVersionConfigAsync(
            Arg.Any<string>(), Arg.Any<LatticeSchemaVersionConfig>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var logger = Substitute.For<ILogger<RepoContextStartupService>>();
        var readiness = new RepoContextReadinessState();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextStartupService(null!, admin, readiness, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, null!, readiness, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, admin, null!, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, admin, readiness, null!, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextStartupService(store, admin, readiness, lifetime, null!),
                Throws.ArgumentNullException);
        });
    }

    private static RepoContextStartupService CreateService(
        ILatticeAuthorizationPolicyStore store,
        RepoContextReadinessState? readiness = null,
        ILatticeSchemaVersionAdmin? admin = null)
        => new(
            store,
            admin ?? Substitute.For<ILatticeSchemaVersionAdmin>(),
            readiness ?? new RepoContextReadinessState(),
            Substitute.For<IHostApplicationLifetime>(),
            Substitute.For<ILogger<RepoContextStartupService>>());
}
