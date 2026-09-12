using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the credential the sweep lists and arms under (issue #2406).
/// <para>
/// The sweep is a <c>BackgroundService</c> loop, not a request, so it carries no
/// ambient caller credential of its own. On a host running a default-deny access
/// gate that does <b>not</b> surface as an error: a denied range read is enforced
/// as a reject-all key filter rather than an exception, so the listing scan
/// returns an <b>empty</b> list cleanly and the sweep reports "nothing to arm" on
/// every pass forever - while every credentialed caller in the same process sees
/// the full set. The approximate index is then never built and every semantic
/// query falls back to an exact brute-force scan.
/// </para>
/// <para>
/// These fixtures pin the remedy: the sweep stamps the
/// <see cref="IRepoIndexRunAuthority"/>'s fixed identity for the whole pass, the
/// same way <c>RepoIndexRunner</c>, <c>RepoContextSelfIndexGrain</c>, and
/// <c>RepoContextGitSourceArmingService</c> already do.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    private const string RunSubject = "local-agent";
    private const string RunScheme = "local-trusted";

    /// <summary>
    /// An authority that supplies the fixed local-agent identity, mirroring the
    /// container's <c>LocalTrustedRunAuthority</c>.
    /// </summary>
    private static IRepoIndexRunAuthority RunAuthority()
    {
        var authority = Substitute.For<IRepoIndexRunAuthority>();
        authority.Resolve().Returns(
            new LatticeCredential(RunSubject, scheme: RunScheme, principalId: RunSubject));
        return authority;
    }

    /// <summary>
    /// A grain factory whose tree behaves like a tree behind a <b>default-deny</b>
    /// access gate: it yields its keys only to a caller carrying the seeded
    /// subject's credential, and yields <b>nothing at all</b> - cleanly, with no
    /// exception - to an anonymous one. That is precisely what
    /// <c>LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync</c> does on the
    /// scan path when the caller is denied, and it is why the live defect presented
    /// as a clean empty rather than a fault.
    /// </summary>
    /// <param name="observedSubjects">Receives the subject id seen on each listing scan, or <c>null</c> for an anonymous one.</param>
    /// <param name="repoIds">The repositories registered in the store.</param>
    private static IGrainFactory GatedGrainFactoryListing(
        List<string?> observedSubjects, params string[] repoIds)
    {
        var records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var repoId in repoIds)
        {
            records[RepoContextKeys.Repo(repoId)] =
                Serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        }

        var empty = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);

        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ReturnsForAnyArgs(call =>
        {
            // Read the ambient credential at the moment of the call, exactly where
            // the real gate resolves the caller subject.
            var subject = LatticeCredentialContext.Current?.Token;
            lock (observedSubjects)
            {
                observedSubjects.Add(subject);
            }

            var admitted = string.Equals(subject, RunSubject, StringComparison.Ordinal);
            return Keys(admitted ? records : empty, call.ArgAt<string?>(0), call.ArgAt<string?>(1));
        });
        tree.EntriesAsync().ReturnsForAnyArgs(call => Entries(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
                Task.FromResult(records.TryGetValue(call.ArgAt<string>(0), out var value) ? value : null));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);
        return grainFactory;
    }

    [Test]
    public async Task The_sweep_lists_the_repositories_actually_registered_when_a_run_authority_supplies_its_credential()
    {
        // The defect this pins: the sweep listed ZERO repositories on every pass
        // while the same ListRepoIdsAsync returned the full set to every other
        // caller in the same process. The difference was never the method, the
        // tree, or the key range - all three are identical. It was that the sweep
        // presented no subject the gate could authorize, and the gate answers that
        // with an empty result rather than a fault.
        var observed = new List<string?>();
        var grainFactory = GatedGrainFactoryListing(observed, "alpha", "beta");

        var sweep = Sweep(
            Store(grainFactory), Scheduler(grainFactory), runAuthority: RunAuthority());
        await sweep.StartAsync(Ct);
        try
        {
            var armed = await WaitForAsync(() => sweep.Reporter.Read().Armed >= 1, Ct);
            Assert.Multiple(() =>
            {
                Assert.That(armed, Is.True,
                    "the sweep must receive the repositories that are actually registered, "
                    + "so it arms a coordinator instead of reporting an empty store forever");
                Assert.That(sweep.Reporter.Read().Empty, Is.Zero,
                    "a store holding two repositories must never be swept as empty");
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }

        lock (observed)
        {
            Assert.That(observed, Is.Not.Empty, "the sweep must have listed at least once");
            Assert.That(observed, Has.All.EqualTo(RunSubject),
                "every listing scan must carry the run authority's subject; an anonymous one "
                + "is silently filtered to nothing by a default-deny gate");
        }
    }

    [Test]
    public async Task The_sweep_arms_a_coordinator_for_every_repository_the_gate_admits_under_its_run_credential()
    {
        // The consequence half: it is not enough that the ids are listed, the
        // arming calls they drive must carry the same subject, or the coordinator
        // write fails closed just as the read did.
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var alpha = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        var beta = Substitute.For<IRepoContextAnnIndexBuildGrain>();

        var observed = new List<string?>();
        var grainFactory = GatedGrainFactoryListing(observed, "alpha", "beta");
        grainFactory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("alpha", space)).Returns(alpha);
        grainFactory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("beta", space)).Returns(beta);

        var sweep = Sweep(
            Store(grainFactory), Scheduler(grainFactory), runAuthority: RunAuthority());
        await sweep.StartAsync(Ct);
        try
        {
            var armed = await WaitForAsync(
                () => alpha.ReceivedCalls().Any() && beta.ReceivedCalls().Any(), Ct);
            Assert.That(armed, Is.True,
                "every registered repository must be armed, not merely enumerated");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }

        await alpha.Received().EnsureBuildingAsync(space);
        await beta.Received().EnsureBuildingAsync(space);
    }

    [Test]
    public async Task The_sweep_restores_the_ambient_credential_it_found_once_the_pass_ends()
    {
        // The scope must not leak. This is a specificity guard rather than a
        // detector: it holds both before and after the fix, and its job is to prove
        // the remedy does not re-globalise credential state onto the host thread.
        var observed = new List<string?>();
        var grainFactory = GatedGrainFactoryListing(observed, "alpha");

        Assert.That(LatticeCredentialContext.Current, Is.Null,
            "positive control: the ambient credential must start clear, or this "
            + "fixture could not tell a leak from its own setup");

        var sweep = Sweep(
            Store(grainFactory), Scheduler(grainFactory), runAuthority: RunAuthority());
        await sweep.StartAsync(Ct);
        try
        {
            await WaitForAsync(() => sweep.Reporter.Read().Armed >= 1, Ct);
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }

        Assert.That(LatticeCredentialContext.Current, Is.Null,
            "the sweep's run credential must not escape onto the calling context");
    }
}
