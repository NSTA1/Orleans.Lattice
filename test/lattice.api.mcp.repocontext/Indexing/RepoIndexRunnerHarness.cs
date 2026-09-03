using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// A host-free unit harness for <see cref="RepoIndexRunner"/>: it builds the runner
/// over a <b>real</b> <see cref="RepoContextBootstrapService"/> whose collaborators are
/// substituted, so the runner's own single-flight, cancellation, credential-scoping,
/// and fault-reporting behaviour is exercised against a genuine pass rather than a
/// stubbed one.
/// <para>
/// The bootstrap service is <c>internal sealed</c> and therefore cannot be
/// substituted. The lever that makes it controllable is its very first collaborator
/// call: it resolves the structural tree and streams the repository's stored file
/// records before doing anything else. Backing that stream with a gate lets a test
/// hold a run open (to observe single-flight and drain), release it (to observe a
/// clean settle), fault it (to observe the failure report), or cancel it (to observe
/// that the run deliberately leaves the grain unsettled so the resume reminder can
/// restart it).
/// </para>
/// </summary>
internal sealed class RepoIndexRunnerHarness : IDisposable
{
    /// <summary>The repository id every harness run is keyed by.</summary>
    internal const string RepoId = "acme";

    private static readonly IServiceProvider SerializerServices = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider();

    private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly CancellationTokenSource _applicationStopping = new();
    private readonly string _repoRoot;

    internal RepoIndexRunnerHarness()
    {
        _repoRoot = Path.Combine(Path.GetTempPath(), "lattice-repoindexrunner-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_repoRoot);

        Job = Substitute.For<IRepoIndexJobGrain>();

        StructuralTree = Substitute.For<ILattice>();
        StructuralTree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => GatedEntries(call.ArgAt<CancellationToken>(4)));

        GrainFactory = Substitute.For<IGrainFactory>();
        GrainFactory.GetGrain<IRepoIndexJobGrain>(Arg.Any<string>()).Returns(_ => Job);
        GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(_ => StructuralTree);

        Lifetime = Substitute.For<IHostApplicationLifetime>();
        Lifetime.ApplicationStopping.Returns(_ => _applicationStopping.Token);

        RunAuthority = Substitute.For<IRepoIndexRunAuthority>();
    }

    /// <summary>The substituted job grain every progress, completion, and failure report lands on.</summary>
    internal IRepoIndexJobGrain Job { get; }

    /// <summary>The substituted structural tree whose stored-record stream gates the pass.</summary>
    internal ILattice StructuralTree { get; }

    /// <summary>The substituted grain factory the runner and bootstrap resolve through.</summary>
    internal IGrainFactory GrainFactory { get; }

    /// <summary>The substituted host lifetime whose stopping token bounds every run.</summary>
    internal IHostApplicationLifetime Lifetime { get; }

    /// <summary>The substituted authority the run stamps its ambient credential from.</summary>
    internal IRepoIndexRunAuthority RunAuthority { get; }

    /// <summary>The credential observed on the ambient context inside the pass, if any.</summary>
    internal LatticeCredential? ObservedCredential { get; private set; }

    /// <summary>Builds the runner under test over this harness's collaborators.</summary>
    /// <returns>A freshly constructed runner.</returns>
    internal RepoIndexRunner CreateRunner() => new(
        CreateBootstrapService(),
        GrainFactory,
        Lifetime,
        RunAuthority,
        NullLogger<RepoIndexRunner>.Instance);

    /// <summary>A well-formed job request pointing at this harness's empty working tree.</summary>
    /// <returns>The request a run is enqueued with.</returns>
    internal RepoIndexJobRequest Request() => new() { RepoRoot = _repoRoot, RepoId = RepoId };

    /// <summary>Lets a gated pass proceed past its stored-record read.</summary>
    internal void Release() => _gate.TrySetResult();

    /// <summary>Faults a gated pass so the runner observes an unexpected failure.</summary>
    /// <param name="error">The fault to raise from inside the pass.</param>
    internal void Fault(Exception error) => _gate.TrySetException(error);

    /// <summary>Signals host shutdown, cancelling every linked run.</summary>
    internal void StopApplication() => _applicationStopping.Cancel();

    /// <summary>
    /// Waits for a condition the background run drives, so a test never races the
    /// runner's own task. Returns false if the condition never holds.
    /// </summary>
    /// <param name="condition">The condition to poll.</param>
    /// <returns>Whether the condition became true inside the budget.</returns>
    internal static async Task<bool> WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(10).ConfigureAwait(false);
        }

        return condition();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _applicationStopping.Dispose();
        try
        {
            Directory.Delete(_repoRoot, recursive: true);
        }
        catch (IOException)
        {
            // A best-effort cleanup of the throwaway working tree; a locked file on a
            // loaded agent must not fail the test that already made its assertion.
        }
    }

    private RepoContextBootstrapService CreateBootstrapService()
    {
        var symbolExtractor = Substitute.For<ISymbolExtractor>();
        var tokenCounter = Substitute.For<IRepoContextTokenCounter>();

        return new RepoContextBootstrapService(
            GrainFactory,
            SerializerServices.GetRequiredService<Serializer<FileNode>>(),
            SerializerServices.GetRequiredService<Serializer<RepoNode>>(),
            Substitute.For<IRepoContextVectorIngestor>(),
            new RepoContextSymbolReconciler(
                GrainFactory,
                SerializerServices.GetRequiredService<Serializer<SymbolRecord>>(),
                SerializerServices.GetRequiredService<Serializer<CrossReferenceNode>>(),
                symbolExtractor,
                NullLogger<RepoContextSymbolReconciler>.Instance),
            new RepoContextContentReconciler(
                GrainFactory,
                SerializerServices.GetRequiredService<Serializer<ContentRecord>>(),
                tokenCounter,
                NullLogger<RepoContextContentReconciler>.Instance),
            symbolExtractor,
            // No allowed roots, so the guard is inert and the throwaway temp tree
            // resolves unchanged; the guard's own enforcement has its own tests.
            new RepoContextWorkspaceGuard([]),
            TimeProvider.System,
            new RepoContextIndexingOptions(),
            NullLogger<RepoContextBootstrapService>.Instance);
    }

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> GatedEntries(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Sampled inside the pass, so a test can assert the run's ambient credential
        // is the authority's rather than the enqueuing caller's.
        ObservedCredential = LatticeCredentialContext.Current;

        await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        yield break;
    }
}
