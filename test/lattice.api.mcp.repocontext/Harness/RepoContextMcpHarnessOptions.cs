using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Options that shape a <see cref="RepoContextMcpHarness"/> instance: the auth
/// posture the served session runs under, and optional hooks a consuming test
/// fixture uses to register the extra facades, tool modules, or grain services a
/// specific repository-context tool sub-issue needs on top of the baseline
/// (an in-memory Lattice cluster plus the repository-context MCP surface).
/// </summary>
public sealed class RepoContextMcpHarnessOptions
{
    /// <summary>
    /// The authorization posture the served MCP session runs under. Defaults to
    /// <see cref="RepoContextMcpAuthPosture.Writer"/> so a fixture that just
    /// wants the full tool surface reachable does not have to opt in; assert the
    /// fail-closed seam by switching to
    /// <see cref="RepoContextMcpAuthPosture.Unauthenticated"/> or
    /// <see cref="RepoContextMcpAuthPosture.Reader"/>.
    /// </summary>
    public RepoContextMcpAuthPosture Posture { get; set; } = RepoContextMcpAuthPosture.Writer;

    /// <summary>
    /// An optional hook to configure the co-hosted Orleans silo beyond the
    /// baseline (in-memory grain storage / reminders and the core Lattice tree),
    /// for example to register a transport-agnostic facade such as
    /// <c>AddLatticeDataApi</c> that a repository-context tool adapts. Runs after
    /// the baseline silo wiring, so it can add to or replace it.
    /// </summary>
    public Action<ISiloBuilder>? ConfigureSilo { get; set; }

    /// <summary>
    /// An optional hook to configure the web host's service collection beyond the
    /// baseline (<c>AddLatticeMcp</c> + <c>AddRepoContextTools</c> and the
    /// posture stub collaborators), for example to register an additional tool
    /// module or override a discovery collaborator. Runs before
    /// <c>AddLatticeMcp</c> so a <c>TryAdd</c>-based override the fixture
    /// registers wins over the package default.
    /// </summary>
    public Action<IServiceCollection>? ConfigureServices { get; set; }
}
