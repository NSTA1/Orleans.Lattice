using Orleans.Lattice.Api.Mcp.RepoContext.Host;

// The RepoContext MCP container host entry point: "codebase memory in a box".
// All wiring lives in RepoContextHostBuilder so it is unit-testable; this file is
// the thin process shell that builds and runs the host.

// The container's Docker healthcheck re-invokes THIS binary with --healthcheck
// rather than a shell tool, because the runtime image is chiseled and shell-less
// (no curl/wget/nc). Handle it before building the host: it is a short-lived HTTP
// probe of the host's own /health/silo endpoint that must not spin up a silo.
if (args.Contains("--healthcheck", StringComparer.Ordinal))
{
    // Assign Environment.ExitCode and return void rather than returning a code:
    // a top-level program that returns int compiles to an int-returning entry
    // point, which silently overrides Environment.ExitCode and would reintroduce
    // the abandoned-drain defect of issue #2401 (pinned by RepoContextExitCodeTests).
    Environment.ExitCode = await RepoContextHealthProbe.RunAsync(
        RepoContextHealthProbe.ResolvePort(
            Environment.GetEnvironmentVariable(RepoContextHostConfiguration.McpPortKey)))
        .ConfigureAwait(false);
    return;
}

var app = RepoContextHostBuilder.Build(args);
await app.RunAsync().ConfigureAwait(false);
