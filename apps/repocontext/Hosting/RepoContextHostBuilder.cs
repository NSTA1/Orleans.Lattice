using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Scaling;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Assembles the RepoContext MCP container host: a single ASP.NET Core web
/// application whose <b>only application listener is the MCP endpoint</b> (plus
/// HTTP health probes, the Prometheus scrape endpoint, and in the azure profile
/// the scaling scrape). It composes
/// the already-shipped seams - the core silo, the file/Azure WAL, the MCP binding
/// with the repository-context tool module, the Onyx embedding provider, and the
/// membership/auth stack - behind an environment-selected durability profile with
/// on-host-mount persistence, per-tree compaction on the churn trees, distinct
/// liveness/readiness probes, and graceful shutdown that drains the WAL.
/// </summary>
/// <remarks>
/// No gRPC facade and no Explorer UI are referenced or mapped: the container
/// deliberately exposes MCP alone. The scaling signal and its scrape endpoint are
/// wired only in the azure profile.
/// </remarks>
public static class RepoContextHostBuilder
{
    /// <summary>The liveness probe path (process + silo alive).</summary>
    public const string LivenessPath = "/health/live";

    /// <summary>The readiness probe path (silo joined, replay done, stores reachable, MCP serving).</summary>
    public const string ReadinessPath = "/health/ready";

    /// <summary>
    /// The Prometheus scrape path. Serves every instrument published on a
    /// Lattice-owned meter in the process, in the standard text exposition format.
    /// </summary>
    /// <remarks>
    /// The image is distroless, so there is no shell to read a counter from inside
    /// the container; without this endpoint every instrument the host publishes is
    /// unreadable in the one deployment that needs it, which is what issue #2363
    /// records. The path is unauthenticated for the same reason the health probes
    /// are: it is reachable only on the container's own listener, and it carries
    /// aggregate counters rather than repository content.
    /// </remarks>
    public const string MetricsPath = "/metrics";

    /// <summary>The health-check tag identifying the liveness probe.</summary>
    public const string LivenessTag = "live";

    /// <summary>The health-check tag identifying the readiness probe.</summary>
    public const string ReadinessTag = "ready";

    /// <summary>
    /// The host's own shutdown budget: how long the generic host will wait for
    /// every hosted service - the silo, and with it the WAL commit-log drainer - to
    /// stop before it abandons the drain and exits.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This value is <b>only reachable if the container grants at least this long
    /// between SIGTERM and SIGKILL</b>. Docker's default <c>stop_grace_period</c> is
    /// 10 seconds, which is a ninth of it, so a deployment that leaves the default
    /// in place can never exercise this budget: every teardown is a crash teardown,
    /// the graceful deactivation path never completes, and the documented behaviour
    /// silently does not hold. That is the defect recorded as issue #2389, and it is
    /// why this budget is a named constant rather than a literal buried in a lambda
    /// - the sample compose file's <c>stop_grace_period</c> is asserted against it,
    /// so the two numbers cannot drift apart unnoticed.
    /// </para>
    /// </remarks>
    public static readonly TimeSpan ShutdownBudget = TimeSpan.FromSeconds(90);

    /// <summary>
    /// Builds the fully-wired <see cref="WebApplication"/> from the ambient
    /// configuration (environment variables). Resolves and validates the durability
    /// profile (failing fast on a missing credential), applies the local schema and
    /// proves the data paths are writable, then wires the silo, MCP surface, health
    /// probes, and graceful shutdown.
    /// </summary>
    /// <param name="args">The process command-line arguments.</param>
    /// <returns>The built, ready-to-run web application.</returns>
    public static WebApplication Build(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var builder = WebApplication.CreateBuilder(args);
        var config = RepoContextHostConfiguration.FromConfiguration(builder.Configuration);
        return Build(builder, config);
    }

    /// <summary>
    /// Wires an already-created <see cref="WebApplicationBuilder"/> for the supplied
    /// <paramref name="config"/>. Exposed so tests can drive the host with an
    /// explicit configuration and a test-server web host without mutating process
    /// environment variables.
    /// </summary>
    /// <param name="builder">The web application builder to wire.</param>
    /// <param name="config">The resolved, validated host configuration.</param>
    /// <returns>The built, ready-to-run web application.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="config"/> is null.</exception>
    public static WebApplication Build(WebApplicationBuilder builder, RepoContextHostConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(config);

        PrepareDataPaths(config);

        // The container's single application listener: the MCP port, bound on all
        // interfaces so it is reachable on the container network.
        builder.WebHost.UseUrls($"http://0.0.0.0:{config.McpPort}");

        // A generous shutdown budget so the silo's WAL commit-log drainer can flush
        // buffered records before the process exits on SIGTERM. It is only reachable
        // if the container's stop_grace_period exceeds it - see ShutdownBudget.
        builder.Services.Configure<HostOptions>(options =>
            options.ShutdownTimeout = ShutdownBudget);

        builder.Services.AddSingleton(config);
        builder.Services.AddSingleton<RepoContextReadinessState>();
        builder.Services.AddSingleton(sp => new RepoContextDrainSignal(
            sp.GetRequiredService<ILogger<RepoContextDrainSignal>>(),
            ShutdownBudget,
            // The production process-exit-code reporter, supplied explicitly because
            // the signal deliberately defaults to reporting nothing: this is the one
            // composition root where assigning the real Environment.ExitCode is
            // correct, and every test host that drives a deliberate overrun would be
            // poisoned by a default that did it everywhere (issue #2401).
            reportExitCode: RepoContextExitCode.SetProcessExitCode));

        // Constructed here rather than resolved lazily on the first scrape: the
        // listener starts accumulating from this point, so an instrument that
        // records during startup (index replay, the ANN sweep) is already counted
        // when the first scrape arrives. A lazily-created collector would silently
        // report those as zero, which reads as a measured negative rather than as
        // the absence of measurement it actually is.
        var metricsCollector = new RepoContextMetricsCollector();
        builder.Services.AddSingleton(metricsCollector);

        var isAzure = config.Profile == DurabilityProfile.Azure;

        builder.Host.UseOrleans(silo =>
        {
            silo.ConfigureDurability(config);

            // Opt the per-symbol structural tree in to self-describing schema-version
            // envelopes (Phase-1 stamping: one schema family, target version 1, no
            // upcasters yet). Registration is transparent to every other tree - the
            // envelope-stripping decoder passes an un-stamped value through verbatim
            // and the stamping interceptor is a no-op for an unversioned tree - so
            // only the symbol tree, opted in during warmup, ever carries an envelope.
            // The value it buys: a later change to the symbol record shape ships as a
            // new target version with an upcaster rather than a breaking reinterpret
            // of stored bytes. RepoContext reads and writes symbol values as opaque
            // whole values (client-side read-merge-write), so this is the simple
            // whole-value envelope case with no CRDT-delta upcasting involved.
            silo.AddLatticeSchemaVersioning();

            // Reap re-embed / prune tombstones on the churn trees in every profile.
            silo.ConfigureRepoContextCompaction();

            // Split the WAL materialiser pin state across buckets so an advancing
            // retention floor rewrites a fraction of the pin blob instead of all of
            // it. Global (not per-tree) by design - see RepoContextPinBucketing.
            silo.ConfigureRepoContextPinBucketing(builder.Configuration);

            // Let a constrained deployment pin the per-silo concurrent leaf WAL
            // replay ceiling instead of inheriting Environment.ProcessorCount,
            // which reports whatever DOTNET_PROCESSOR_COUNT says rather than the
            // container's CPU quota. Inert unless the variable is set - see
            // RepoContextReplayConcurrency.
            silo.ConfigureRepoContextReplayConcurrency(builder.Configuration);

            // Raise the named-lock lease ceiling above the library's five-minute
            // maximum. Backlog claims are held across a full build-and-test cycle,
            // which routinely exceeds it - see RepoContextClaimLeases.
            silo.ConfigureRepoContextClaimLeases(builder.Configuration);

            // Membership resolves the ambient credential into a subject; Auth
            // installs the default-deny gate with the bootstrap administrator that
            // seeds the local agent's grant.
            silo.AddLatticeMembership();
            silo.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(LocalTrustedAgent.BootstrapAdministrator);
            });
            silo.Services.AddSingleton<ILatticeCredentialAuthenticator, LocalTrustedAuthenticator>();
            silo.AddLatticeAuthApi();

            // Scaling signal is azure-only; never wired in the local topology.
            if (isAzure)
            {
                silo.AddLatticeScalingSignal();
            }
        });

        // Configure our log filters AFTER UseOrleans, deliberately. Filter rules
        // are evaluated by longest matching category prefix and, among equals, the
        // LAST rule registered wins - so a rule Orleans registers for one of its
        // own categories during UseOrleans silently overrides an identical rule we
        // added earlier. That is not hypothetical: with this call sited before
        // UseOrleans the Orleans.Runtime.CallbackData filter took effect while the
        // Orleans.Messaging one did not, which is a confusing half-working state to
        // debug. Registering last makes our intent win regardless of what the
        // framework configures for itself.
        ConfigureContainerLogging(builder.Logging);

        // The local-trusted credential bridge must be registered BEFORE AddLatticeMcp
        // so its TryAdd-registered HttpContext bridge is skipped and ours wins.
        builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge, LocalTrustedCredentialBridge>();

        // The box is a single trusted local agent, so writes are enabled. Run the
        // streamable-HTTP transport STATELESS: this host exposes a fixed
        // repocontext_* tool set to one local agent (RequireAuthorization is off and
        // an AllowAll authorizer is wired below), so it needs none of the
        // permission-scoped per-session tool collections that stateful mode exists to
        // serve. Stateless makes every request self-contained, so restarting or
        // recreating the container no longer expires the client's in-memory session
        // and 404s every subsequent tool call until the client reconnects.
        builder.Services.AddLatticeMcp(options =>
        {
            options.RequireAuthorization = false;
            options.Stateless = true;
        });

        // Opt in past the default-deny coarse MCP gate. The container stamps every
        // request as the trusted local agent through LocalTrustedCredentialBridge,
        // and the per-tree access gate plus the seeded local-agent grant remain the
        // real, fail-closed enforcement seam - so the coarse transport gate adds no
        // value here and would otherwise hide the entire repocontext_* surface.
        builder.Services.AddSingleton<ILatticeApiMcpAuthorizer, AllowAllMcpAuthorizer>();

        // The background indexing runner must write as the trusted local agent for
        // BOTH a request-initiated pass and a reminder-driven resume. Register this
        // authority BEFORE AddRepoContextTools so its TryAdd-registered null default
        // is skipped and the runner stamps the local-agent credential on every run;
        // otherwise a resume after restart would write as anonymous and the
        // default-deny access gate would deny its structural writes.
        builder.Services.AddSingleton<IRepoIndexRunAuthority, LocalTrustedRunAuthority>();

        // Enforce the workspace boundary: repositories added at runtime through
        // repocontext_add_repo must resolve under the mounted read-only workspace
        // root. Passing the root turns on workspace mode, which swaps the
        // single-repo bootstrap tool for the dynamic add_repo/list_repos/remove_repo
        // surface and installs the fail-closed path guard.
        builder.Services.AddRepoContextTools(
            enableWrites: true,
            workspaceMode: true,
            workspaceRoot: config.WorkspaceRoot);

        // The default embedding provider points at the separate Onyx companion
        // container, preserving the MCP-only single-listener surface.
        builder.Services.AddOnyxEmbeddingProvider(options =>
        {
            options.BaseAddress = config.EmbeddingEndpoint;
            options.ModelName = config.EmbeddingModel;
            options.Dimension = config.EmbeddingDimension;
        });

        // Warmup + graceful-drain coordinator (flips readiness).
        builder.Services.AddHostedService<RepoContextStartupService>();

        // State the configuration this process actually resolved, once, at startup. The
        // deployment's real settings arrive from an untracked compose override, so reading
        // the repository does not tell you what the container runs - and the only reason
        // that divergence was ever noticed is that one subsystem already reports its own
        // resolved cadence. This generalises that to every supplied variable plus the
        // runtime facts (ProcessorCount) that are not variables at all. Observability
        // only; it validates nothing. See issue #2294, and #2075 for the precedent.
        builder.Services.AddHostedService<RepoContextEffectiveConfigurationReporter>();

        // Vector-plane warmup driver: issues the first semantic query itself so the
        // retrieval readiness component reports demonstrated capability instead of
        // waiting for traffic an orchestrator will not route to a not-ready box.
        builder.Services.AddHostedService<RepoContextRetrievalWarmupService>();

        var healthChecks = builder.Services.AddHealthChecks();
        healthChecks.AddCheck<RepoContextLivenessHealthCheck>(
            RepoContextLivenessHealthCheck.Name,
            tags: new[] { LivenessTag });
        healthChecks.AddCheck<RepoContextReadinessHealthCheck>(
            RepoContextReadinessHealthCheck.Name,
            tags: new[] { ReadinessTag });

        // The vector-plane component of readiness. Tagged 'ready' so /health/ready is
        // the conjunction of lifecycle phase and retrieval capability: a box that
        // cannot serve a semantic query does not report ready. Liveness is untagged by
        // it on purpose - a replaying box is alive and must not be restarted.
        healthChecks.AddCheck<RepoContextRetrievalReadinessHealthCheck>(
            RepoContextRetrievalReadinessHealthCheck.Name,
            tags: new[] { ReadinessTag });
        if (isAzure)
        {
            healthChecks.AddLatticeScalingHealthCheck(tags: new[] { ReadinessTag });
        }

        var app = builder.Build();

        app.MapLatticeMcp();
        app.MapHealthChecks(LivenessPath, new HealthCheckOptions
        {
            Predicate = registration => registration.Tags.Contains(LivenessTag),
        });
        app.MapHealthChecks(ReadinessPath, new HealthCheckOptions
        {
            Predicate = registration => registration.Tags.Contains(ReadinessTag),
        });

        app.MapGet(MetricsPath, (RepoContextMetricsCollector collector) =>
            Results.Text(collector.Render(), RepoContextPrometheusExposition.ContentType));

        // Dispose is idempotent, so registering it here is safe whether or not the
        // service provider also disposes the instance it did not create.
        app.Lifetime.ApplicationStopped.Register(metricsCollector.Dispose);

        // The observable drain-complete signal. Registered here rather than as a
        // hosted service so its ApplicationStopped callback is not itself one of the
        // services being waited on: it must be able to report the duration of a stop
        // sequence it is not part of.
        app.Services
            .GetRequiredService<RepoContextDrainSignal>()
            .Bind(app.Lifetime);

        if (isAzure)
        {
            app.MapLatticeScalingSignal();
        }

        return app;
    }

    /// <summary>
    /// Quietens the framework and runtime log categories that would otherwise
    /// dominate the container's log stream, so the operator sees signal (indexing
    /// lifecycle, warnings, and errors) rather than a line per HTTP request, per
    /// outbound embedder call, or per slow grain turn. Our own
    /// <c>Orleans.Lattice.*</c> categories keep their default Information level, so
    /// the walk, plan, and vectorisation progress lines are untouched.
    /// </summary>
    /// <param name="logging">The host's logging builder.</param>
    private static void ConfigureContainerLogging(ILoggingBuilder logging)
    {
        // Timestamp every console line (UTC, millisecond precision) so each tool
        // call and each indexing-activity line carries a wall-clock reference in
        // `docker logs`. The Simple console formatter is registered through
        // TryAddEnumerable, so this configures the console provider the host
        // already adds rather than attaching a second, duplicate sink.
        logging.AddSimpleConsole(options =>
        {
            options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
            options.UseUtcTimestamp = true;
        });

        // Orleans warns when a grain turn runs longer than its turn-length
        // threshold. Under CPU-bound embedding load the WAL commit turns routinely
        // cross one second; that is expected back-pressure here, not a fault, so it
        // is pure noise. Keep genuine scheduler errors.
        logging.AddFilter("Orleans.Runtime.Scheduler", LogLevel.Error);

        // ASP.NET Core logs "Request starting/finished" and "Executing endpoint"
        // for every call, including the frequent Docker health probes. Keep
        // warnings and above.
        logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

        // The health-check middleware logs each probe evaluation; keep warnings+.
        logging.AddFilter("Microsoft.Extensions.Diagnostics.HealthChecks", LogLevel.Warning);

        // The MCP server logs a pair of lines per tool call, which the repeated
        // index-status poll turns into a wall of noise. Our handlers still log the
        // meaningful lifecycle lines under their own categories.
        logging.AddFilter("ModelContextProtocol", LogLevel.Warning);

        // IHttpClientFactory's default logging emits ~4 Information lines
        // ("Start/End processing HTTP request", "Sending HTTP request",
        // "Received HTTP response") for every outbound call. The only outbound
        // client here is the Onyx embedder client, which the near-continuous
        // reconcile drives on almost every tick, so this dominates the log stream
        // at the source (over and above the size cap). Keep warnings and above; a
        // failed embed still surfaces through our own provider category.
        logging.AddFilter("System.Net.Http.HttpClient", LogLevel.Warning);

        // Orleans warns per dropped message and per late callback. During the
        // cold-start replay window the vector trees are legitimately slower than
        // the 30s response deadline, so every probe against them expires and each
        // expiry logs twice - once as "Response did not arrive on time"
        // (CallbackData) and once as "Dropping expired message" (Messaging). On a
        // real volume that was 1,216 lines in fifteen minutes, 776 of them merely
        // dropped STATUS RESPONSES (diagnostic probes about an already-late call,
        // not lost work), which is enough to bury the handful of genuine faults
        // mixed in among them.
        //
        // This is self-clearing back-pressure, not a fault: the replay converges
        // and the callers retry. The condition stays observable through the
        // over-budget replay counter, through our own RepoIndexRunner category,
        // and through Error-level Orleans messaging problems, which these filters
        // keep. Raise them back to Warning when diagnosing a hang that does NOT
        // clear on its own.
        logging.AddFilter("Orleans.Runtime.CallbackData", LogLevel.Error);
        logging.AddFilter("Orleans.Messaging", LogLevel.Error);

        // The runtime's companion to the above: while a request is outstanding past
        // its deadline, Orleans polls the target activation and logs the reply at
        // INFORMATION as "Received status update for pending request", one line per
        // poll carrying a full activation and task-scheduler dump. It is a
        // diagnostic about an already-reported slow call, so it adds no fact the
        // callback and messaging lines did not, and it ran to 502 lines in ten
        // minutes on a real cold start. Keep warnings and above.
        logging.AddFilter("Orleans.Runtime.InsideRuntimeClient", LogLevel.Warning);
    }

    /// <summary>
    /// Proves the durable data paths are on a writable host mount before the silo
    /// starts, and applies the SQLite schema when a SQLite-backed store is selected.
    /// Fails fast (throws) on a missing or unwritable data path.
    /// </summary>
    /// <param name="config">The resolved host configuration.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="InvalidOperationException">A required data path is missing or unwritable.</exception>
    public static void PrepareDataPaths(RepoContextHostConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (config.UsesFileWal)
        {
            DataPathGuard.EnsureDirectoryWritable(config.WalDirectory, "WAL");
        }

        if (config.UsesSqlite)
        {
            new SqliteSchemaInitializer(config.SqlitePath).Initialize();
        }
    }
}
