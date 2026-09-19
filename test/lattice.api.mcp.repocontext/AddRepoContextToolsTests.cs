using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeMcpRepoContextServiceCollectionExtensions.AddRepoContextTools"/>.
/// Proves the opt-in registers exactly one repository-context tool group serving
/// the repository-context group, is idempotent, and validates its arguments.
/// </summary>
[TestFixture]
public sealed class AddRepoContextToolsTests
{
    // The tool-group service interface is internal to the MCP package; obtain its
    // Type via the accessible RepoContextToolGroup rather than naming it.
    private static readonly Type ToolGroupInterface = typeof(RepoContextToolGroup)
        .GetInterfaces()
        .Single(i => i.Name == "ILatticeApiMcpToolGroup");

    [Test]
    public void AddRepoContextTools_registers_a_single_repo_context_tool_group()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();
        var groups = provider.GetServices(ToolGroupInterface).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(groups, Has.Exactly(1).InstanceOf<RepoContextToolGroup>());
            Assert.That(((RepoContextToolGroup)groups.Single()!).Group,
                Is.EqualTo(LatticeApiMcpGroup.RepoContext));
        });
    }

    [Test]
    public void AddRepoContextTools_is_idempotent_for_the_tool_group()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();
        services.AddRepoContextTools();

        var registrations = services.Count(d => d.ServiceType == ToolGroupInterface);
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddRepoContextTools_returns_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();
        Assert.That(services.AddRepoContextTools(), Is.SameAs(services));
    }

    [Test]
    public void AddRepoContextTools_rejects_a_null_service_collection()
        => Assert.Throws<ArgumentNullException>(
            () => LatticeMcpRepoContextServiceCollectionExtensions.AddRepoContextTools(null!));

    [Test]
    public void AddRepoContextTools_registers_the_bootstrap_coordinator_and_real_vector_ingestor()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        var ingestor = services.SingleOrDefault(d => d.ServiceType == typeof(IRepoContextVectorIngestor));

        Assert.Multiple(() =>
        {
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextBootstrapService)), Is.True);
            Assert.That(ingestor, Is.Not.Null);
            // The seam is wired to the embed-and-store ingestor via a factory, so
            // the deferred no-op is no longer the registered implementation.
            Assert.That(ingestor!.ImplementationType, Is.Not.EqualTo(typeof(NoOpRepoContextVectorIngestor)));
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextVectorWriter)), Is.True);
            // The always-on embedding gap scanner (consumed by the self-index grain)
            // is wired alongside the writer it reads membership through.
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextEmbeddingGapScanner)), Is.True);
            Assert.That(
                services.Any(d => d.ServiceType == typeof(IRepoContextSemanticIndex)), Is.True);
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextSearchService)), Is.True);
        });
    }

    [Test]
    public void AddRepoContextTools_registers_the_marker_scan_reporter_the_writer_takes_optionally()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            // The descriptor must be present, because the writer takes the reporter
            // as an OPTIONAL constructor parameter: the container supplies the
            // declared default for a parameter it cannot resolve rather than
            // failing, so dropping this registration is not a startup error. It is a
            // null reporter, an instrument that exists on no host at all, and a
            // suite that stays green because every fixture passes one explicitly.
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextMemoryMarkerScanReporter)),
                Is.True,
                "the marker-scan reporter must be registered, or the writer's optional "
                + "parameter silently defaults to null and the instrument never exists");

            // Resolvable, not merely described - a descriptor whose implementation
            // cannot be constructed would satisfy the check above and still yield
            // nothing at runtime.
            Assert.That(provider.GetService<RepoContextMemoryMarkerScanReporter>(), Is.Not.Null);
        });
    }

    [Test]
    public void AddRepoContextTools_registers_the_symbol_walk_reporter_the_ingestor_takes_optionally()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            // Same silent-default hazard as the two reporters above. It is worth
            // asserting separately here because this instrument's whole job is to
            // distinguish a symbol walk that resumed banked progress from one that
            // silently restarted at the head of the range - at the tree the two
            // passes are byte-identical, so an absent series does not read as a
            // missing instrument, it reads as "the resumable cursor is not live",
            // which is exactly the wrong answer about the fix in issue #2953.
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextSymbolWalkReporter)),
                Is.True,
                "the symbol-walk reporter must be registered, or the ingestor's optional "
                + "parameter silently defaults to null and every arm of the instrument is absent");

            Assert.That(provider.GetService<RepoContextSymbolWalkReporter>(), Is.Not.Null);

            // One instance, because the three arms are a single tally over one
            // series; a transient would give each resolution its own meter and its
            // own priming.
            Assert.That(
                provider.GetService<RepoContextSymbolWalkReporter>(),
                Is.SameAs(provider.GetService<RepoContextSymbolWalkReporter>()),
                "the reporter must be a singleton so every pass charges one instrument");
        });
    }

    [Test]
    public void AddRepoContextTools_registers_the_coverage_probe_reporter_both_consumers_take_optionally()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            // Same silent-default hazard as the marker-scan reporter above, and worse
            // here because TWO consumers take this one optionally - the vector
            // ingestor and the embedding gap scanner. Dropping the registration
            // costs no startup error and no red test: it costs every arm of
            // repocontext.bootstrap.coverage_probe, on every host, at once.
            //
            // That failure is uniquely bad for THIS instrument. An absent series
            // would be read as all-arms-zero, and all-arms-zero is a meaningful
            // diagnostic state for it - "no coverage resolution was ever reached" -
            // so an unregistered reporter does not read as a missing instrument. It
            // reads as a confident, wrong answer.
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextCoverageProbeReporter)),
                Is.True,
                "the coverage-probe reporter must be registered, or both consumers' optional "
                + "parameters silently default to null and every arm of the instrument is absent");

            Assert.That(provider.GetService<RepoContextCoverageProbeReporter>(), Is.Not.Null);

            // One instance, because the arms are a single cross-cutting tally: the
            // ingestor and the scanner charge different arms of the SAME series, and
            // a transient would give each consumer its own meter and its own priming.
            Assert.That(
                provider.GetService<RepoContextCoverageProbeReporter>(),
                Is.SameAs(provider.GetService<RepoContextCoverageProbeReporter>()),
                "the reporter must be a singleton so both consumers charge one instrument");
        });
    }

    [Test]
    public void AddRepoContextTools_does_not_offer_the_write_tools_by_default()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "repocontext_health", "repocontext_recall", "repocontext_scan", "repocontext_list_topics",
                "repocontext_search", "repocontext_index_status", "repocontext_neighbors",
                "repocontext_outline", "repocontext_related", "repocontext_context",
                "repocontext_stats", "repocontext_claim_status",
            }));
    }

    [Test]
    public void AddRepoContextTools_offers_the_write_tools_when_writes_are_enabled()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true, workspaceRoot: "/workspace");

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "repocontext_health", "repocontext_recall", "repocontext_scan", "repocontext_list_topics",
                "repocontext_search", "repocontext_index_status", "repocontext_neighbors",
                "repocontext_outline", "repocontext_changed", "repocontext_related", "repocontext_context",
                "repocontext_stats", "repocontext_claim_status",
                "repocontext_bootstrap", "repocontext_remember", "repocontext_update", "repocontext_forget",
                "repocontext_claim", "repocontext_renew_claim", "repocontext_release_claim",
            }));
    }

    /// <summary>
    /// The single-repository counterpart of the workspace-mode withholding below,
    /// and the regression for the arbitrary-local-read hole:
    /// <c>repocontext_bootstrap</c> takes its <c>repoRoot</c> from the wire exactly
    /// as <c>repocontext_add_repo</c> does, so with no workspace root the guard
    /// admits every absolute path on the host. The mutating repository-context
    /// tools need only a data-plane write grant - not an administrative one - so
    /// any caller who may write could have had the server index (and then make
    /// searchable) any directory it could read.
    /// <para>
    /// The read-only <c>repocontext_changed</c> is withheld on the same ground and
    /// was for a long time the exception that undid the rule. It takes an equally
    /// unbounded caller-supplied <c>path</c>, and it answers with the names of every
    /// file it walked, so leaving it advertised under an inert guard left a read
    /// primitive open beside two closed write ones - and it needs no write grant at
    /// all to reach.
    /// </para>
    /// <para>
    /// Every path-free tool must survive: withholding these fails the path-taking
    /// surface closed, it does not disable the server.
    /// </para>
    /// </summary>
    [Test]
    public void AddRepoContextTools_without_a_root_withholds_bootstrap_but_keeps_the_path_free_tools()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true);

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;
        var names = group.Tools.Select(t => t.ProtocolTool.Name).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<RepoContextWorkspaceGuard>().IsEnforcing, Is.False);
            Assert.That(names, Does.Not.Contain("repocontext_bootstrap"));
            Assert.That(names, Does.Not.Contain("repocontext_changed"));

            // The capture and claim tools key on a repository id, never a path.
            Assert.That(names, Does.Contain("repocontext_remember"));
            Assert.That(names, Does.Contain("repocontext_update"));
            Assert.That(names, Does.Contain("repocontext_forget"));
            Assert.That(names, Does.Contain("repocontext_claim"));
            Assert.That(names, Does.Contain("repocontext_search"));

            // The other two graph verbs project stored records for an indexed path
            // and never walk the filesystem, so they are unaffected.
            Assert.That(names, Does.Contain("repocontext_outline"));
            Assert.That(names, Does.Contain("repocontext_related"));
        });
    }

    [Test]
    public void AddRepoContextTools_workspace_mode_offers_the_dynamic_repo_tools()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true, workspaceMode: true, workspaceRoot: "/workspace");

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "repocontext_health", "repocontext_recall", "repocontext_scan", "repocontext_list_topics",
                "repocontext_search", "repocontext_index_status", "repocontext_neighbors", "repocontext_list_repos",
                "repocontext_outline", "repocontext_changed", "repocontext_related", "repocontext_context",
                "repocontext_stats", "repocontext_claim_status",
                "repocontext_add_repo", "repocontext_remove_repo", "repocontext_reset_index",
                "repocontext_remember", "repocontext_update", "repocontext_forget",
                "repocontext_claim", "repocontext_renew_claim", "repocontext_release_claim",
            }));
    }

    [Test]
    public void AddRepoContextTools_registers_an_enforcing_guard_when_a_workspace_root_is_supplied()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true, workspaceMode: true, workspaceRoot: "/workspace");

        using var provider = services.BuildServiceProvider();
        var guard = provider.GetRequiredService<RepoContextWorkspaceGuard>();

        Assert.That(guard.IsEnforcing, Is.True);
    }

    /// <summary>
    /// Workspace mode without a workspace root leaves the guard unable to enforce
    /// any boundary, so <c>repocontext_add_repo</c> - whose contract promises the
    /// path is "resolved against the workspace boundary" - must not be advertised
    /// at all. Critically the group must not substitute
    /// <c>repocontext_bootstrap</c> either: bootstrap accepts an equally unbounded
    /// caller-supplied path, so falling back to it would reopen the same
    /// arbitrary-filesystem-read hole under a different name.
    /// </summary>
    [Test]
    public void AddRepoContextTools_workspace_mode_without_a_root_withholds_add_repo_and_bootstrap()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true, workspaceMode: true);

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;
        var names = group.Tools.Select(t => t.ProtocolTool.Name).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<RepoContextWorkspaceGuard>().IsEnforcing, Is.False);
            Assert.That(names, Does.Not.Contain("repocontext_add_repo"));
            Assert.That(names, Does.Not.Contain("repocontext_bootstrap"));

            // The path-free workspace tools stay: remove_repo takes a repository
            // id and never touches the working tree, and list_repos is read-only.
            Assert.That(names, Does.Contain("repocontext_remove_repo"));
            Assert.That(names, Does.Contain("repocontext_list_repos"));
        });
    }

    /// <summary>
    /// The withholding is scoped to the unguarded case: supplying a workspace root
    /// restores <c>repocontext_add_repo</c>, so the fix is a fail-closed refinement
    /// rather than a blanket removal of the workspace onboarding tool.
    /// </summary>
    [Test]
    public void AddRepoContextTools_workspace_mode_with_a_root_still_offers_add_repo()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true, workspaceMode: true, workspaceRoot: "/workspace");

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;

        Assert.That(group.Tools.Select(t => t.ProtocolTool.Name), Does.Contain("repocontext_add_repo"));
    }

    [Test]
    public void AddRepoContextTools_registers_a_disabled_guard_by_default()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();
        var guard = provider.GetRequiredService<RepoContextWorkspaceGuard>();

        Assert.That(guard.IsEnforcing, Is.False);
    }

    [Test]
    public void AddRepoContextTools_registers_the_default_tiktoken_token_counter_as_a_singleton()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        var descriptor = services.SingleOrDefault(d => d.ServiceType == typeof(IRepoContextTokenCounter));

        Assert.Multiple(() =>
        {
            Assert.That(descriptor, Is.Not.Null);
            Assert.That(descriptor!.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
        });

        using var provider = services.BuildServiceProvider();
        var counter = provider.GetRequiredService<IRepoContextTokenCounter>();

        Assert.Multiple(() =>
        {
            Assert.That(counter, Is.InstanceOf<TiktokenRepoContextTokenCounter>());
            // Singleton: the same instance is resolved every time.
            Assert.That(provider.GetRequiredService<IRepoContextTokenCounter>(), Is.SameAs(counter));
        });
    }

    [Test]
    public void AddRepoContextTools_lets_a_host_supplied_token_counter_win()
    {
        var host = new FixedTokenCounter();
        var services = new ServiceCollection();
        services.AddSingleton<IRepoContextTokenCounter>(host);
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IRepoContextTokenCounter>(), Is.SameAs(host));
    }

    private sealed class FixedTokenCounter : IRepoContextTokenCounter
    {
        public int CountTokens(string text) => 0;

        public int CountTokens(ReadOnlySpan<char> text) => 0;
    }
}
