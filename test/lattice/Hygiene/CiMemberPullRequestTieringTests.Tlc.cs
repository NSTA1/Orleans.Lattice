using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Tests.Formal;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Exhaustive TLC-input scoping: routine main and member pull requests retain
/// the smoke shard and skip exhaustive shards only when no TLC input changed;
/// release-line and other fully gated runs keep exhaustive TLC.
/// </summary>
public sealed partial class CiMemberPullRequestTieringTests
{
    private const string MemberBase = "feat/epic/formal-coverage";

    [TestCase(null)]
    [TestCase(MemberSkippedTiers)]
    public void Scheduling_shards_price_all_certified_modules_in_full_and_member_plans(string? skippedTiers)
    {
        // Mixed-scope conservative certificates and retained Shipping allocation, not new group clocks.
        const double generalSeconds = 144.684 + 177 + 729.46094 + 62.8925529 + 54.083547
            + 6562.8434051 + 1413.7458152 + 125.8571349;
        const double shippingSeconds = 2745.4195361;
        const double variantSeconds = 1901.2196480;
        const double itemOverheadMinutes = 0.04;
        var expected = new Dictionary<string, (double Minutes, double Seconds)>(StringComparer.Ordinal)
        {
            ["formal-tlc-scheduling"] = (154.51, generalSeconds),
            ["formal-tlc-scheduling-shipping"] = (14.07, shippingSeconds - variantSeconds),
            ["formal-tlc-scheduling-shipping-variants"] = (31.69, variantSeconds),
        };
        var scheduling = Plan(["lattice"], skippedTiers)
            .Where(item => item.Shard.StartsWith("formal-tlc-scheduling", StringComparison.Ordinal)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(scheduling.Select(item => item.Shard), Is.EquivalentTo(expected.Keys));
            foreach (var item in scheduling)
            {
                Assert.That(item.Skip, Is.Null, item.Shard);
                Assert.That(item.Tier, Is.EqualTo("deterministic"), item.Shard);
                var price = expected[item.Shard];
                Assert.That(item.EstimateMinutes, Is.EqualTo(price.Minutes + itemOverheadMinutes).Within(0.000001));
                Assert.That(price.Minutes * 60, Is.GreaterThanOrEqualTo(price.Seconds), item.Shard);
                Assert.That(price.Minutes * 60 - price.Seconds, Is.LessThan(0.6), "round once to hundredths of a minute");
            }
            Assert.That(scheduling.Sum(item => item.EstimateMinutes) - 3 * itemOverheadMinutes,
                Is.EqualTo(200.27).Within(0.000001), "do not add the earlier standalone Shipping diagnostics again");
        });
    }

    [TestCase(null)]
    [TestCase(MemberSkippedTiers)]
    public void Scheduling_partition_reaches_first_match_filters_and_independently_packed_items(string? skippedTiers)
    {
        var (exitCode, _, stderr) = RunPlanner(["lattice"], skippedTiers, out var matrix);
        Assert.That(exitCode, Is.Zero, stderr);
        using var document = JsonDocument.Parse(matrix);
        var legs = document.RootElement.EnumerateArray().ToArray();
        var scheduling = legs.SelectMany(leg => leg.GetProperty("items").EnumerateArray()
                .Where(item => item.GetProperty("shard").GetString()!.StartsWith("formal-tlc-scheduling", StringComparison.Ordinal))
                .Select(item => (Leg: leg.GetProperty("id").GetString(), Item: item)))
            .ToArray();
        string[] categories = ["TlcShardScheduling", "TlcShardSchedulingShipping", "TlcShardSchedulingShippingVariants"];
        string[] shards = ["formal-tlc-scheduling", "formal-tlc-scheduling-shipping", "formal-tlc-scheduling-shipping-variants"];

        Assert.That(scheduling, Has.Length.EqualTo(3), "three independently runnable work items, not one opaque 200.27-minute item");
        Assert.Multiple(() =>
        {
            Assert.That(scheduling.Select(entry => entry.Leg), Is.Unique, "the retained lattice plan can pack these independently");
            for (var index = 0; index < scheduling.Length; index++)
            {
                var filter = scheduling.Single(entry => entry.Item.GetProperty("shard").GetString() == shards[index])
                    .Item.GetProperty("filter").GetString()!;
                var terms = filter.Split('&').Select(term => term.Trim('(', ')')).ToArray();
                Assert.That(terms, Does.Contain("TestCategory=" + categories[index]));
                foreach (var earlier in categories.Take(index))
                    Assert.That(terms, Does.Contain("TestCategory!=" + earlier));
            }
            var catchAll = legs.SelectMany(leg => leg.GetProperty("items").EnumerateArray())
                .Single(item => item.GetProperty("shard").GetString() == "formal-tlc"
                    && item.GetProperty("tier").GetString() == "deterministic");
            foreach (var category in categories)
                Assert.That(catchAll.GetProperty("filter").GetString(), Does.Contain("TestCategory!=" + category));
            foreach (var leg in legs)
                Assert.That(leg.GetProperty("estimate").GetDouble(),
                    Is.EqualTo(leg.GetProperty("items").EnumerateArray().Sum(item => item.GetProperty("estimate").GetDouble()))
                        .Within(0.001), "packing must retain the sum, including each new invocation's overhead");
        });
    }

    [TestCase("src/lattice/BPlusTree/Grains/LatticeGrain.cs")]
    [TestCase("spec/wal/Refinement.md")]
    [TestCase("spec/README.md")]
    [TestCase("docs/lattice/api.md")]
    [TestCase("test/lattice/BPlusTree/LatticeGrainTests.cs")]
    public void A_member_pull_request_that_touches_no_TLC_input_skips_the_TLC_shards(string changed)
    {
        var decision = DecideTierScope("pull_request", MemberBase, ["src/lattice.replication/X.cs", changed]);

        Assert.That(
            decision["skip_tlc_reason"],
            Does.Contain(MemberBase),
            "a member pull request whose diff no TLC verdict depends on should skip the TLC shards. "
                + Describe(changed, decision));
    }

    [Test]
    public void A_main_pull_request_that_touches_no_TLC_input_keeps_the_smoke_shard_and_skips_only_exhaustive_TLC()
    {
        var decision = DecideTierScope("pull_request", "main", ["src/lattice/BPlusTree/Grains/LatticeGrain.cs"]);
        var run = Plan(["lattice"], skipTiers: null);
        var scoped = Plan(["lattice"], skipTiers: null, skipTlcReason: decision["skip_tlc_reason"]);

        Assert.Multiple(() =>
        {
            Assert.That(decision["skip_tlc_reason"], Does.Contain("smoke shard still runs"));
            Assert.That(
                scoped.Single(item => item.Shard == "formal-tlc-smoke").Skip,
                Is.Null,
                "every routine pull request keeps its base-model smoke check");
            var smokeFilter = $"FullyQualifiedName~Orleans.Lattice.Tests.Formal.TlcModelCheckTests.{SpecModuleCases.SmokeTestName}";
            Assert.That(scoped.Single(item => item.Shard == "formal-tlc-smoke").Filter, Does.Contain(smokeFilter));
            Assert.That(
                scoped.Single(item => item.Shard == "formal-tlc" && item.Tier == "deterministic").Filter,
                Does.Contain($"FullyQualifiedName!~Orleans.Lattice.Tests.Formal.TlcModelCheckTests.{SpecModuleCases.SmokeTestName}"),
                "the exhaustive catch-all must not re-run the base-model smoke case");
            Assert.That(
                scoped.Where(item => item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal)
                    && item.Shard != "formal-tlc-smoke" && item.Skip is null),
                Is.Empty,
                "only the exhaustive TLC shards are skipped");
            Assert.That(
                scoped.Where(item => item.Skip == decision["skip_tlc_reason"]).Select(item => item.Shard).Distinct(),
                Is.EquivalentTo(run.Where(item => item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal)
                    && item.Shard != "formal-tlc-smoke").Select(item => item.Shard).Distinct()));
        });
    }

    [TestCase("spec/atomic-commit/AtomicCommit.tla")]
    [TestCase("test/lattice/Formal/TlcModelCheckTests.cs")]
    public void A_main_pull_request_that_touches_a_TLC_input_runs_exhaustive_TLC(string changed)
    {
        var decision = DecideTierScope("pull_request", "main", [changed]);

        Assert.That(decision["skip_tlc_reason"], Is.Empty, Describe(changed, decision));
    }

    [TestCase("spec/wal/WalMove.tla")]
    [TestCase("spec/wal/WalMove.cfg")]
    [TestCase("spec/wal/WalMove.manifest.json")]
    [TestCase("spec/wal/mutations/AnyMutation.mutation")]
    [TestCase("test/lattice/Formal/TlcModelCheckTests.cs")]
    [TestCase("test/lattice/Orleans.Lattice.Tests.csproj")]
    [TestCase(".github/workflows/ci.yml")]
    [TestCase(".github/workflows/test-shards.json")]
    [TestCase("Directory.Packages.props")]
    [TestCase("global.json")]
    public void A_member_pull_request_that_touches_a_TLC_input_runs_the_TLC_shards(string changed)
    {
        var decision = DecideTierScope("pull_request", MemberBase, ["src/lattice/X.cs", changed]);

        Assert.That(
            decision["skip_tlc_reason"],
            Is.Empty,
            "a change TLC reads must be model-checked on the pull request that wrote it. " + Describe(changed, decision));
    }

    [Test]
    public void A_member_pull_request_with_no_changed_file_list_runs_the_TLC_shards()
    {
        Assert.That(DecideTierScope("pull_request", MemberBase)["skip_tlc_reason"], Is.Empty, "no list means run more");
        Assert.That(DecideTierScope("pull_request", MemberBase, [])["skip_tlc_reason"], Is.Empty, "an empty list means run more");
    }

    [Test]
    public void A_main_pull_request_with_no_changed_file_list_runs_exhaustive_TLC()
    {
        Assert.That(DecideTierScope("pull_request", "main")["skip_tlc_reason"], Is.Empty);
        Assert.That(DecideTierScope("pull_request", "main", [])["skip_tlc_reason"], Is.Empty);
    }

    [TestCase("pull_request", "release/1.4")]
    [TestCase("push", "release/1.4")]
    [TestCase("push", MemberBase)]
    [TestCase("workflow_dispatch", "")]
    public void A_fully_gated_run_never_skips_the_TLC_shards(string eventName, string baseRef)
    {
        var decision = DecideTierScope(eventName, baseRef, ["src/lattice/X.cs"]);

        Assert.That(decision["skip_tlc_reason"], Is.Empty, Describe(eventName + " " + baseRef, decision));
    }

    [Test]
    public void The_planner_skips_exactly_the_tlc_shards_when_asked()
    {
        string[] packages = ["lattice"];

        var run = Plan(packages, MemberSkippedTiers);
        var scoped = Plan(packages, MemberSkippedTiers, skipTlcReason: "no TLC input (test)");

        var tlcShards = run
            .Where(item => item.Skip is null && item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal)
                && item.Shard != "formal-tlc-smoke")
            .Select(item => item.Shard)
            .Distinct()
            .ToList();

        Assert.That(tlcShards, Is.Not.Empty, "the plan carries no formal-tlc shard; this comparison would prove nothing");
        Assert.That(
            scoped.Where(item => item.Skip is null && item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal)),
            Is.Not.Empty,
            "a routine plan must retain its smoke shard");
        Assert.That(
            scoped.Where(item => item.Skip is null && item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal)
                && item.Shard != "formal-tlc-smoke"),
            Is.Empty,
            "a TLC-scoped plan must not run an exhaustive formal-tlc shard");
        Assert.That(
            scoped.Where(item => item.Skip == "no TLC input (test)").Select(item => item.Shard).Distinct(),
            Is.EquivalentTo(tlcShards),
            "every exhaustive formal-tlc shard must be recorded as skipped, with the stated reason, rather than dropped");
        Assert.That(
            scoped.Where(item => item.Skip is null).Select(item => item.Label),
            Is.EquivalentTo(run.Where(item => item.Skip is null
                && (!item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal)
                    || item.Shard == "formal-tlc-smoke")).Select(item => item.Label)),
            "skipping TLC must not change any other item");
    }

    [Test]
    public void The_workflow_feeds_the_TLC_scope_to_the_planner_and_the_verdict()
    {
        var ci = Read(CiWorkflow);

        Assert.That(
            ci,
            Does.Contain("--changed-files /tmp/changed-files.txt")
                .And.Contain("SKIP_TLC_REASON: ${{ steps.tierscope.outputs.skip_tlc_reason }}")
                .And.Contain("--skip-tlc-reason \"$SKIP_TLC_REASON\""),
            "the tier decision must see the diff, and the planner must take the TLC skip from it");

        var verdict = VerdictStep();
        Assert.That(verdict, Does.Contain("SKIP_TLC_REASON: ${{ needs.plan.outputs.skip_tlc_reason }}"));
        Assert.That(
            Regex.IsMatch(
                verdict,
                @"if \[ -n ""\$SKIP_TLC_REASON"" \] && \[ ""\$tlc_may_skip"" != ""true"" \]; then[\s\S]*?ok=false"),
            Is.True,
            "a TLC skip outside a member pull request must fail the verdict");
        Assert.That(
            verdict,
            Does.Contain("Exhaustive TLC shards NOT run").And.Contain("base-model smoke shard still runs"),
            "a TLC skip must be announced without implying that the smoke shard was skipped");

        var publish = Read(PublishWorkflow);
        Assert.That(publish, Does.Contain("--skip-tlc-reason").And.Contain("--skip-tlc-smoke-reason"),
            "Publish can skip the per-package TLC work only because it verifies successful CI for the exact tag SHA");
        Assert.That(publish, Does.Contain("verify-release-ci").And.Contain("TAG_SHA: ${{ github.sha }}"));
    }

    [Test]
    public void Publish_can_skip_both_TLC_shard_kinds_only_when_exact_SHA_CI_is_required()
    {
        var skipped = Plan(
            ["lattice"],
            skipTiers: null,
            skipTlcReason: "exact SHA verified by CI",
            skipTlcSmokeReason: "exact SHA verified by CI");

        Assert.That(
            skipped.Where(item => item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal)
                && item.Skip is not null).Select(item => item.Shard).Distinct(),
            Is.EquivalentTo(Plan(["lattice"], skipTiers: null)
                .Where(item => item.Shard.StartsWith("formal-tlc", StringComparison.Ordinal))
                .Select(item => item.Shard).Distinct()));
    }
}
