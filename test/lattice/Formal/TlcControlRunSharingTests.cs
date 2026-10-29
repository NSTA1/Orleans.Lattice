namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// When TLC clean-source controls are shared between pairing cases (#5152).
/// <para>
/// A control checks the unmutated base, so its input is decided by the module,
/// the target, the bounds and the TLC options, never by which mutation asked
/// for it. Sharing must follow exactly that: the same input shares one run, any
/// difference is a separate run, and a failing shared control fails every case
/// that shares it. These tests pin the cfg generation and the key
/// (<see cref="TlcControlRunKey"/>) that <see cref="TlcModelCheckTests"/> uses;
/// they need no JVM.
/// </para>
/// </summary>
[TestFixture]
public sealed class TlcControlRunSharingTests
{
    private const string SpecificationPath = "/spec/area/Sharing.tla";

    private const string BaseSpecification = "---- MODULE Sharing ----\nA == 1\nB == 2\n====\n";

    private const string BaseConfig = "SPECIFICATION Spec\nCONSTANTS\n    N = 3\nINVARIANTS\n    TypeOK\n    Safe\n";

    private static readonly IReadOnlyDictionary<string, string> Siblings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["Sharing.tla"] = BaseSpecification,
        ["Helper.tla"] = "---- MODULE Helper ----\nH == 0\n====\n",
    };

    private static SpecMutation Mutation(
        string name,
        string target = "Safe",
        SpecPropertyClass propertyClass = SpecPropertyClass.Invariant,
        string find = "A == 1",
        string replace = "A == 9",
        bool deadlockOff = false,
        IReadOnlyList<CfgAssignment>? bounds = null) =>
        new()
        {
            Name = name,
            Module = name,
            Target = target,
            PropertyClass = propertyClass,
            Summary = name,
            Edits = [new SpecEdit(find, replace)],
            DeadlockCheckDisabled = deadlockOff,
            Bounds = bounds ?? [],
        };

    private static CfgAssignment Bound(string name, string value) => new(name, IsOverride: false, value);

    private static string Key(string config, IReadOnlyList<string> options, IReadOnlyDictionary<string, string>? siblings = null) =>
        TlcControlRunKey.Compute(SpecificationPath, BaseSpecification, config, options, siblings ?? Siblings);

    private static string FullBoundKey(SpecMutation mutation) => Key(mutation.BuildConfig(BaseConfig), mutation.TlcOptions);

    private static string MatchingBoundKey(SpecMutation mutation) =>
        Key(mutation.BuildMatchingControlConfig(BaseConfig), mutation.TlcOptions);

    /// <summary>
    /// The case the change exists for: two mutations that differ only in name
    /// and edits build byte-identical control cfgs, share one control key, and
    /// one cache entry runs once for both.
    /// </summary>
    [Test]
    public void Mutations_with_the_same_module_target_bounds_and_options_share_one_control_run()
    {
        var first = Mutation("SafeFirst", find: "A == 1", replace: "A == 7", bounds: [Bound("N", "2")]);
        var second = Mutation("SafeSecond", find: "B == 2", replace: "B == 8", bounds: [Bound("N", "2")]);

        var cache = new TlcControlRunCache<int>();
        var launches = 0;
        int Run() => Interlocked.Increment(ref launches);

        Assert.Multiple(() =>
        {
            Assert.That(first.BuildConfig(BaseConfig), Is.EqualTo(second.BuildConfig(BaseConfig)));
            Assert.That(first.BuildMatchingControlConfig(BaseConfig), Is.EqualTo(second.BuildMatchingControlConfig(BaseConfig)));
            Assert.That(FullBoundKey(first), Is.EqualTo(FullBoundKey(second)));
            Assert.That(MatchingBoundKey(first), Is.EqualTo(MatchingBoundKey(second)));

            Assert.That(cache.GetOrRun(FullBoundKey(first), Run), Is.EqualTo(1));
            Assert.That(cache.GetOrRun(FullBoundKey(second), Run), Is.EqualTo(1));
            Assert.That(cache.GetOrRun(MatchingBoundKey(first), Run), Is.EqualTo(2));
            Assert.That(cache.GetOrRun(MatchingBoundKey(second), Run), Is.EqualTo(2));
            Assert.That(launches, Is.EqualTo(2), "two distinct controls, each launched once for both mutations");
            Assert.That(cache.Count, Is.EqualTo(2));
        });
    }

    /// <summary>
    /// The mutant arm keeps naming its mutation, which is the diagnostic the
    /// control cfgs gave up; only the comment header separates it from the
    /// matching-bound control.
    /// </summary>
    [Test]
    public void The_mutant_cfg_names_its_mutation_and_otherwise_matches_the_matching_bound_control()
    {
        var mutation = Mutation("SafeNamed", bounds: [Bound("N", "2")]);

        var mutant = mutation.BuildMutantConfig(BaseConfig);
        var matching = mutation.BuildMatchingControlConfig(BaseConfig);

        Assert.Multiple(() =>
        {
            Assert.That(FirstLine(mutant), Is.EqualTo("\\* Generated for mutation SafeNamed. Do not check this file in."));
            Assert.That(mutation.BuildConfig(BaseConfig), Does.Not.Contain("SafeNamed"));
            Assert.That(matching, Does.Not.Contain("SafeNamed"));
            Assert.That(WithoutComments(matching), Is.EqualTo(WithoutComments(mutant)));
        });
    }

    /// <summary>
    /// Different bounds share the full-bound control, which never carries
    /// bounds, but not the matching-bound control.
    /// </summary>
    [Test]
    public void Different_bounds_share_the_full_bound_control_but_not_the_matching_bound_control()
    {
        var two = Mutation("SafeAtTwo", bounds: [Bound("N", "2")]);
        var one = Mutation("SafeAtOne", bounds: [Bound("N", "1")]);
        var none = Mutation("SafeAtBase");

        Assert.Multiple(() =>
        {
            Assert.That(FullBoundKey(two), Is.EqualTo(FullBoundKey(one)));
            Assert.That(FullBoundKey(two), Is.EqualTo(FullBoundKey(none)));
            Assert.That(MatchingBoundKey(two), Is.Not.EqualTo(MatchingBoundKey(one)));
            Assert.That(MatchingBoundKey(two), Is.Not.EqualTo(FullBoundKey(two)));
            Assert.That(
                none.BuildMatchingControlConfig(BaseConfig),
                Is.EqualTo(none.BuildConfig(BaseConfig)),
                "a mutation without bounds needs no matching-bound control");
        });
    }

    /// <summary>
    /// Different TLC options (a <c>DEADLOCK: off</c> mutation) or a different
    /// target are different experiments.
    /// </summary>
    [Test]
    public void Different_options_or_targets_do_not_share_a_control()
    {
        var deadlockOn = Mutation("SafeDeadlockOn");
        var deadlockOff = Mutation("SafeDeadlockOff", deadlockOff: true);
        var otherTarget = Mutation("LiveOther", target: "Live", propertyClass: SpecPropertyClass.Temporal);

        Assert.Multiple(() =>
        {
            Assert.That(deadlockOn.BuildConfig(BaseConfig), Is.EqualTo(deadlockOff.BuildConfig(BaseConfig)));
            Assert.That(FullBoundKey(deadlockOn), Is.Not.EqualTo(FullBoundKey(deadlockOff)));
            Assert.That(FullBoundKey(deadlockOn), Is.Not.EqualTo(FullBoundKey(otherTarget)));
        });
    }

    /// <summary>
    /// TLC reads every sibling module copied beside the base, and the base
    /// itself, so a change to any of them is a different control.
    /// </summary>
    [Test]
    public void Different_sibling_or_base_text_does_not_share_a_control()
    {
        var config = Mutation("SafeSiblings").BuildConfig(BaseConfig);
        var changedSibling = new Dictionary<string, string>(Siblings, StringComparer.Ordinal)
        {
            ["Helper.tla"] = "---- MODULE Helper ----\nH == 1\n====\n",
        };
        var addedSibling = new Dictionary<string, string>(Siblings, StringComparer.Ordinal)
        {
            ["Extra.tla"] = "---- MODULE Extra ----\n====\n",
        };
        var reordered = Siblings.Reverse().ToDictionary(s => s.Key, s => s.Value, StringComparer.Ordinal);

        var key = Key(config, []);

        Assert.Multiple(() =>
        {
            Assert.That(Key(config, [], changedSibling), Is.Not.EqualTo(key));
            Assert.That(Key(config, [], addedSibling), Is.Not.EqualTo(key));
            Assert.That(
                TlcControlRunKey.Compute(SpecificationPath, BaseSpecification + "\\* edit\n", config, [], Siblings),
                Is.Not.EqualTo(key));
            Assert.That(
                TlcControlRunKey.Compute("/spec/area/Other.tla", BaseSpecification, config, [], Siblings),
                Is.Not.EqualTo(key));
            Assert.That(Key(config, [], reordered), Is.EqualTo(key), "sibling enumeration order is not an input");
        });
    }

    /// <summary>
    /// A failing shared control fails every case that shares it: the run is
    /// launched once and its failure, an exception included, is what every
    /// sharing case observes.
    /// </summary>
    [Test]
    public void A_failing_shared_control_fails_every_case_that_shares_it()
    {
        var cache = new TlcControlRunCache<int>();
        var key = FullBoundKey(Mutation("SafeFailing"));
        var launches = 0;

        int Run()
        {
            Interlocked.Increment(ref launches);
            throw new InvalidOperationException("control failed");
        }

        Assert.Multiple(() =>
        {
            Assert.That(() => cache.GetOrRun(key, Run), Throws.InvalidOperationException.With.Message.EqualTo("control failed"));
            Assert.That(() => cache.GetOrRun(key, Run), Throws.InvalidOperationException.With.Message.EqualTo("control failed"));
            Assert.That(launches, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// Concurrent requests for one key wait for the single run rather than
    /// starting their own.
    /// </summary>
    [Test]
    public void Concurrent_requests_for_one_control_launch_it_once()
    {
        var cache = new TlcControlRunCache<int>();
        var launches = 0;

        var results = Enumerable.Range(0, 8)
            .AsParallel()
            .WithDegreeOfParallelism(8)
            .Select(_ => cache.GetOrRun("key", () =>
            {
                Thread.Sleep(50);
                return Interlocked.Increment(ref launches);
            }))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(results, Is.All.EqualTo(1));
            Assert.That(launches, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// The cache refuses an empty key or a missing run.
    /// </summary>
    [Test]
    public void The_cache_refuses_an_empty_key_or_a_missing_run()
    {
        var cache = new TlcControlRunCache<int>();

        Assert.Multiple(() =>
        {
            Assert.That(() => cache.GetOrRun(string.Empty, () => 0), Throws.ArgumentException);
            Assert.That(() => cache.GetOrRun("key", null!), Throws.ArgumentNullException);
            Assert.That(() => TlcControlRunKey.For(null!, BaseSpecification, BaseConfig, []), Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// Over the repository's real catalogue, the control launches are exactly
    /// the distinct experiments the base run does not serve: full-bound
    /// controls are one per module, target and options unless
    /// <see cref="TlcBaseRunSubsumption.ServesFullBoundControl"/> serves them
    /// from the base run (#5161), and matching-bound controls one per module,
    /// target, options and bounds. A cfg or key that reintroduced a
    /// per-mutation input (the mutation-name header #5152 removed) would break
    /// the equality and is caught here without a JVM. The counts, and the
    /// launches when every TLC shard category runs in its own process (sharing
    /// is process-local), are written to the test output for the CI budget in
    /// <c>spec/README.md</c>.
    /// </summary>
    [Test]
    public void Repository_control_launches_equal_the_distinct_control_experiments()
    {
        var moduleCount = 0;
        var variantCount = 0;
        var mutationCount = 0;
        var deadlockOnCount = 0;
        var matchingRequests = 0;
        var servedRequests = 0;
        var fullBoundKeys = new HashSet<string>(StringComparer.Ordinal);
        var launchedFullBoundKeys = new HashSet<string>(StringComparer.Ordinal);
        var matchingKeys = new HashSet<string>(StringComparer.Ordinal);
        var fullBoundExperiments = new HashSet<string>(StringComparer.Ordinal);
        var launchedFullBoundExperiments = new HashSet<string>(StringComparer.Ordinal);
        var matchingExperiments = new HashSet<string>(StringComparer.Ordinal);
        var perShardKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var module in SpecModuleCatalogue.Repository())
        {
            moduleCount++;
            variantCount += module.Manifest.Variants.Count;
            var baseSpecification = module.ReadSpecification();
            var baseConfig = module.ReadConfig();
            var siblings = module.ReadSiblingSpecifications();

            foreach (var mutation in module.LoadMutations())
            {
                mutationCount++;
                deadlockOnCount += mutation.DeadlockCheckDisabled ? 1 : 0;
                var options = string.Join(' ', mutation.TlcOptions);
                var experiment = $"{module.Name}\0{mutation.Target}\0{options}";

                var shard = TlcCiShard.Of(module, mutation) ?? "formal-tlc";
                var config = mutation.BuildConfig(baseConfig);
                var fullBoundKey = TlcControlRunKey.Compute(module.SpecificationPath, baseSpecification, config, mutation.TlcOptions, siblings);
                fullBoundKeys.Add(fullBoundKey);
                fullBoundExperiments.Add(experiment);
                if (TlcBaseRunSubsumption.ServesFullBoundControl(module, mutation, baseConfig))
                {
                    servedRequests++;
                }
                else
                {
                    launchedFullBoundKeys.Add(fullBoundKey);
                    launchedFullBoundExperiments.Add(experiment);
                    perShardKeys.Add(shard + "\0" + fullBoundKey);
                }

                var matching = mutation.BuildMatchingControlConfig(baseConfig);
                if (!string.Equals(config, matching, StringComparison.Ordinal))
                {
                    matchingRequests++;
                    var matchingKey = TlcControlRunKey.Compute(module.SpecificationPath, baseSpecification, matching, mutation.TlcOptions, siblings);
                    matchingKeys.Add(matchingKey);
                    perShardKeys.Add(shard + "\0" + matchingKey);
                    matchingExperiments.Add(
                        experiment + "\0" + string.Join(',', mutation.Bounds.Select(b => $"{b.Name}={b.Value}")));
                }
            }
        }

        var launches = launchedFullBoundKeys.Count + matchingKeys.Count;
        TestContext.Out.WriteLine(
            $"Control requests: {mutationCount} full-bound + {matchingRequests} matching-bound = {mutationCount + matchingRequests}.");
        TestContext.Out.WriteLine(
            $"Distinct controls: {fullBoundKeys.Count} full-bound + {matchingKeys.Count} matching-bound = {fullBoundKeys.Count + matchingKeys.Count}.");
        TestContext.Out.WriteLine(
            $"Full-bound requests served by the base run: {servedRequests}; full-bound controls still launched: {launchedFullBoundKeys.Count}.");
        TestContext.Out.WriteLine(
            $"Control launches: {launchedFullBoundKeys.Count} full-bound + {matchingKeys.Count} matching-bound = {launches}.");
        TestContext.Out.WriteLine(
            $"Control launches with each TLC shard category in its own process: {perShardKeys.Count}.");
        TestContext.Out.WriteLine(
            $"TLC launches per full run with each shard in its own process: {moduleCount} base + {variantCount} variant + "
            + $"{perShardKeys.Count} control + {mutationCount} mutant + {deadlockOnCount} deadlock-on = "
            + $"{moduleCount + variantCount + perShardKeys.Count + mutationCount + deadlockOnCount}.");

        Assert.Multiple(() =>
        {
            Assert.That(mutationCount, Is.GreaterThan(0), "the catalogue is empty, so this check would be vacuous");
            Assert.That(fullBoundKeys, Has.Count.EqualTo(fullBoundExperiments.Count));
            Assert.That(launchedFullBoundKeys, Has.Count.EqualTo(launchedFullBoundExperiments.Count));
            Assert.That(matchingKeys, Has.Count.EqualTo(matchingExperiments.Count));
            Assert.That(
                fullBoundKeys.Count,
                Is.LessThan(mutationCount),
                "no two mutations share a full-bound control, so sharing is not happening at all");
            Assert.That(servedRequests, Is.GreaterThan(0), "no full-bound control is served by its base run, so #5161 serves nothing");
            Assert.That(
                launchedFullBoundKeys.Count,
                Is.GreaterThan(0),
                "every full-bound control is served, so the placement rule that keeps an expensive base from running twice is not acting");
        });
    }

    private static string FirstLine(string text) => text.ReplaceLineEndings("\n").Split('\n')[0];

    private static string WithoutComments(string config) =>
        string.Join(
            '\n',
            config.ReplaceLineEndings("\n").Split('\n').Where(line => !line.TrimStart().StartsWith("\\*", StringComparison.Ordinal)));
}
