namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// When a module's clean base run is allowed to stand in for a full-bound
/// control (#5161, <see cref="TlcBaseRunSubsumption"/>). A control served
/// wrongly would read as clean whatever the property does on the base, which is
/// the vacuity the control arm exists to prevent, so every refusal below is a
/// negative control: each one is red if subsumption is applied where it does
/// not hold. Toolchain-free; the TLC-level control for a base cfg that lacks
/// the target is in <see cref="TlcModelCheckTests.A_synthetic_module_is_model_checked_by_every_TLC_gate"/>.
/// </summary>
[TestFixture]
public sealed class TlcBaseRunSubsumptionTests
{
    private static readonly string BaseConfig = """
        \* TLC model.
        SPECIFICATION Spec
        CONSTANTS
            N = 3
        INVARIANTS
            TypeOK
            Safe
        PROPERTIES
            Live
        """.ReplaceLineEndings("\n");

    private static SpecMutation Mutation(
        string target = "Safe",
        SpecPropertyClass propertyClass = SpecPropertyClass.Invariant,
        bool deadlockOff = false,
        IReadOnlyList<CfgAssignment>? bounds = null) =>
        new()
        {
            Name = "M" + target,
            Module = "M" + target,
            Target = target,
            PropertyClass = propertyClass,
            Summary = target,
            Edits = [new SpecEdit("A", "B")],
            DeadlockCheckDisabled = deadlockOff,
            Bounds = bounds ?? [],
        };

    private static string? Refute(SpecMutation mutation, string? baseConfig = null, IReadOnlyList<string>? baseOptions = null) =>
        TlcBaseRunSubsumption.Refute(
            baseConfig ?? BaseConfig,
            baseOptions ?? TlcBaseRunSubsumption.BaseRunOptions,
            mutation.BuildConfig(baseConfig ?? BaseConfig),
            mutation.TlcOptions);

    /// <summary>
    /// The positive controls: an invariant, an action or temporal property,
    /// and <c>TypeOK</c> itself, each checked by the base cfg in the same
    /// block under the same carried configuration, are subsumed; so is a
    /// <c>DEADLOCK: off</c> control, which checks less than the base run.
    /// </summary>
    [Test]
    public void A_full_bound_control_the_base_cfg_checks_is_subsumed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Refute(Mutation("Safe")), Is.Null);
            Assert.That(Refute(Mutation("TypeOK")), Is.Null);
            Assert.That(Refute(Mutation("Live", SpecPropertyClass.Temporal)), Is.Null);
            Assert.That(Refute(Mutation("Safe", deadlockOff: true)), Is.Null, "a DEADLOCK: off control is weaker than the base run");
            Assert.That(
                TlcBaseRunSubsumption.Subsumes(BaseConfig, [], Mutation().BuildConfig(BaseConfig), []),
                Is.True);
        });
    }

    /// <summary>A target the base cfg does not check is not proved by the base run.</summary>
    [Test]
    public void A_target_the_base_cfg_does_not_check_still_launches_a_control()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Refute(Mutation("Unchecked")), Does.Contain("does not check Unchecked"));
            Assert.That(
                Refute(Mutation("Safe"), baseConfig: BaseConfig.Replace("    Safe\n", string.Empty, StringComparison.Ordinal)),
                Does.Contain("INVARIANTS block does not check Safe"));
        });
    }

    /// <summary>
    /// A temporal formula listed under <c>INVARIANTS</c> is evaluated per
    /// state, so checking a name in one block proves nothing about it in the
    /// other.
    /// </summary>
    [Test]
    public void A_target_checked_in_the_other_block_still_launches_a_control()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Refute(Mutation("Safe", SpecPropertyClass.Action)), Does.Contain("PROPERTIES block does not check Safe"));
            Assert.That(Refute(Mutation("Live", SpecPropertyClass.Invariant)), Does.Contain("INVARIANTS block does not check Live"));
        });
    }

    /// <summary>
    /// Different constants are a different instance: a matching-bound control
    /// (a <c>BOUNDS:</c> override) and a base cfg whose carried configuration
    /// differs are both refused.
    /// </summary>
    [Test]
    public void Different_constants_still_launch_a_control()
    {
        var bounded = Mutation("Safe", bounds: [new CfgAssignment("N", IsOverride: false, "2")]);
        var otherBase = BaseConfig.Replace("N = 3", "N = 4", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(
                TlcBaseRunSubsumption.Refute(BaseConfig, [], bounded.BuildMatchingControlConfig(BaseConfig), bounded.TlcOptions),
                Does.Contain("carried configuration"));
            Assert.That(
                TlcBaseRunSubsumption.Refute(otherBase, [], Mutation().BuildConfig(BaseConfig), []),
                Does.Contain("carried configuration"));
            Assert.That(Refute(bounded), Is.Null, "the full-bound control of a bounded mutation still carries the base's own constants");
        });
    }

    /// <summary>
    /// A control that checks more than the base run is not proved by it: one
    /// that drops an option the base run uses, or adds one not known only to
    /// weaken a check.
    /// </summary>
    [Test]
    public void Stronger_control_options_still_launch_a_control()
    {
        var control = Mutation().BuildConfig(BaseConfig);

        Assert.Multiple(() =>
        {
            Assert.That(
                TlcBaseRunSubsumption.Refute(BaseConfig, [SpecMutation.DeadlockSwitch], control, []),
                Does.Contain("without the base run's option(s) -deadlock"));
            Assert.That(
                TlcBaseRunSubsumption.Refute(BaseConfig, [], control, ["-lncheck", "final"]),
                Does.Contain("-lncheck final"));
        });
    }

    /// <summary>
    /// Comments and blank lines are not part of the instance, so the generated
    /// control's header comment does not stop it being subsumed.
    /// </summary>
    [Test]
    public void Comments_and_blank_lines_do_not_matter()
    {
        var spaced = "\\* A different comment.\n\n" + BaseConfig.Replace("CONSTANTS", "\nCONSTANTS \\* the bounds", StringComparison.Ordinal);

        Assert.That(TlcBaseRunSubsumption.Refute(spaced, [], Mutation().BuildConfig(BaseConfig), []), Is.Null);
    }

    /// <summary>
    /// The synthetic module's single control is served: its base cfg checks
    /// TypeOK and nothing routes it out of the catch-all. This is the positive
    /// control the refusals below are measured against.
    /// </summary>
    [Test]
    public void The_synthetic_module_control_is_served_by_its_base_run()
    {
        using var synthetic = SyntheticSpecModule.Create();
        var module = synthetic.Discover();

        Assert.That(
            TlcBaseRunSubsumption.ServesFullBoundControl(module, module.LoadMutations().Single(), module.ReadConfig()),
            Is.True);
    }

    /// <summary>
    /// A synthetic base cfg that lacks the target must still launch a control.
    /// </summary>
    [Test]
    public void A_synthetic_base_cfg_that_lacks_the_target_launches_a_control()
    {
        using var synthetic = SyntheticSpecModule.Create();
        synthetic.Replace($"{SyntheticSpecModule.ModuleName}.tla", "Wrap == 3", "Wrap == 3\n\nBounded == x < 2");
        synthetic.Replace($"mutations/{SyntheticSpecModule.MutationName}.mutation", "TARGET: TypeOK", "TARGET: Bounded");
        var module = synthetic.Discover();

        Assert.That(
            TlcBaseRunSubsumption.ServesFullBoundControl(module, module.LoadMutations().Single(), module.ReadConfig()),
            Is.False);
    }

    /// <summary>
    /// A synthetic base cfg with different constants from the control must
    /// still launch it: the matching-bound control of a <c>BOUNDS:</c>
    /// mutation is refused, while its full-bound control is served.
    /// </summary>
    [Test]
    public void A_synthetic_control_with_different_constants_launches_a_control()
    {
        using var synthetic = SyntheticSpecModule.Create();
        synthetic.Replace($"mutations/{SyntheticSpecModule.MutationName}.mutation", "PERTURBS: Step\n", "PERTURBS: Step\nBOUNDS: Wrap = 4\n");
        var module = synthetic.Discover();
        var mutation = module.LoadMutations().Single();
        var baseConfig = module.ReadConfig();

        Assert.Multiple(() =>
        {
            Assert.That(
                TlcBaseRunSubsumption.Subsumes(baseConfig, TlcBaseRunSubsumption.BaseRunOptions, mutation.BuildMatchingControlConfig(baseConfig), mutation.TlcOptions),
                Is.False);
            Assert.That(TlcBaseRunSubsumption.ServesFullBoundControl(module, mutation, baseConfig), Is.True);
        });
    }

    /// <summary>
    /// A synthetic control with options stronger than the base run's must
    /// still launch: the base run checks deadlock, and a base run without that
    /// check cannot stand in for a control with it.
    /// </summary>
    [Test]
    public void A_synthetic_control_with_stronger_options_launches_a_control()
    {
        using var synthetic = SyntheticSpecModule.Create();
        var module = synthetic.Discover();
        var mutation = module.LoadMutations().Single();
        var baseConfig = module.ReadConfig();

        Assert.That(
            TlcBaseRunSubsumption.Subsumes(baseConfig, [SpecMutation.DeadlockSwitch], mutation.BuildConfig(baseConfig), mutation.TlcOptions),
            Is.False);
    }

    /// <summary>
    /// Placement: a subsumed control is served only where the base case runs
    /// in the same shard. ReplayPermits' base stays in the scheduling shard and
    /// its mutations run in workload B, so its controls launch rather than run
    /// the expensive base a second time; AtomicCommit's base is co-located with
    /// its mutations, so its controls are served.
    /// </summary>
    [Test]
    public void A_subsumed_control_is_served_only_where_the_base_shares_its_shard()
    {
        var replay = Module("ReplayPermits");
        var replayMutation = replay.LoadMutations().First(m => m.Bounds.Count == 0);
        var atomic = Module("AtomicCommit");
        var atomicMutation = atomic.LoadMutations().First();

        Assert.Multiple(() =>
        {
            Assert.That(
                TlcBaseRunSubsumption.Subsumes(replay.ReadConfig(), [], replayMutation.BuildConfig(replay.ReadConfig()), replayMutation.TlcOptions),
                Is.True,
                "the ReplayPermits control is subsumed, so only placement keeps it launching");
            Assert.That(TlcCiShard.SharesBaseShard(replay, replayMutation), Is.False);
            Assert.That(TlcBaseRunSubsumption.ServesFullBoundControl(replay, replayMutation, replay.ReadConfig()), Is.False);
            Assert.That(TlcCiShard.SharesBaseShard(atomic, atomicMutation), Is.True);
            Assert.That(TlcBaseRunSubsumption.ServesFullBoundControl(atomic, atomicMutation, atomic.ReadConfig()), Is.True);
        });
    }

    /// <summary>The check refuses missing arguments.</summary>
    [Test]
    public void Arguments_are_validated()
    {
        var module = Module("AtomicCommit");
        var mutation = module.LoadMutations().First();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => TlcBaseRunSubsumption.Refute(null!, [], BaseConfig, []));
            Assert.Throws<ArgumentNullException>(() => TlcBaseRunSubsumption.Refute(BaseConfig, null!, BaseConfig, []));
            Assert.Throws<ArgumentNullException>(() => TlcBaseRunSubsumption.Refute(BaseConfig, [], null!, []));
            Assert.Throws<ArgumentNullException>(() => TlcBaseRunSubsumption.Refute(BaseConfig, [], BaseConfig, null!));
            Assert.Throws<ArgumentNullException>(() => TlcBaseRunSubsumption.ServesFullBoundControl(null!, mutation, BaseConfig));
            Assert.Throws<ArgumentNullException>(() => TlcBaseRunSubsumption.ServesFullBoundControl(module, null!, BaseConfig));
            Assert.Throws<ArgumentNullException>(() => TlcBaseRunSubsumption.ServesFullBoundControl(module, mutation, null!));
        });
    }

    private static SpecModule Module(string name) =>
        SpecModuleCatalogue.Repository().Single(module => module.Name == name);
}
