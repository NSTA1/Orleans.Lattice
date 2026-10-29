namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// When a module's base run proves a full-bound control clean, so the control
/// need not be launched (#5161).
/// <para>
/// A full-bound control checks the unmutated base under the base cfg's own
/// carried configuration with <c>TypeOK</c> and one target property. The base
/// run checks the same specification under the same carried configuration,
/// with every property of the base cfg at once. When the control's checked
/// properties are a subset of the base cfg's, block by block, and its TLC
/// options check nothing the base run does not, a clean base run implies a
/// clean control: the reachable states and behaviours are identical, and every
/// check the control makes the base run also made.
/// </para>
/// <para>
/// The check is toolchain-free and conservative: anything it cannot prove
/// (a different carried configuration, a property the base cfg does not check
/// in the same block, an option not known only to weaken a check) leaves the
/// control to run. Matching-bound controls carry a <c>BOUNDS:</c> override, so
/// their carried configuration differs from the base cfg's and they always run.
/// </para>
/// <para>
/// PLACEMENT. Serving also requires the module's base case to run in the same
/// CI shard as the mutation (<see cref="TlcCiShard.SharesBaseShard"/>):
/// sharing is process-local, so serving a control in another shard would
/// launch the base a second time there, which for an expensive base such as
/// ReplayPermits costs more than the controls it replaces.
/// </para>
/// </summary>
internal static class TlcBaseRunSubsumption
{
    /// <summary>The extra TLC options the base run is launched with: none, so TLC's deadlock check is on.</summary>
    public static IReadOnlyList<string> BaseRunOptions { get; } = [];

    /// <summary>
    /// The TLC options known only to remove a check, so a run with one of them
    /// checks no more than the same run without it: <c>-deadlock</c> turns the
    /// deadlock check off.
    /// </summary>
    public static IReadOnlyList<string> WeakeningOptions { get; } = [SpecMutation.DeadlockSwitch];

    /// <summary>
    /// Why a clean run under <paramref name="baseConfig"/> with
    /// <paramref name="baseOptions"/> does NOT prove a run under
    /// <paramref name="controlConfig"/> with <paramref name="controlOptions"/>
    /// clean, or <see langword="null"/> when it does. Both runs are of the same
    /// specification and sibling modules.
    /// </summary>
    /// <param name="baseConfig">The base cfg text.</param>
    /// <param name="baseOptions">The extra TLC options of the base run.</param>
    /// <param name="controlConfig">The control cfg text.</param>
    /// <param name="controlOptions">The extra TLC options of the control.</param>
    public static string? Refute(
        string baseConfig,
        IReadOnlyList<string> baseOptions,
        string controlConfig,
        IReadOnlyList<string> controlOptions)
    {
        ArgumentNullException.ThrowIfNull(baseConfig);
        ArgumentNullException.ThrowIfNull(baseOptions);
        ArgumentNullException.ThrowIfNull(controlConfig);
        ArgumentNullException.ThrowIfNull(controlOptions);

        if (!string.Equals(
                SpecMutationCatalogue.CarriedConfiguration(controlConfig),
                SpecMutationCatalogue.CarriedConfiguration(baseConfig),
                StringComparison.Ordinal))
        {
            return "the control's carried configuration (everything but its INVARIANT and PROPERTY blocks) "
                + "differs from the base cfg's, so it checks a different instance.";
        }

        var checkedByBase = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(baseConfig);
        var checkedByControl = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(controlConfig);
        foreach (var block in new[] { SpecMutationCatalogue.InvariantsBlock, SpecMutationCatalogue.PropertiesBlock })
        {
            var missing = checkedByControl[block].Except(checkedByBase[block], StringComparer.Ordinal).ToArray();
            if (missing.Length > 0)
            {
                return $"the base cfg's {block} block does not check {string.Join(", ", missing)}, which the control does.";
            }
        }

        var dropped = baseOptions.Except(controlOptions, StringComparer.Ordinal).ToArray();
        if (dropped.Length > 0)
        {
            return $"the control runs without the base run's option(s) {string.Join(" ", dropped)}, so it may check more.";
        }

        var stronger = controlOptions
            .Except(baseOptions, StringComparer.Ordinal)
            .Except(WeakeningOptions, StringComparer.Ordinal)
            .ToArray();
        if (stronger.Length > 0)
        {
            return $"the control runs with option(s) {string.Join(" ", stronger)} that the base run does not, "
                + $"and only {string.Join(" ", WeakeningOptions)} is known only to weaken a check.";
        }

        return null;
    }

    /// <summary>Whether a clean base run proves the control clean; see <see cref="Refute"/>.</summary>
    /// <param name="baseConfig">The base cfg text.</param>
    /// <param name="baseOptions">The extra TLC options of the base run.</param>
    /// <param name="controlConfig">The control cfg text.</param>
    /// <param name="controlOptions">The extra TLC options of the control.</param>
    public static bool Subsumes(
        string baseConfig,
        IReadOnlyList<string> baseOptions,
        string controlConfig,
        IReadOnlyList<string> controlOptions) =>
        Refute(baseConfig, baseOptions, controlConfig, controlOptions) is null;

    /// <summary>
    /// Whether <see cref="TlcModelCheckTests"/> serves <paramref name="mutation"/>'s
    /// full-bound control from the module's base run instead of launching it:
    /// the base run subsumes the control, and the base case runs in the
    /// mutation's CI shard.
    /// </summary>
    /// <param name="module">The module the mutation belongs to.</param>
    /// <param name="mutation">The mutation whose full-bound control is in question.</param>
    /// <param name="baseConfig">The module's base cfg text.</param>
    public static bool ServesFullBoundControl(SpecModule module, SpecMutation mutation, string baseConfig)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentNullException.ThrowIfNull(mutation);
        ArgumentNullException.ThrowIfNull(baseConfig);

        return TlcCiShard.SharesBaseShard(module, mutation)
            && Subsumes(baseConfig, BaseRunOptions, mutation.BuildConfig(baseConfig), mutation.TlcOptions);
    }
}
