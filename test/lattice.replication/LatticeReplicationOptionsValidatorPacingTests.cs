namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeReplicationOptionsValidator"/> guards the
/// sibling <see cref="LatticeReplicationOptionsValidatorTests"/> does not reach:
/// the four shipper pacing knobs whose non-positive values would tight-loop or
/// stall the outbound ship loop, and the two per-tree declaration guards on
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/>.
/// </summary>
/// <remarks>
/// Every one of these is a fail-fast startup guard, so the only way it can be
/// wrong is by not firing. A guard that silently stopped rejecting its value
/// would let a silo start with a configuration that tight-loops the shipper or
/// leaves the WAL GC unable to advance - a production fault with no local symptom
/// - which is exactly why each is asserted here rather than inferred from the
/// chain around it. Deterministic - pure validation, no cluster.
/// </remarks>
[TestFixture]
public sealed class LatticeReplicationOptionsValidatorPacingTests
{
    private static readonly LatticeReplicationOptionsValidator Validator = new();

    /// <summary>A minimally-valid options instance, mutated one field at a time per test.</summary>
    private static LatticeReplicationOptions Valid() => new() { ClusterId = "site-a" };

    private static void AssertRejects(LatticeReplicationOptions options, string expectedFieldName)
    {
        var result = Validator.Validate(name: null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True,
                $"A non-viable {expectedFieldName} must be rejected at startup, not at the first pump tick.");
            Assert.That(result.FailureMessage, Does.Contain(expectedFieldName),
                "The failure must name the offending option so an operator can fix it without a debugger.");
        });
    }

    [Test]
    public void Baseline_options_are_valid_so_each_rejection_below_is_attributable()
    {
        var result = Validator.Validate(name: null, Valid());

        Assert.That(result.Failed, Is.False,
            "The baseline must validate, otherwise a rejection below could come from an unrelated field.");
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_rejects_a_non_positive_ship_partition_page_size(int pageSize)
    {
        var options = Valid();
        options.ShipPartitionPageSize = pageSize;

        AssertRejects(options, nameof(LatticeReplicationOptions.ShipPartitionPageSize));
    }

    [TestCase(0)]
    [TestCase(-5)]
    public void Validate_rejects_a_non_positive_ship_cursor_write_interval(int interval)
    {
        var options = Valid();
        options.ShipCursorWriteInterval = interval;

        AssertRejects(options, nameof(LatticeReplicationOptions.ShipCursorWriteInterval));
    }

    [Test]
    public void Validate_rejects_a_non_positive_ship_phase_timer_period()
    {
        var options = Valid();
        options.ShipPhaseTimerPeriod = TimeSpan.Zero;

        AssertRejects(options, nameof(LatticeReplicationOptions.ShipPhaseTimerPeriod));
    }

    [Test]
    public void Validate_rejects_a_negative_ship_phase_timer_period()
    {
        var options = Valid();
        options.ShipPhaseTimerPeriod = TimeSpan.FromSeconds(-1);

        AssertRejects(options, nameof(LatticeReplicationOptions.ShipPhaseTimerPeriod));
    }

    [Test]
    public void Validate_rejects_a_non_positive_source_identity_backstop_interval()
    {
        var options = Valid();
        options.ShipSourceIdentityBackstopInterval = TimeSpan.Zero;

        AssertRejects(options, nameof(LatticeReplicationOptions.ShipSourceIdentityBackstopInterval));
    }

    [Test]
    public void Validate_rejects_a_negative_source_identity_backstop_interval()
    {
        var options = Valid();
        options.ShipSourceIdentityBackstopInterval = TimeSpan.FromMilliseconds(-1);

        AssertRejects(options, nameof(LatticeReplicationOptions.ShipSourceIdentityBackstopInterval));
    }

    [TestCase("")]
    [TestCase("   ")]
    public void Validate_rejects_a_replicated_tree_declared_under_a_blank_tree_id(string treeId)
    {
        var options = Valid();
        options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
        {
            [treeId] = LatticeMergeMode.LwwRegister,
        };

        AssertRejects(options, nameof(LatticeReplicationOptions.ReplicatedTrees));
    }

    [Test]
    public void Validate_rejects_a_replicated_tree_declared_with_an_undefined_merge_mode()
    {
        var options = Valid();
        options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = (LatticeMergeMode)9999,
        };

        var result = Validator.Validate(name: null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True,
                "An undefined merge mode must be rejected: the commit-time observer could not resolve it.");
            Assert.That(result.FailureMessage, Does.Contain("orders"),
                "The failure must name the offending tree.");
            Assert.That(result.FailureMessage, Does.Contain("9999"),
                "The failure must quote the undefined value so the misconfiguration is obvious.");
        });
    }

    [Test]
    public void Validate_accepts_every_defined_merge_mode_for_a_replicated_tree()
    {
        var options = Valid();
        options.ReplicatedTrees = Enum.GetValues<LatticeMergeMode>()
            .ToDictionary(mode => $"tree-{mode}", mode => mode);

        var result = Validator.Validate(name: null, options);

        Assert.That(result.Failed, Is.False,
            "Every merge mode the enum defines must be declarable, or a valid configuration would be rejected.");
    }

    [Test]
    public void Validate_accepts_an_empty_replicated_tree_map()
    {
        var options = Valid();
        options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>();

        var result = Validator.Validate(name: null, options);

        Assert.That(result.Failed, Is.False);
    }
}
