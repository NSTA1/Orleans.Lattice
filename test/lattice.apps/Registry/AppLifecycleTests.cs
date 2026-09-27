namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Exhaustive coverage of the pure <see cref="AppLifecycle"/> transition table: every
/// action against an absent record and against every lifecycle state.
/// </summary>
[TestFixture]
public sealed class AppLifecycleTests
{
    // (current state or null for absent, action, expected kind, expected next state, expected error)
    private static readonly object?[][] Table =
    {
        new object?[] { null, AppLifecycleAction.Install, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Installed, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Uninstalled, AppLifecycleAction.Install, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Installed, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Installed, AppLifecycleAction.Install, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.AlreadyInstalled },
        new object?[] { AppRegistryLifecycleState.Enabled, AppLifecycleAction.Install, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.AlreadyInstalled },
        new object?[] { AppRegistryLifecycleState.Disabled, AppLifecycleAction.Install, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.AlreadyInstalled },

        new object?[] { null, AppLifecycleAction.Upgrade, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.NotInstalled },
        new object?[] { AppRegistryLifecycleState.Uninstalled, AppLifecycleAction.Upgrade, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.NotInstalled },
        new object?[] { AppRegistryLifecycleState.Installed, AppLifecycleAction.Upgrade, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Installed, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Enabled, AppLifecycleAction.Upgrade, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Enabled, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Disabled, AppLifecycleAction.Upgrade, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Disabled, AppRegistryTransitionError.None },

        new object?[] { null, AppLifecycleAction.Enable, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.NotInstalled },
        new object?[] { AppRegistryLifecycleState.Uninstalled, AppLifecycleAction.Enable, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.NotInstalled },
        new object?[] { AppRegistryLifecycleState.Installed, AppLifecycleAction.Enable, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Enabled, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Enabled, AppLifecycleAction.Enable, AppLifecycleDecisionKind.NoOp, null, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Disabled, AppLifecycleAction.Enable, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Enabled, AppRegistryTransitionError.None },

        new object?[] { null, AppLifecycleAction.Disable, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.NotInstalled },
        new object?[] { AppRegistryLifecycleState.Uninstalled, AppLifecycleAction.Disable, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.NotInstalled },
        new object?[] { AppRegistryLifecycleState.Installed, AppLifecycleAction.Disable, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.InvalidTransition },
        new object?[] { AppRegistryLifecycleState.Enabled, AppLifecycleAction.Disable, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Disabled, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Disabled, AppLifecycleAction.Disable, AppLifecycleDecisionKind.NoOp, null, AppRegistryTransitionError.None },

        new object?[] { null, AppLifecycleAction.Uninstall, AppLifecycleDecisionKind.Reject, null, AppRegistryTransitionError.NotInstalled },
        new object?[] { AppRegistryLifecycleState.Uninstalled, AppLifecycleAction.Uninstall, AppLifecycleDecisionKind.NoOp, null, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Installed, AppLifecycleAction.Uninstall, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Uninstalled, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Enabled, AppLifecycleAction.Uninstall, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Uninstalled, AppRegistryTransitionError.None },
        new object?[] { AppRegistryLifecycleState.Disabled, AppLifecycleAction.Uninstall, AppLifecycleDecisionKind.Apply, AppRegistryLifecycleState.Uninstalled, AppRegistryTransitionError.None },
    };

    // Parameters are object-typed because the table's action and decision-kind enums are
    // internal and a public test method cannot expose them.
    [TestCaseSource(nameof(Table))]
    public void Evaluate_matches_the_lifecycle_table(
        object? state,
        object action,
        object kind,
        object? next,
        object error)
    {
        var current = state is AppRegistryLifecycleState s ? AppRegistryTestData.Record(s) : null;

        var decision = AppLifecycle.Evaluate(current, (AppLifecycleAction)action);

        Assert.That(decision.Kind, Is.EqualTo((AppLifecycleDecisionKind)kind));
        Assert.That(decision.Error, Is.EqualTo((AppRegistryTransitionError)error));
        if (next is AppRegistryLifecycleState expected)
        {
            Assert.That(decision.NextState, Is.EqualTo(expected));
        }

        Assert.That(decision.Message is null, Is.EqualTo(decision.Kind != AppLifecycleDecisionKind.Reject),
            "exactly the rejections carry a diagnostic");
    }

    [Test]
    public void Table_covers_every_action_against_absence_and_every_state()
    {
        var covered = Table.Select(row => ((AppRegistryLifecycleState?)row[0], (AppLifecycleAction)row[1]!)).ToHashSet();
        foreach (var action in Enum.GetValues<AppLifecycleAction>())
        {
            Assert.That(covered, Does.Contain(((AppRegistryLifecycleState?)null, action)));
            foreach (var state in Enum.GetValues<AppRegistryLifecycleState>())
            {
                Assert.That(covered, Does.Contain(((AppRegistryLifecycleState?)state, action)));
            }
        }
    }

    [TestCase(AppRegistryLifecycleState.Installed)]
    [TestCase(AppRegistryLifecycleState.Disabled)]
    public void Enable_rejects_a_record_whose_ceiling_is_not_pinned_to_its_version(AppRegistryLifecycleState state)
    {
        var current = AppRegistryTestData.Record(state, version: AppRegistryTestData.V2, ceilingVersion: AppRegistryTestData.V1);

        var decision = AppLifecycle.Evaluate(current, AppLifecycleAction.Enable);

        Assert.That(decision.Kind, Is.EqualTo(AppLifecycleDecisionKind.Reject));
        Assert.That(decision.Error, Is.EqualTo(AppRegistryTransitionError.CeilingNotPinned));
    }

    [Test]
    public void Upgrade_is_allowed_for_an_unpinned_record_because_it_is_the_re_consent_path()
    {
        var current = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, version: AppRegistryTestData.V2, ceilingVersion: AppRegistryTestData.V1);

        var decision = AppLifecycle.Evaluate(current, AppLifecycleAction.Upgrade);

        Assert.That(decision.Kind, Is.EqualTo(AppLifecycleDecisionKind.Apply));
        Assert.That(decision.NextState, Is.EqualTo(AppRegistryLifecycleState.Enabled));
    }

    [Test]
    public void Evaluate_unknown_action_throws()
    {
        Assert.That(() => AppLifecycle.Evaluate(null, (AppLifecycleAction)99), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
