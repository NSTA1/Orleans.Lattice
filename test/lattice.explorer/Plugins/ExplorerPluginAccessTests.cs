using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginAccessTests
{
    [Test]
    public void Default_is_denied_with_no_reason()
    {
        ExplorerPluginAccess access = default;

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.Reason, Is.Null);
            Assert.That(access, Is.EqualTo(ExplorerPluginAccess.Denied));
        });
    }

    [Test]
    public void Denied_is_the_zero_state_so_an_unset_decision_fails_closed()
    {
        Assert.That((int)ExplorerPluginAccessState.Denied, Is.Zero);
    }

    [Test]
    public void Access_model_declares_exactly_four_states()
    {
        Assert.That(
            Enum.GetValues<ExplorerPluginAccessState>(),
            Is.EquivalentTo(new[]
            {
                ExplorerPluginAccessState.Denied,
                ExplorerPluginAccessState.Allowed,
                ExplorerPluginAccessState.AuthenticationRequired,
                ExplorerPluginAccessState.Unavailable,
            }));
    }

    [Test]
    public void Cached_results_carry_their_state_and_no_reason()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPluginAccess.Allowed.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(ExplorerPluginAccess.Denied.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(
                ExplorerPluginAccess.AuthenticationRequired.State,
                Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(ExplorerPluginAccess.Unavailable.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(ExplorerPluginAccess.Allowed.Reason, Is.Null);
            Assert.That(ExplorerPluginAccess.Denied.Reason, Is.Null);
            Assert.That(ExplorerPluginAccess.AuthenticationRequired.Reason, Is.Null);
            Assert.That(ExplorerPluginAccess.Unavailable.Reason, Is.Null);
        });
    }

    [Test]
    public void IsAllowed_is_true_only_for_allowed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPluginAccess.Allowed.IsAllowed, Is.True);
            Assert.That(ExplorerPluginAccess.Denied.IsAllowed, Is.False);
            Assert.That(ExplorerPluginAccess.AuthenticationRequired.IsAllowed, Is.False);
            Assert.That(ExplorerPluginAccess.Unavailable.IsAllowed, Is.False);
        });
    }

    [Test]
    public void IsVisible_is_false_only_for_unavailable()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPluginAccess.Allowed.IsVisible, Is.True);
            Assert.That(ExplorerPluginAccess.Denied.IsVisible, Is.True);
            Assert.That(ExplorerPluginAccess.AuthenticationRequired.IsVisible, Is.True);
            Assert.That(ExplorerPluginAccess.Unavailable.IsVisible, Is.False);
        });
    }

    [Test]
    public void Allow_with_reason_carries_it()
    {
        var access = ExplorerPluginAccess.Allow("probe succeeded");

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(access.Reason, Is.EqualTo("probe succeeded"));
        });
    }

    [Test]
    public void Deny_with_reason_carries_it()
    {
        var access = ExplorerPluginAccess.Deny("no admin role");

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.Reason, Is.EqualTo("no admin role"));
        });
    }

    [Test]
    public void RequireAuthentication_with_reason_carries_it()
    {
        var access = ExplorerPluginAccess.RequireAuthentication("no credential presented");

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(access.Reason, Is.EqualTo("no credential presented"));
        });
    }

    [Test]
    public void ReportUnavailable_with_reason_carries_it()
    {
        var access = ExplorerPluginAccess.ReportUnavailable("tenancy add-on absent");

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.EqualTo("tenancy add-on absent"));
        });
    }

    [Test]
    public void Factories_return_the_cached_result_when_no_reason_is_supplied()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPluginAccess.Allow(null), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(ExplorerPluginAccess.Deny(null), Is.EqualTo(ExplorerPluginAccess.Denied));
            Assert.That(
                ExplorerPluginAccess.RequireAuthentication(null),
                Is.EqualTo(ExplorerPluginAccess.AuthenticationRequired));
            Assert.That(
                ExplorerPluginAccess.ReportUnavailable(null),
                Is.EqualTo(ExplorerPluginAccess.Unavailable));
        });
    }

    [Test]
    public void Results_with_the_same_state_and_reason_are_equal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPluginAccess.Deny("x"), Is.EqualTo(ExplorerPluginAccess.Deny("x")));
            Assert.That(ExplorerPluginAccess.Deny("x"), Is.Not.EqualTo(ExplorerPluginAccess.Deny("y")));
            Assert.That(ExplorerPluginAccess.Deny("x"), Is.Not.EqualTo(ExplorerPluginAccess.Denied));
        });
    }
}
