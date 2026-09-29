using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// The failure mapper turns the engine's value-shaped rejections into the exception
/// categories the app-control contract promises. The category is load-bearing: a caller
/// distinguishes "this app is not installed" from "this app could not be changed" by
/// catching <see cref="KeyNotFoundException"/> rather than by reading the message.
/// </summary>
[TestFixture]
public sealed class AppsControlFailuresTests
{
    private static AppSlug Slug => AppSlug.Parse(AppsControlHarness.Slug);

    [Test]
    public void FromTransition_maps_a_not_installed_rejection_to_the_not_found_category()
    {
        // The registry reports an absent app as a transition error rather than as a null
        // record, so this arm is the only thing keeping that rejection in the not-found
        // category; every other transition error maps to invalid-operation.
        var mapped = AppsControlFailures.FromTransition(
            Slug, "enable", AppsControlHarness.Rejected(AppRegistryTransitionError.NotInstalled));

        Assert.Multiple(() =>
        {
            Assert.That(mapped, Is.TypeOf<KeyNotFoundException>());
            Assert.That(mapped.Message, Does.Contain("is not installed"));
            Assert.That(mapped.Message, Does.Contain(AppsControlHarness.Slug));
        });
    }

    [Test]
    public void FromTransition_maps_an_already_installed_rejection_to_invalid_operation()
    {
        var mapped = AppsControlFailures.FromTransition(
            Slug, "install", AppsControlHarness.Rejected(AppRegistryTransitionError.AlreadyInstalled));

        Assert.Multiple(() =>
        {
            Assert.That(mapped, Is.TypeOf<InvalidOperationException>());
            Assert.That(mapped.Message, Does.Contain("already installed"));
        });
    }

    [Test]
    public void FromTransition_names_the_verb_and_sanitizes_the_engine_message_for_every_other_error()
    {
        // Anti-vacuity for the two arms above: the fall-through arm is the one that would
        // absorb them if the switch stopped discriminating, and it is distinguishable only
        // by carrying the verb and the engine's own detail.
        var mapped = AppsControlFailures.FromTransition(
            Slug,
            "disable",
            AppsControlHarness.Rejected(AppRegistryTransitionError.TreeOwnershipConflict, "tree 'a/crm/contacts' is pinned"));

        Assert.Multiple(() =>
        {
            Assert.That(mapped, Is.TypeOf<InvalidOperationException>());
            Assert.That(mapped.Message, Does.Contain("disable"));
            Assert.That(mapped.Message, Does.Contain("contacts"));
            Assert.That(mapped.Message, Does.Not.Contain("a/crm"), "the engine detail is sanitized");
        });
    }
}
