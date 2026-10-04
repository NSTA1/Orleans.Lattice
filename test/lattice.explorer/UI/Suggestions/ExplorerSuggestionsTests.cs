using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Suggestions;

/// <summary>
/// The circuit's suggestion hub over a head that registers nothing. Both of its guards
/// are fail-closed refusals that let a picker degrade to free text rather than fail a
/// render, and neither is reachable from a page: a dependency the head never registered
/// and a facade it cannot construct both end in a source answering "unavailable". The
/// two refusals are distinguished here by the note each one shows, because that is the
/// only thing the caller ever sees of them.
/// </summary>
[TestFixture]
public sealed class ExplorerSuggestionsTests
{
    [Test]
    public async Task A_source_whose_dependency_the_head_never_registered_degrades_to_free_text()
    {
        var hub = new ExplorerSuggestions(new ServiceCollection().BuildServiceProvider());

        var answer = await hub.Trees.SuggestAsync("o", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(hub.Trees, Is.SameAs(UnavailableSuggestionSource.Instance));
            Assert.That(answer.UnavailableReason, Is.EqualTo(UnavailableSuggestionSource.Reason));
            Assert.That(answer.Items, Is.Empty);
        });
    }

    [Test]
    public void Every_source_whose_dependencies_are_required_refuses_the_same_way()
    {
        var hub = new ExplorerSuggestions(new ServiceCollection().BuildServiceProvider());

        Assert.Multiple(() =>
        {
            Assert.That(hub.Trees, Is.SameAs(UnavailableSuggestionSource.Instance));
            Assert.That(hub.ClusterTrees, Is.SameAs(UnavailableSuggestionSource.Instance));
            Assert.That(hub.Tenants, Is.SameAs(UnavailableSuggestionSource.Instance));

            // The contrast that keeps the guard honest: a source whose dependencies are
            // all optional is still built, so the refusal above is the guard firing and
            // not simply "an empty provider refuses everything".
            Assert.That(hub.Regions, Is.Not.SameAs(UnavailableSuggestionSource.Instance));
            Assert.That(hub.Users, Is.Not.SameAs(UnavailableSuggestionSource.Instance));
        });
    }

    [Test]
    public async Task An_auth_facade_the_head_cannot_construct_reads_as_no_directory_to_search()
    {
        var hub = new ExplorerSuggestions(Throwing());

        var answer = await hub.Users.SuggestAsync("a", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Not the hub's own refusal: the source IS built, over no directory, so the
            // note names the directory rather than suggestions in general.
            Assert.That(hub.Users, Is.Not.SameAs(UnavailableSuggestionSource.Instance));
            Assert.That(answer.UnavailableReason, Is.EqualTo(DirectorySuggestionSource.UnavailableReason));
            Assert.That(answer.UnavailableReason, Is.Not.EqualTo(UnavailableSuggestionSource.Reason));
        });
    }

    [Test]
    public async Task Every_directory_backed_source_survives_a_facade_the_head_cannot_construct()
    {
        var hub = new ExplorerSuggestions(Throwing());

        var answers = await Task.WhenAll(
            new[] { hub.Users, hub.Groups, hub.GroupsOrStoredGroups, hub.Principals }
                .Select(async source => await source.SuggestAsync("a", 5, CancellationToken.None)));

        Assert.That(
            answers.Select(answer => answer.UnavailableReason),
            Is.All.EqualTo(DirectorySuggestionSource.UnavailableReason));
    }

    [Test]
    public void A_source_is_built_once_and_shared_by_every_field_on_every_page()
    {
        var hub = new ExplorerSuggestions(Throwing());

        Assert.Multiple(() =>
        {
            Assert.That(hub.Users, Is.SameAs(hub.Users));
            Assert.That(hub.Groups, Is.SameAs(hub.Groups));
            Assert.That(hub.GroupsOrStoredGroups, Is.SameAs(hub.GroupsOrStoredGroups));
            Assert.That(hub.Principals, Is.SameAs(hub.Principals));
            Assert.That(hub.Groups, Is.Not.SameAs(hub.Users), "a user field and a group field search different things");
        });
    }

    /// <summary>A head whose auth facade is registered but cannot be constructed.</summary>
    private static ServiceProvider Throwing() =>
        new ServiceCollection()
            .AddKeyedSingleton<ILatticeAuthAdmin>(ShellFacades.Key, (_, _) => throw new InvalidOperationException("the endpoint is not configured"))
            .BuildServiceProvider();
}
