using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Builds a stub <see cref="IAccessDomain"/> for the Access plugin's workspace
/// and render tests: a membership service, a policy service, and a tree catalog
/// that all answer from data supplied up front.
/// </summary>
/// <remarks>
/// Substituting at the plugin's own controlled domain contract is the whole of
/// its reach, so a test never stands up a connection, a channel, or a container -
/// and never touches a clock, a network, or a background task.
/// </remarks>
internal static class StubAccessDomain
{
    /// <summary>
    /// Builds a domain over the supplied data.
    /// </summary>
    /// <param name="groups">The groups the single membership page returns.</param>
    /// <param name="trees">The trees the catalog returns.</param>
    /// <param name="rules">The rules the single policy page returns.</param>
    /// <param name="directoryAvailable">Whether the access model reports a searchable directory.</param>
    /// <param name="status">The status every list page reports.</param>
    /// <param name="message">The message a non-success page carries.</param>
    public static IAccessDomain Create(
        IReadOnlyList<AuthGroup>? groups = null,
        IReadOnlyList<string>? trees = null,
        IReadOnlyList<LatticeAuthorizationRule>? rules = null,
        bool directoryAvailable = true,
        AccessOperationStatus status = AccessOperationStatus.Succeeded,
        string? message = null)
    {
        var membership = Substitute.For<IMembershipAdminService>();
        membership
            .ListGroupsAsync(Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new AccessListView<AuthGroup>
            {
                Status = status,
                Message = message ?? string.Empty,
                Entries = groups ?? [],
            });
        membership
            .ListDirectMembersAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new AccessListView<string> { Status = AccessOperationStatus.Succeeded });
        membership.GetAccessModelAsync(Arg.Any<CancellationToken>()).Returns(new AccessModelView
        {
            Status = AccessOperationStatus.Succeeded,
            DirectoryAvailable = directoryAvailable,
            RulesEnforced = true,
            LocalMembershipEffective = true,
        });

        var policy = Substitute.For<IPolicyAdminService>();
        policy
            .ListRulesAsync(Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new AccessListView<LatticeAuthorizationRule>
            {
                Status = status,
                Message = message ?? string.Empty,
                Entries = rules ?? [],
            });

        var catalog = Substitute.For<ICatalogReader>();
        catalog.LoadAsync(CatalogKind.Trees, Arg.Any<string?>(), Arg.Any<int>()).Returns(new CatalogPage
        {
            Items = [.. (trees ?? []).Select(id => new CatalogItem { Id = id, Kind = CatalogKind.Trees })],
        });

        var domain = Substitute.For<IAccessDomain>();
        domain.Membership.Returns(membership);
        domain.Policy.Returns(policy);
        domain.Catalog.Returns(catalog);
        domain.AuthenticationMode.Returns(ExplorerAccessAuthenticationMode.Claims);
        domain.CreateLabelResolver().Returns(_ => new PrincipalLabelResolver(membership));
        domain.CreateSubjectPicker().Returns(_ => new SubjectPickerModel(
            membership,
            Substitute.For<ISubjectSearchDebounce>()));
        return domain;
    }

    /// <summary>A whole-tree allow rule, the simplest thing the rule table can render.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="treeId">The governed tree.</param>
    /// <param name="subjectId">The subject the rule names.</param>
    public static LatticeAuthorizationRule Rule(
        string ruleId,
        string treeId = "orders",
        string subjectId = "alice") =>
        new(
            ruleId,
            LatticeSubjectSelector.User(subjectId),
            LatticeScope.Tree(treeId),
            LatticeOperation.Read,
            LatticeEffect.Allow);
}
