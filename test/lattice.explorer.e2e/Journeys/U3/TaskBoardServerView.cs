using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Apps = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Apps;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U3;

/// <summary>
/// What the cluster answers about the task board at the moment its window refused to open: the
/// administrator's view of the install (its live state), and the persona's own workspace
/// answers, through the same gateway and credential the Explorer uses on their behalf. Read
/// side by side, they say whether a refusal is the server's (no grant, not enabled, or a
/// revision the two workspace reads disagree on) or the Explorer's frame gate misreading a
/// correct answer.
/// </summary>
/// <param name="Admin">The administrator's view of the install.</param>
/// <param name="Listed">The persona's <c>ListMyApps</c> entry for the board.</param>
/// <param name="Described">The persona's <c>DescribeMyApp</c> answer for the board.</param>
/// <param name="WorkspaceRefuses">Whether the workspace answers would refuse the launch on their own.</param>
internal sealed record TaskBoardServerView(string Admin, string Listed, string Described, bool WorkspaceRefuses)
{
    /// <summary>Reads the cluster's answers for <paramref name="session"/>'s persona, in its region.</summary>
    /// <param name="estate">The estate.</param>
    /// <param name="session">The persona's session.</param>
    public static async Task<TaskBoardServerView> ReadAsync(JourneyEstate estate, PersonaSession session)
    {
        ArgumentNullException.ThrowIfNull(estate);
        ArgumentNullException.ThrowIfNull(session);

        string admin;
        try
        {
            var app = await ContractFacades.Of(estate, region: session.Region).Apps.DescribeAsync(Apps.TaskBoard);
            admin = app is null ? "not installed" : $"{app.State} (pending {app.PendingState?.ToString() ?? "none"}), version {app.Version}";
        }
        catch (Exception exception)
        {
            admin = "unread: " + exception.GetType().Name + ": " + exception.Message;
        }

        var workspace = LatticeAppWorkspaceApiGrpcClient.Create(estate.Invoker(session.Persona.Id, region: session.Region), JourneyEstate.Serialization);
        WorkspaceAppSummary? summary;
        string listed;
        try
        {
            var mine = await workspace.ListMyAppsAsync();
            summary = mine.IsDefault ? null : mine.FirstOrDefault(candidate => candidate.Slug == Apps.TaskBoard);
            listed = summary is null
                ? $"absent ({(mine.IsDefault ? 0 : mine.Length)} app(s) listed)"
                : $"version {summary.Version}, revision {summary.InstallRevision}, roles [{string.Join(", ", summary.Roles)}], has UI {summary.HasUi}";
        }
        catch (Exception exception)
        {
            return new(admin, "unread: " + exception.GetType().Name + ": " + exception.Message, "not read", WorkspaceRefuses: false);
        }

        try
        {
            var described = await workspace.DescribeMyAppAsync(Apps.TaskBoard);
            var refuses = summary is null
                || described is null
                || described.State != AppLifecycleState.Enabled
                || described.InstallRevision != summary.InstallRevision
                || described.Ui is null;
            var text = described is null
                ? "no grant (null)"
                : $"{described.State}, version {described.Version}, revision {described.InstallRevision}, UI {(described.Ui is null ? "none" : "declared")}";
            return new(admin, listed, text, refuses);
        }
        catch (Exception exception)
        {
            return new(admin, listed, "unread: " + exception.GetType().Name + ": " + exception.Message, WorkspaceRefuses: false);
        }
    }

    /// <inheritdoc />
    public override string ToString() =>
        $"admin describes {Admin}; the persona's ListMyApps: {Listed}; DescribeMyApp: {Described}; the workspace {(WorkspaceRefuses ? "refuses" : "grants")} the launch";
}
