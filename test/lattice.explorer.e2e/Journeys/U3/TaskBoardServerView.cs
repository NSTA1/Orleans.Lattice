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
/// <param name="Listed">The persona's <c>ListMyApps</c> entries for the board, one per sample.</param>
/// <param name="Described">The persona's <c>DescribeMyApp</c> answers for the board, one per sample.</param>
/// <param name="WorkspaceRefuses">Whether any sample of the workspace answers would refuse the launch on its own.</param>
internal sealed record TaskBoardServerView(string Admin, string Listed, string Described, bool WorkspaceRefuses)
{
    /// <summary>How many times the persona's workspace is asked: its answers are known to vary between reads (#5595).</summary>
    private const int Samples = 5;

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
        var listed = new List<string>(Samples);
        var described = new List<string>(Samples);
        var refusals = 0;
        for (var sample = 0; sample < Samples; sample++)
        {
            WorkspaceAppSummary? summary = null;
            try
            {
                var mine = await workspace.ListMyAppsAsync();
                summary = mine.IsDefault ? null : mine.FirstOrDefault(candidate => candidate.Slug == Apps.TaskBoard);
                listed.Add(summary is null
                    ? $"absent of {(mine.IsDefault ? 0 : mine.Length)}"
                    : $"v{summary.Version} r{summary.InstallRevision} [{string.Join(",", summary.Roles)}] ui={summary.HasUi}");
            }
            catch (Exception exception)
            {
                listed.Add("unread " + exception.GetType().Name);
            }

            try
            {
                var answer = await workspace.DescribeMyAppAsync(Apps.TaskBoard);
                if (summary is null
                    || answer is null
                    || answer.State != AppLifecycleState.Enabled
                    || answer.InstallRevision != summary.InstallRevision
                    || answer.Ui is null)
                {
                    refusals++;
                }

                described.Add(answer is null
                    ? "null"
                    : $"{answer.State} v{answer.Version} r{answer.InstallRevision} ui={(answer.Ui is null ? "none" : "yes")}");
            }
            catch (Exception exception)
            {
                described.Add("unread " + exception.GetType().Name);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        return new(admin, string.Join(" | ", listed), string.Join(" | ", described), refusals > 0);
    }

    /// <inheritdoc />
    public override string ToString() =>
        $"admin describes {Admin}; the persona's ListMyApps: {Listed}; DescribeMyApp: {Described}; the workspace {(WorkspaceRefuses ? "refuses the launch in at least one sample" : "grants the launch in every sample")}";
}
