using System.Globalization;
using System.Text.Json;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A team on the task board (U3, #5386), established as priya through the editor role A5
/// bound to her group. Produces three task cards in the task board's tree, the shape the app
/// writes (<c>tasks/{id}</c> holding <c>{title, column, created}</c>), the last one in the
/// <c>done</c> column; their ids are recorded at <see cref="StoryKeys.TaskBoardTasks"/>.
/// </summary>
public sealed class U3Contract() : ChapterContract("U3")
{
    /// <summary>The three cards: id, title and the column each ends in.</summary>
    public static IReadOnlyList<(string Id, string Title, string Column)> Tasks { get; } =
    [
        ("u3-press-maintenance", "Schedule press maintenance", "todo"),
        ("u3-oven-calibration", "Calibrate oven 1", "todo"),
        ("u3-conveyor-audit", "Audit conveyor 1", "done"),
    ];

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var tree = context.Recall(StoryKeys.TaskBoardTree);
        var data = ContractFacades.Of(context, Personas.Priya.Id).Data;
        await ContractCalls.SetManyAsync(data, tree, Tasks.Select(task => new StoryEntry(Key(task.Id), Card(task.Title, task.Column))));
        context.Record(StoryKeys.TaskBoardTasks, (IReadOnlyList<string>)[.. Tasks.Select(task => task.Id)]);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var ids = context.Recall(StoryKeys.TaskBoardTasks);
        RequireEqual(ids.Count, 3, "three tasks are recorded");
        if (context.IsProduced("C3"))
        {
            return;
        }

        var tree = context.Recall(StoryKeys.TaskBoardTree);
        var data = ContractFacades.Of(context).Data;
        var columns = new List<string?>();
        foreach (var id in ids)
        {
            var value = await ContractCalls.GetAsync(data, tree, Key(id));
            Require(value is not null, $"task {id} is in the task board's tree");
            using var card = JsonDocument.Parse(value!);
            columns.Add(card.RootElement.GetProperty("column").GetString());
        }

        Require(columns.Count(column => column == "done") == 1, "one task is Done");
    }

    /// <summary>A task's key.</summary>
    /// <param name="id">The task's id.</param>
    public static string Key(string id) => "tasks/" + id;

    private static string Card(string title, string column) =>
        string.Create(CultureInfo.InvariantCulture, $"{{\"title\":\"{title}\",\"column\":\"{column}\",\"created\":\"2026-10-31T09:00:00.000Z\"}}");
}
