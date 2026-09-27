namespace Orleans.Lattice.Apps.Tests;

/// <summary>Records every delivery; optionally throws or returns a caller-supplied task.</summary>
internal sealed class RecordingChangeFeedHandler : IAppChangeFeedHandler
{
    public List<(AppSubscriptionContext Subscription, LatticeMutation Mutation)> Deliveries { get; } = [];

    public Exception? Throw { get; set; }

    public Func<Task>? Result { get; set; }

    public Task HandleAsync(AppSubscriptionContext subscription, LatticeMutation mutation, CancellationToken cancellationToken)
    {
        Deliveries.Add((subscription, mutation));
        if (Throw is { } ex)
            throw ex;
        return Result?.Invoke() ?? Task.CompletedTask;
    }
}
