namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// One staged backup operation the circuit runs (epic decision E15): a named
/// sequence of stages the status page draws, advanced as the work proceeds. It
/// lives for the circuit, so its status page can be left and resumed.
/// </summary>
/// <remarks>
/// A capture or restore uses one only to check access and start the work on the
/// cluster (#4122): once the cluster accepts it, the operation is handed off to
/// the cluster's tracked operation (<see cref="ClusterOperationId"/>), whose status
/// outlives the circuit, and the status page follows that instead. Revert and
/// catalogue maintenance run here end to end.
/// <para>
/// Written from the operation's own flow and read from the renderer, so every
/// member is guarded. <see cref="Changed"/> is raised off the renderer; a
/// subscriber marshals onto it.
/// </para>
/// </remarks>
internal sealed class BackupOperation
{
    private readonly object _gate = new();
    private readonly TaskCompletionSource _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TimeProvider _time;
    private int _stage;
    private BackupOperationStatus _status = BackupOperationStatus.Running;
    private string? _message;
    private DateTimeOffset? _completedAt;
    private IReadOnlyList<BackupOperationLink> _links = [];
    private IReadOnlyList<KeyValuePair<string, string>> _facts = [];
    private IReadOnlyList<string> _items = [];
    private string? _clusterOperationId;

    /// <summary>Creates a running operation at its first stage.</summary>
    /// <param name="id">The operation's id within the circuit.</param>
    /// <param name="kind">What it does.</param>
    /// <param name="title">Its title, such as "Capture a full backup of orders".</param>
    /// <param name="stages">Its stages, in order; at least one.</param>
    /// <param name="time">The clock its times are read from.</param>
    /// <param name="reverts">For a revert, the cluster operation id of the restore it reverts.</param>
    public BackupOperation(string id, BackupOperationKind kind, string title, IReadOnlyList<string> stages, TimeProvider time, string? reverts = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(title);
        ArgumentNullException.ThrowIfNull(stages);
        ArgumentNullException.ThrowIfNull(time);
        if (stages.Count == 0)
        {
            throw new ArgumentException("An operation needs at least one stage.", nameof(stages));
        }

        Id = id;
        Kind = kind;
        Title = title;
        Stages = stages;
        _time = time;
        Reverts = reverts;
        StartedAt = time.GetUtcNow();
    }

    /// <summary>Raised whenever the operation moves; raised off the renderer.</summary>
    public event Action? Changed;

    /// <summary>The operation's id within the circuit.</summary>
    public string Id { get; }

    /// <summary>What it does.</summary>
    public BackupOperationKind Kind { get; }

    /// <summary>Its title.</summary>
    public string Title { get; }

    /// <summary>Its stages, in order.</summary>
    public IReadOnlyList<string> Stages { get; }

    /// <summary>When it started.</summary>
    public DateTimeOffset StartedAt { get; }

    /// <summary>Completes once the operation has finished, however it finished.</summary>
    public Task Completion => _completed.Task;

    /// <summary>The index of the stage under way, or of the stage it stopped at.</summary>
    public int CurrentStage
    {
        get
        {
            lock (_gate)
            {
                return _stage;
            }
        }
    }

    /// <summary>Where it is.</summary>
    public BackupOperationStatus Status
    {
        get
        {
            lock (_gate)
            {
                return _status;
            }
        }
    }

    /// <summary>The outcome in one sentence once it has finished; <see langword="null"/> while it runs.</summary>
    public string? Message
    {
        get
        {
            lock (_gate)
            {
                return _message;
            }
        }
    }

    /// <summary>When it finished.</summary>
    public DateTimeOffset? CompletedAt
    {
        get
        {
            lock (_gate)
            {
                return _completedAt;
            }
        }
    }

    /// <summary>What it produced, as links.</summary>
    public IReadOnlyList<BackupOperationLink> Links
    {
        get
        {
            lock (_gate)
            {
                return _links;
            }
        }
    }

    /// <summary>Its figures, such as how many entries a restore applied.</summary>
    public IReadOnlyList<KeyValuePair<string, string>> Facts
    {
        get
        {
            lock (_gate)
            {
                return _facts;
            }
        }
    }

    /// <summary>Identifiers it reported, such as the orphan backup ids a scrub found.</summary>
    public IReadOnlyList<string> Items
    {
        get
        {
            lock (_gate)
            {
                return _items;
            }
        }
    }

    /// <summary>For a revert, the cluster operation id of the restore it reverts; otherwise <see langword="null"/>.</summary>
    public string? Reverts { get; }

    /// <summary>
    /// The id of the cluster's tracked operation this one started and handed off
    /// to, or <see langword="null"/> while it has not (or never will).
    /// </summary>
    public string? ClusterOperationId
    {
        get
        {
            lock (_gate)
            {
                return _clusterOperationId;
            }
        }
    }
    /// <summary>Moves to the stage at <paramref name="stage"/>.</summary>
    /// <param name="stage">The stage index.</param>
    public void Advance(int stage)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(stage);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(stage, Stages.Count);
        lock (_gate)
        {
            if (_status != BackupOperationStatus.Running)
            {
                return;
            }

            _stage = stage;
        }

        Changed?.Invoke();
    }

    /// <summary>Records what the operation produced, before it finishes.</summary>
    /// <param name="links">Links to what it produced.</param>
    /// <param name="facts">Its figures.</param>
    /// <param name="items">Identifiers it reported.</param>
    public void Report(
        IReadOnlyList<BackupOperationLink>? links = null,
        IReadOnlyList<KeyValuePair<string, string>>? facts = null,
        IReadOnlyList<string>? items = null)
    {
        lock (_gate)
        {
            _links = links ?? _links;
            _facts = facts ?? _facts;
            _items = items ?? _items;
        }

        Changed?.Invoke();
    }

    /// <summary>
    /// Records that the cluster accepted the work as tracked operation
    /// <paramref name="clusterOperationId"/>. The status page follows the cluster's
    /// operation from here on.
    /// </summary>
    /// <param name="clusterOperationId">The cluster's operation id.</param>
    public void HandOff(string clusterOperationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(clusterOperationId);
        lock (_gate)
        {
            _clusterOperationId = clusterOperationId;
        }

        Changed?.Invoke();
    }
    /// <summary>Finishes successfully at the last stage.</summary>
    /// <param name="message">The outcome in one sentence.</param>
    public void Succeed(string message) => Finish(BackupOperationStatus.Succeeded, message, Stages.Count - 1);

    /// <summary>Finishes with a failure at the current stage.</summary>
    /// <param name="message">What went wrong, in one sentence.</param>
    public void Fail(string message) => Finish(BackupOperationStatus.Failed, message, null);

    /// <summary>Finishes because the circuit ended.</summary>
    public void Cancel() => Finish(BackupOperationStatus.Cancelled, "The operation stopped because the session ended.", null);

    private void Finish(BackupOperationStatus status, string message, int? stage)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        lock (_gate)
        {
            if (_status != BackupOperationStatus.Running)
            {
                return;
            }

            _status = status;
            _message = message;
            _completedAt = _time.GetUtcNow();
            if (stage is { } last)
            {
                _stage = last;
            }
        }

        Changed?.Invoke();
        _completed.TrySetResult();
    }
}
