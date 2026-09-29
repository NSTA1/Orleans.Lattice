using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// One staged backup operation (epic decision E15): a named sequence of stages
/// the status page draws, advanced as the work proceeds, so an operation reads
/// the same whether today's contract answers at once or a later one answers in
/// stages. It lives for the circuit, so its status page can be left and resumed.
/// </summary>
/// <remarks>
/// Written from the operation's own flow and read from the renderer, so every
/// member is guarded. <see cref="Changed"/> is raised off the renderer; a
/// subscriber marshals onto it.
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
    private LatticeRestoreResult? _restore;
    private string? _revertedBy;

    /// <summary>Creates a running operation at its first stage.</summary>
    /// <param name="id">The operation's id within the circuit.</param>
    /// <param name="kind">What it does.</param>
    /// <param name="title">Its title, such as "Capture a full backup of orders".</param>
    /// <param name="stages">Its stages, in order; at least one.</param>
    /// <param name="time">The clock its times are read from.</param>
    public BackupOperation(string id, BackupOperationKind kind, string title, IReadOnlyList<string> stages, TimeProvider time)
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

    /// <summary>The id of the operation that reverted this restore, once one has.</summary>
    public string? RevertedBy
    {
        get
        {
            lock (_gate)
            {
                return _revertedBy;
            }
        }
    }

    /// <summary>Whether this is a finished point-in-time restore that has not been reverted.</summary>
    public bool CanRevert
    {
        get
        {
            lock (_gate)
            {
                return _status == BackupOperationStatus.Succeeded
                    && _restore is { Mode: LatticeRestoreMode.ShadowCutover }
                    && _revertedBy is null;
            }
        }
    }

    /// <summary>
    /// The restore's result, kept only to revert it. It carries the physical tree
    /// ids of the cut-over, which are never shown.
    /// </summary>
    internal LatticeRestoreResult? RestoreResult
    {
        get
        {
            lock (_gate)
            {
                return _restore;
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

    /// <summary>Keeps a restore's result so it can be reverted.</summary>
    /// <param name="result">The restore's result.</param>
    internal void KeepRestore(LatticeRestoreResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        lock (_gate)
        {
            _restore = result;
        }
    }

    /// <summary>Marks this restore reverted by <paramref name="operationId"/>.</summary>
    /// <param name="operationId">The reverting operation's id.</param>
    internal void MarkReverted(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        lock (_gate)
        {
            _revertedBy = operationId;
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
