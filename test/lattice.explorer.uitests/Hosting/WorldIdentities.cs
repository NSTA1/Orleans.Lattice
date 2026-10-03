namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>The people the test world knows, and the groups they are in.</summary>
internal static class WorldIdentities
{
    /// <summary>The bootstrap administrator: every area the world serves is visible.</summary>
    public const string Admin = "explorer-admin";

    /// <summary>A floor operator who edits the task board.</summary>
    public const string Alice = "alice";

    /// <summary>A task-board viewer who also holds broad data rights of their own on the whole cluster.</summary>
    public const string Bob = "bob";

    /// <summary>A task-board viewer with no rights of their own.</summary>
    public const string Dave = "dave";

    /// <summary>A visitor bound to no task-board role.</summary>
    public const string Carol = "carol";

    /// <summary>
    /// The administrator of tenant globex in the delegated-access world, and nothing
    /// else: not a platform operator, and named by no cluster rule.
    /// </summary>
    public const string GlobexAdmin = "globex-admin";

    /// <summary>The password every identity signs in with; the world never checks it.</summary>
    public const string Password = "explorer";

    /// <summary>The group that may read the demo tree; <see cref="Alice"/> is a member.</summary>
    public const string OperatorsGroup = "operators";

    /// <summary>The group the task board's <c>editor</c> role is bound to.</summary>
    public const string EditorsGroup = "task-editors";

    /// <summary>The group the task board's <c>viewer</c> role is bound to.</summary>
    public const string ViewersGroup = "task-viewers";

    /// <summary>A group bound to no task-board role.</summary>
    public const string VisitorsGroup = "visitors";

    /// <summary>A group on the roster the world does not create, for the New group journey.</summary>
    public const string AuditorsGroup = "auditors";
}
