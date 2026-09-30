namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// The fixed names the sample seeds: its sign-in identities, groups, tenants,
/// regions and trees. The sample's trusted-token authenticator never checks a
/// password, so any password signs in as any of these users.
/// </summary>
internal static class SampleIdentities
{
    /// <summary>The bootstrap administrator and platform operator; the console signs in as it automatically.</summary>
    public const string Administrator = "explorer-admin";

    /// <summary>The password the console's automatic sign-in presents. It is never checked.</summary>
    public const string AdministratorPassword = "explorer";

    /// <summary>The tenant administrator of <see cref="AcmeTenant"/>.</summary>
    public const string AcmeAdmin = "acme-admin";

    /// <summary>The tenant administrator of <see cref="GlobexTenant"/>.</summary>
    public const string GlobexAdmin = "globex-admin";

    /// <summary>A member of the <c>operators</c> and <c>task-editors</c> groups.</summary>
    public const string Alice = "alice";

    /// <summary>A member of the <c>task-viewers</c> group.</summary>
    public const string Bob = "bob";

    /// <summary>A member of the <c>visitors</c> group, bound to no app role.</summary>
    public const string Carol = "carol";

    /// <summary>The group granted Read on <see cref="FactoryFloorTree"/>.</summary>
    public const string OperatorsGroup = "operators";

    /// <summary>The group the task board's <c>editor</c> role is bound to in the walkthrough.</summary>
    public const string TaskEditorsGroup = "task-editors";

    /// <summary>The group the task board's <c>viewer</c> role is bound to in the walkthrough.</summary>
    public const string TaskViewersGroup = "task-viewers";

    /// <summary>A group bound to no app role.</summary>
    public const string VisitorsGroup = "visitors";

    /// <summary>The group the task board's <c>editor</c> role is bound to in <see cref="AcmeTenant"/>.</summary>
    public const string AcmeEditorsGroup = "acme-editors";

    /// <summary>
    /// A group on the static roster that the sample does not create, so Access &gt;
    /// Groups &gt; New group has an id it accepts.
    /// </summary>
    public const string AuditorsGroup = "auditors";

    /// <summary>The first seeded tenant; the task board is installed in it at startup.</summary>
    public const string AcmeTenant = "acme";

    /// <summary>The second seeded tenant; the walkthrough installs the task board in it.</summary>
    public const string GlobexTenant = "globex";

    /// <summary>The primary region: the Explorer connects to it by default.</summary>
    public const string EastRegion = "east";

    /// <summary>The peer region.</summary>
    public const string WestRegion = "west";

    /// <summary>The demo tree in the default tenant, replicated between the regions.</summary>
    public const string FactoryFloorTree = "factory-floor";

    /// <summary>The tenant-local name of the tree each tenant owns.</summary>
    public const string TenantOrdersTree = "orders";

    /// <summary>The number of machines seeded in <see cref="FactoryFloorTree"/>.</summary>
    public const int MachineCount = 12;
}
