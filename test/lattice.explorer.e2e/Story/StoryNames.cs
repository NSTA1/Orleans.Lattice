namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// Every fixed name the story uses, taken from the epic's decision record (#5401, "Personas and
/// fixed names") and the journey contracts (#5371-#5399): trees, groups, rules, aliases, views,
/// tag indexes, backups, tenants, apps, keys and files. Journeys and contracts read these and
/// never redeclare a name, so two parallel items cannot disagree about one. Persona user ids
/// are the persona catalogue's (#5360), not this file's.
/// </summary>
public static class StoryNames
{
    /// <summary>The two storylines, one per estate (decision D2).</summary>
    public static class Storylines
    {
        /// <summary>Contoso: the non-tenanted manufacturing estate that carries the main story.</summary>
        public const string Contoso = "contoso";

        /// <summary>Fabrikam Cloud: the tenanted hosting provider that carries the tenancy storyline.</summary>
        public const string Fabrikam = "fabrikam";
    }

    /// <summary>The Contoso estate's names.</summary>
    public static class Contoso
    {
        /// <summary>Groups.</summary>
        public static class Groups
        {
            /// <summary>The data stewards (priya, from A2 until C3).</summary>
            public const string DataStewards = "data-stewards";

            /// <summary>The schema owners (sam).</summary>
            public const string SchemaOwners = "schema-owners";

            /// <summary>The auditors (dana).</summary>
            public const string Auditors = "auditors";

            /// <summary>The ingestion services (the line feed).</summary>
            public const string IngestionServices = "ingestion-services";

            /// <summary>The nested group A9 creates, holding <see cref="Auditors"/>.</summary>
            public const string FactoryStaff = "factory-staff";

            /// <summary>How many directory-synced team groups A2 consumes.</summary>
            public const int SyncedTeamCount = 230;

            /// <summary>The <paramref name="number"/>th directory-synced team group, <c>org-team-001</c> to <c>org-team-230</c>.</summary>
            /// <param name="number">From 1 to <see cref="SyncedTeamCount"/>.</param>
            public static string SyncedTeam(int number) => $"org-team-{number:D3}";
        }

        /// <summary>Access rules.</summary>
        public static class Rules
        {
            /// <summary>The data stewards' rule on the factory trees (A2).</summary>
            public const string StewardsFactory = "stewards-factory";

            /// <summary>The schema owners' rule on the factory trees (A2).</summary>
            public const string SchemaOwnersFactory = "schema-owners-factory";

            /// <summary>The auditors' read rule on the factory trees (A2).</summary>
            public const string AuditorsFactoryRead = "auditors-factory-read";

            /// <summary>The line feed's write rule (A2).</summary>
            public const string LineFeedFactory = "line-feed-factory";

            /// <summary>The nested group's telemetry read rule (A9).</summary>
            public const string FactoryStaffTelemetryRead = "factory-staff-telemetry-read";

            /// <summary>How many directory-synced rules A9 consumes.</summary>
            public const int SyncedRuleCount = 230;
        }

        /// <summary>Trees.</summary>
        public static class Trees
        {
            /// <summary>The orders tree: 4 shards, 240 seeded orders (A3).</summary>
            public const string FactoryOrders = "factory-orders";

            /// <summary>The line feed's telemetry tree.</summary>
            public const string FactoryTelemetry = "factory-telemetry";

            /// <summary>The production-line register: 12 JSON lines (A3).</summary>
            public const string FactoryLines = "factory-lines";

            /// <summary>The October archive, seeded from CSV (U1).</summary>
            public const string FactoryOrdersArchive = "factory-orders-archive-2026-10";

            /// <summary>The orders snapshot (A8).</summary>
            public const string FactoryOrdersSnapshot = "factory-orders-snapshot";

            /// <summary>The point-in-time restore target (A10).</summary>
            public const string FactoryLinesRestored = "factory-lines-restored";

            /// <summary>The cold restore target (A10).</summary>
            public const string FactoryLinesCold = "factory-lines-cold";

            /// <summary>The online snapshot of the telemetry tree (A12).</summary>
            public const string TelemetrySnapOnline = "telemetry-snap-online";

            /// <summary>The offline snapshot of the lines tree (A12).</summary>
            public const string LinesSnapOffline = "lines-snap-offline";

            /// <summary>The tree created with explicit sizing (A12).</summary>
            public const string FactorySized = "factory-sized";

            /// <summary>The tree an unsorted seed is refused into, then deleted (A3).</summary>
            public const string FactoryScratch = "factory-scratch";

            /// <summary>The tree a cancelled 500,000-line import starts, then deleted (U1).</summary>
            public const string FactoryHistory = "factory-history";
        }

        /// <summary>Aliases.</summary>
        public static class Aliases
        {
            /// <summary>The stable alias of <see cref="Trees.FactoryOrders"/> (A3).</summary>
            public const string Orders = "orders";
        }

        /// <summary>Materialised views.</summary>
        public static class Views
        {
            /// <summary>The stewards' status view over <see cref="Trees.FactoryOrders"/> (A3).</summary>
            public const string OrdersByStatus = "orders-by-status";
        }

        /// <summary>Tag indexes.</summary>
        public static class TagIndexes
        {
            /// <summary>The order-status index the line feed owns.</summary>
            public const string OrderStatus = "order-status";
        }

        /// <summary>Backups and backup sets.</summary>
        public static class Backups
        {
            /// <summary>The monitored full baseline of the orders (A6).</summary>
            public const string OrdersBaseline = "orders-baseline";

            /// <summary>The prefix capture of the orders (A10).</summary>
            public const string OrdersPrefix0001 = "orders-prefix-0001";

            /// <summary>The one-key capture of the orders (A10).</summary>
            public const string OrdersKey42 = "orders-key-42";

            /// <summary>The full capture of the lines (A10).</summary>
            public const string LinesFull = "lines-full";

            /// <summary>The incremental on <see cref="LinesFull"/> (A10).</summary>
            public const string LinesInc1 = "lines-inc-1";

            /// <summary>The set of orders and lines at one cross-tree fence (A10).</summary>
            public const string FactorySet1 = "factory-set-1";

            /// <summary>How many historical scheduled backups of the telemetry A10 consumes.</summary>
            public const int HistoricalCount = 60;
        }

        /// <summary>The task board app (A5, U3).</summary>
        public static class Apps
        {
            /// <summary>The app's slug.</summary>
            public const string TaskBoard = "task-board";

            /// <summary>The source holding version 1.</summary>
            public const string SamplesSource = "samples";

            /// <summary>The source holding vNext.</summary>
            public const string SamplesNextSource = "samples-next";

            /// <summary>Version 1.</summary>
            public const string TaskBoardVersion = "1.0.0";

            /// <summary>vNext: adds the editor role's RangeDelete and declares its tree replicated.</summary>
            public const string TaskBoardNextVersion = "1.1.0";

            /// <summary>The editor role, bound to <see cref="Groups.DataStewards"/>.</summary>
            public const string EditorRole = "editor";

            /// <summary>The viewer role, bound to <see cref="Groups.Auditors"/> and <see cref="Groups.SchemaOwners"/>.</summary>
            public const string ViewerRole = "viewer";
        }

        /// <summary>The orders' keys and values.</summary>
        public static class Orders
        {
            /// <summary>How many orders A3 seeds.</summary>
            public const int SeededCount = 240;

            /// <summary>The first order number the line feed writes.</summary>
            public const int LineFeedFirstOrder = 100001;

            /// <summary>An order with a negative total, which U1 makes valid.</summary>
            public const string Order17 = "order/000017";

            /// <summary>An order with a negative total, which U1 diverts with an invalid status.</summary>
            public const string Order101 = "order/000101";

            /// <summary>An order with a negative total, which U2's compliance scan lists.</summary>
            public const string Order233 = "order/000233";

            /// <summary>The order U1 edits while the line feed transitions it.</summary>
            public const string Order50 = "order/000050";

            /// <summary>The order A10 backs up alone.</summary>
            public const string Order42 = "order/000042";

            /// <summary>The entry U1 creates and deletes.</summary>
            public const string Scratch = "order/scratch";

            /// <summary>A10's prefix capture.</summary>
            public const string BackupPrefix = "order/00001";

            /// <summary>U1's export prefix.</summary>
            public const string ExportPrefix = "order/0001";

            /// <summary>U1's History live-tail prefix.</summary>
            public const string LiveTailPrefix = "order/0002";

            /// <summary>The order statuses the policy allows (A4).</summary>
            public static IReadOnlyList<string> Statuses { get; } = ["open", "shipped", "cancelled"];

            /// <summary>The currency v2 adds, and its default (A4).</summary>
            public const string DefaultCurrency = "EUR";

            /// <summary>The seeded orders with negative totals.</summary>
            public static IReadOnlyList<string> NegativeTotals { get; } = [Order17, Order101, Order233];

            /// <summary>The key of order <paramref name="number"/>: <c>order/000017</c>.</summary>
            /// <param name="number">The order number.</param>
            public static string Key(int number) => $"order/{number:D6}";
        }

        /// <summary>The production-line register.</summary>
        public static class Lines
        {
            /// <summary>How many lines A3 pastes.</summary>
            public const int Count = 12;
        }

        /// <summary>The files personas import and export.</summary>
        public static class Files
        {
            /// <summary>The October orders, NDJSON, 240 lines (A3), key template <c>order/{/id}</c>.</summary>
            public const string OrdersNdjson = "orders-2026-10.ndjson";

            /// <summary>The October archive, CSV (U1), key template <c>order/{id}</c>.</summary>
            public const string ArchiveCsv = "archive-2026-10.csv";

            /// <summary>The archive's rows.</summary>
            public const int ArchiveRows = 5000;

            /// <summary>Finance's correction file, NDJSON (U1, re-imported by C1).</summary>
            public const string CorrectionsNdjson = "corrections.ndjson";

            /// <summary>The corrections' lines.</summary>
            public const int CorrectionLines = 50;

            /// <summary>The corrections that apply.</summary>
            public const int CorrectionsApplied = 48;

            /// <summary>The corrections schema refuses.</summary>
            public const int CorrectionsRefused = 2;

            /// <summary>The import U1 cancels.</summary>
            public const string History2025Ndjson = "history-2025.ndjson";

            /// <summary>The cancelled import's lines.</summary>
            public const int History2025Lines = 500_000;
        }

        /// <summary>Hosting names (C4).</summary>
        public static class Hosting
        {
            /// <summary>The east console's base path.</summary>
            public const string ConsoleBasePath = "/console";

            /// <summary>The kiosk head, seeded from the environment.</summary>
            public const string Kiosk = "contoso-kiosk";
        }
    }

    /// <summary>The Fabrikam estate's names.</summary>
    public static class Fabrikam
    {
        /// <summary>Tenants.</summary>
        public static class Tenants
        {
            /// <summary>Acme.</summary>
            public const string Acme = "acme";

            /// <summary>Globex.</summary>
            public const string Globex = "globex";

            /// <summary>The reserved default tenant.</summary>
            public const string Default = "default";
        }

        /// <summary>Tenant groups.</summary>
        public static class Groups
        {
            /// <summary>Acme's analysts (T2).</summary>
            public const string AcmeAnalysts = "acme-analysts";

            /// <summary>Globex's buyers.</summary>
            public const string GlobexBuyers = "globex-buyers";
        }

        /// <summary>Trees.</summary>
        public static class Trees
        {
            /// <summary>Acme's inventory, as acme names it (T2).</summary>
            public const string AcmeInventory = "acme-inventory";

            /// <summary>Acme's inventory as the platform names it.</summary>
            public const string AcmeInventoryQualified = "t/" + Tenants.Acme + "/" + AcmeInventory;
        }

        /// <summary>Platform rules.</summary>
        public static class Rules
        {
            /// <summary>Acme's admins' platform rule on <c>t/acme/*</c> (T1).</summary>
            public const string AcmeAdminsPlatform = "acme-admins-platform";

            /// <summary>Globex's admins' platform rule on <c>t/globex/*</c> (T1).</summary>
            public const string GlobexAdminsPlatform = "globex-admins-platform";

            /// <summary>The default-tenant user's read rule (T1).</summary>
            public const string DeeDefaultRead = "dee-default-read";

            /// <summary>The cross-tenant reader's rule on <see cref="Trees.AcmeInventoryQualified"/> (T3).</summary>
            public const string GiaReadAcmeInventory = "gia-read-acme-inventory";
        }

        /// <summary>Backups.</summary>
        public static class Backups
        {
            /// <summary>Acme's inventory backup (T7).</summary>
            public const string AcmeInv1 = "acme-inv-1";
        }

        /// <summary>Files.</summary>
        public static class Files
        {
            /// <summary>Acme's inventory import (T2).</summary>
            public const string InventoryCsv = "inventory.csv";
        }
    }
}
