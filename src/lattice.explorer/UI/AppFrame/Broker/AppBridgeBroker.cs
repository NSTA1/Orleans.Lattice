using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Framing.Broker;

/// <summary>
/// Relays an app frame's bridge requests to <see cref="ILatticeAppBridge"/> (epic #3807, E5).
/// It is transport only: the cluster enforces, and the broker validates envelopes, bounds
/// size, rate and concurrency per frame, and refuses early what cannot succeed.
/// </summary>
/// <remarks>
/// <para>
/// Registered <b>scoped</b>, one per circuit, and it captures only that circuit's services:
/// the credential-aware bridge and workspace transport, the circuit's toast queue, and its
/// host context. Nothing a singleton holds reaches it except the clock.
/// </para>
/// <para>
/// Every request fails closed at each step, in order: (1) the envelope is well formed and
/// within the protocol's size limits; (2) the operation is in the bridge vocabulary and in
/// the install's consented bridge set, over a declared logical tree for a data operation;
/// (3) the frame's token bucket and concurrency limit admit it; (4) it is relayed with an
/// <see cref="AppBridgeTarget"/> built from the launch's <c>(slug, install revision)</c>
/// and the frame's logical tree name. The broker never accepts or forwards a physical tree
/// id, and never sends one to the frame. A message with no usable id is dropped; every
/// other refusal is answered with a sanitised error. Denials are logged structurally, with
/// no keys or values.
/// </para>
/// </remarks>
internal sealed partial class AppBridgeBroker(
    ILatticeAppBridge? bridge,
    AppFrameBundleLoader loader,
    IAppFrameHostContext? hostContext,
    LtToastService? toasts,
    TimeProvider time,
    ILogger<AppBridgeBroker> logger,
    ILatticeActiveTenantProvider? activeTenant = null)
{

    private const int Action = 1 << 0;
    private const int Tree = 1 << 1;
    private const int Key = 1 << 2;
    private const int Prefix = 1 << 3;
    private const int PageSize = 1 << 4;
    private const int Continuation = 1 << 5;
    private const int Value = 1 << 6;
    private const int Path = 1 << 7;
    private const int Text = 1 << 8;

    /// <summary>The largest base64 text that can decode to at most <see cref="AppFrameProtocol.MaxValueBytes"/>.</summary>
    private const int MaxValueBase64Length = Orleans.Lattice.Explorer.AppKit.AppKitProtocol.Limits.MaxValueBase64Length;

    private static readonly JsonDocumentOptions ParseOptions = new()
    {
        MaxDepth = 4,
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
    };

    /// <summary>Opens a session for one frame of an authorised launch.</summary>
    /// <param name="launch">A launch authorised by this circuit's loader.</param>
    /// <returns>The session.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="launch"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="launch"/> was authorised by another circuit.</exception>
    public AppBridgeSession Open(AppFrameLaunch launch)
    {
        ArgumentNullException.ThrowIfNull(launch);
        if (!ReferenceEquals(launch.Issuer, loader))
        {
            throw new InvalidOperationException("A bridge session can only be opened for a launch this circuit authorised.");
        }

        return new AppBridgeSession(this, launch, new AppBridgeRateLimiter(time));
    }

    /// <summary>Handles one message the frame posted over its port.</summary>
    /// <param name="session">The frame's session.</param>
    /// <param name="message">The message's JSON text, as the host module relayed it.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The reply to post and any effect for the host.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="session"/> is <see langword="null"/>.</exception>
    public async Task<AppBridgeOutcome> HandleAsync(AppBridgeSession session, string? message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        var slug = session.Launch.Slug;
        if (!ReferenceEquals(session.Owner, this) || session.IsClosed)
        {
            return Drop(slug, null, AppBridgeDenial.Closed);
        }

        if (string.IsNullOrEmpty(message))
        {
            return Drop(slug, null, AppBridgeDenial.Malformed);
        }

        // A UTF-8 encoding is never shorter than the UTF-16 length, so the first test
        // refuses most oversize messages without counting.
        if (message.Length > AppFrameProtocol.MaxRequestBytes
            || Encoding.UTF8.GetByteCount(message) > AppFrameProtocol.MaxRequestBytes)
        {
            return Drop(slug, null, AppBridgeDenial.Oversize);
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(message, ParseOptions);
        }
        catch (JsonException)
        {
            return Drop(slug, null, AppBridgeDenial.Malformed);
        }

        using (document)
        {
            Envelope envelope;
            Args args;
            try
            {
                envelope = ReadEnvelope(document.RootElement);
                args = ReadArgs(envelope.Args);
            }
            catch (InvalidOperationException)
            {
                // An escape that does not decode to well-formed UTF-16 (a lone surrogate).
                return Drop(slug, null, AppBridgeDenial.Malformed);
            }

            if (envelope.Id == 0)
            {
                return Drop(slug, null, AppBridgeDenial.Malformed);
            }

            var id = envelope.Id;
            if (!envelope.WellFormed)
            {
                return Refuse(id, slug, null, AppBridgeDenial.Malformed, AppFrameProtocol.ErrorInvalid);
            }

            if (envelope.Operation is not { } op || !AppFrameProtocol.Operations.Contains(op))
            {
                return Refuse(id, slug, null, AppBridgeDenial.UnknownOperation, AppFrameProtocol.ErrorDenied);
            }

            if (args.Unknown)
            {
                return Refuse(id, slug, op, AppBridgeDenial.UnknownArgument, AppFrameProtocol.ErrorDenied);
            }

            if (args.Invalid || !HasExactShape(op, args))
            {
                return Refuse(id, slug, op, AppBridgeDenial.InvalidArguments, AppFrameProtocol.ErrorInvalid);
            }

            string? tree = null;
            if (AppFrameProtocol.IsDataOperation(op))
            {
                tree = args.Tree!;
                if (!AppFrameProtocol.IsLogicalTreeName(tree))
                {
                    return Refuse(id, slug, op, AppBridgeDenial.PhysicalTree, AppFrameProtocol.ErrorDenied);
                }

                if (!session.Launch.Trees.Contains(tree))
                {
                    return Refuse(id, slug, op, AppBridgeDenial.UndeclaredTree, AppFrameProtocol.ErrorDenied);
                }
            }

            if (!session.Launch.IsGranted(op, tree))
            {
                return Refuse(id, slug, op, AppBridgeDenial.Unconsented, AppFrameProtocol.ErrorDenied);
            }

            var sizeFailure = CheckSizes(args);
            if (sizeFailure is not null)
            {
                return Refuse(id, slug, op, AppBridgeDenial.InvalidArguments, sizeFailure);
            }

            if (!session.Limiter.TryEnter(out var concurrencyLimited))
            {
                return Refuse(
                    id,
                    slug,
                    op,
                    concurrencyLimited ? AppBridgeDenial.ConcurrencyLimited : AppBridgeDenial.RateLimited,
                    AppFrameProtocol.ErrorRateLimited);
            }

            try
            {
                return op switch
                {
                    AppFrameProtocol.ContextRead => ContextRead(id, session.Launch),
                    AppFrameProtocol.ContextUser => ContextUser(id, slug),
                    AppFrameProtocol.NavSync => new AppBridgeOutcome(WriteEmptyOk(id), AppBridgeEffect.NavSync, args.Path),
                    AppFrameProtocol.UiNotify => Notify(id, session.Launch, args.Text!),
                    AppFrameProtocol.DataRead or AppFrameProtocol.DataWrite or AppFrameProtocol.DataDelete =>
                        await RelayAsync(id, session, op, tree!, args, cancellationToken).ConfigureAwait(false),
                    _ => Refuse(id, slug, op, AppBridgeDenial.UnknownOperation, AppFrameProtocol.ErrorDenied),
                };
            }
            finally
            {
                session.Limiter.Exit();
            }
        }
    }

    private async Task<AppBridgeOutcome> RelayAsync(
        long id,
        AppBridgeSession session,
        string op,
        string tree,
        Args args,
        CancellationToken cancellationToken)
    {
        var launch = session.Launch;
        if (bridge is null)
        {
            return Refuse(id, launch.Slug, op, AppBridgeDenial.CollaboratorMissing, AppFrameProtocol.ErrorUnavailable);
        }

        // The bridge call asserts the circuit's tenant as it starts, so it must be the
        // tenant the app was launched in. Once the circuit asserts any other, the frame
        // is closed rather than let it read or write another tenant's data.
        if (!string.Equals(launch.Tenant, activeTenant?.AssertedTenant, StringComparison.Ordinal))
        {
            session.Close();
            LogDenied(logger, launch.Slug, op, AppBridgeDenial.TenantChanged);
            return new AppBridgeOutcome(WriteError(id, AppFrameProtocol.ErrorUnavailable), AppBridgeEffect.Revoked, AppFrameProtocol.RevokedClosed);
        }

        var target = new AppBridgeTarget
        {
            AppSlug = launch.Slug,
            InstallRevision = launch.InstallRevision,
            LogicalTree = tree,
        };

        try
        {
            string reply;
            switch (args.Action)
            {
                case AppFrameProtocol.ActionGet:
                    var value = await bridge.GetAsync(target, args.Key!, cancellationToken).ConfigureAwait(false);
                    if (value is not null && value.Value.Length > AppFrameProtocol.MaxValueBytes)
                    {
                        return Refuse(id, launch.Slug, op, AppBridgeDenial.BridgeRefused, AppFrameProtocol.ErrorTooLarge);
                    }

                    reply = WriteOk(id, value, static (writer, found) =>
                    {
                        writer.WriteBoolean("found", found is not null);
                        if (found is null)
                        {
                            writer.WriteNull("value");
                        }
                        else
                        {
                            writer.WriteBase64String("value", found.Value.Span);
                        }
                    });
                    break;

                case AppFrameProtocol.ActionScan:
                    var page = await bridge.ScanAsync(
                        target,
                        args.Prefix!,
                        args.PageSize == 0 ? AppFrameProtocol.DefaultPageSize : args.PageSize,
                        args.Continuation,
                        cancellationToken).ConfigureAwait(false);
                    if (page is null)
                    {
                        return Refuse(id, launch.Slug, op, AppBridgeDenial.BridgeRefused, AppFrameProtocol.ErrorUnavailable);
                    }

                    reply = WriteOk(id, page, static (writer, scanned) =>
                    {
                        writer.WriteStartArray("entries");
                        if (!scanned.Entries.IsDefault)
                        {
                            foreach (var entry in scanned.Entries)
                            {
                                if (entry is null)
                                {
                                    continue;
                                }

                                writer.WriteStartObject();
                                writer.WriteString("key", entry.Key);
                                writer.WriteBase64String("value", entry.Value.Span);
                                writer.WriteEndObject();
                            }
                        }

                        writer.WriteEndArray();
                        writer.WriteString("continuation", scanned.Continuation);
                    });
                    break;

                case AppFrameProtocol.ActionSet:
                    var decoded = DecodeValue(args.Value!, out var tooLarge);
                    if (decoded is null)
                    {
                        return Refuse(
                            id,
                            launch.Slug,
                            op,
                            AppBridgeDenial.InvalidArguments,
                            tooLarge ? AppFrameProtocol.ErrorTooLarge : AppFrameProtocol.ErrorInvalid);
                    }

                    await bridge.SetAsync(target, args.Key!, decoded.Value, cancellationToken).ConfigureAwait(false);
                    reply = WriteEmptyOk(id);
                    break;

                case AppFrameProtocol.ActionDelete:
                    var deleted = await bridge.DeleteAsync(target, args.Key!, cancellationToken).ConfigureAwait(false);
                    reply = WriteOk(id, deleted, static (writer, removed) => writer.WriteBoolean("deleted", removed));
                    break;

                default:
                    return Refuse(id, launch.Slug, op, AppBridgeDenial.InvalidArguments, AppFrameProtocol.ErrorDenied);
            }

            if (Encoding.UTF8.GetByteCount(reply) > AppFrameProtocol.MaxResponseBytes)
            {
                return Refuse(id, launch.Slug, op, AppBridgeDenial.BridgeRefused, AppFrameProtocol.ErrorTooLarge);
            }

            return new AppBridgeOutcome(reply);
        }
        catch (AppBridgeException exception)
        {
            var code = MapFailure(exception.Failure);
            LogDenied(logger, launch.Slug, op, AppBridgeDenial.BridgeRefused);
            if (exception.Failure is AppBridgeFailure.NotFound or AppBridgeFailure.Denied)
            {
                var reason = await loader.GetRevocationAsync(launch, cancellationToken).ConfigureAwait(false);
                if (reason is not null)
                {
                    session.Close();
                    LogDenied(logger, launch.Slug, op, AppBridgeDenial.Revoked);
                    return new AppBridgeOutcome(WriteError(id, AppFrameProtocol.ErrorUnavailable), AppBridgeEffect.Revoked, reason);
                }
            }

            return new AppBridgeOutcome(WriteError(id, code));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            LogRelayFailed(logger, launch.Slug, op, exception.GetType().Name);
            return new AppBridgeOutcome(WriteError(id, AppFrameProtocol.ErrorUnavailable));
        }
    }

    private AppBridgeOutcome ContextRead(long id, AppFrameLaunch launch)
    {
        var appearance = (hostContext?.Appearance ?? AppFrameAppearance.Default).Sanitise();
        var state = (launch, appearance, tenant: hostContext?.TenantDisplayName);
        return new AppBridgeOutcome(WriteOk(id, state, static (writer, context) =>
        {
            writer.WriteString("slug", context.launch.Slug);
            writer.WriteString("version", context.launch.Version);
            writer.WriteNumber("protocol", AppFrameProtocol.Version);
            writer.WriteString("theme", context.appearance.Theme);
            writer.WriteString("contrast", context.appearance.Contrast);
            writer.WriteString("density", context.appearance.Density);
            writer.WriteBoolean("reducedMotion", context.appearance.ReducedMotion);
            writer.WriteString("tenant", string.IsNullOrWhiteSpace(context.tenant) ? null : context.tenant);
            writer.WriteStartArray("roles");
            foreach (var role in context.launch.Roles)
            {
                writer.WriteStringValue(role);
            }

            writer.WriteEndArray();
        }));
    }

    private AppBridgeOutcome ContextUser(long id, string slug)
    {
        var displayName = hostContext?.UserDisplayName;
        if (string.IsNullOrWhiteSpace(displayName))
        {
            return Refuse(id, slug, AppFrameProtocol.ContextUser, AppBridgeDenial.CollaboratorMissing, AppFrameProtocol.ErrorUnavailable);
        }

        return new AppBridgeOutcome(WriteOk(id, displayName, static (writer, name) => writer.WriteString("displayName", name)));
    }

    private AppBridgeOutcome Notify(long id, AppFrameLaunch launch, string text)
    {
        if (toasts is null)
        {
            return Refuse(id, launch.Slug, AppFrameProtocol.UiNotify, AppBridgeDenial.CollaboratorMissing, AppFrameProtocol.ErrorUnavailable);
        }

        // The toast region renders text, never markup; the prefix names who is speaking.
        toasts.Show(launch.DisplayName + ": " + text, LtToastTone.Info);
        return new AppBridgeOutcome(WriteEmptyOk(id));
    }

    private AppBridgeOutcome Drop(string slug, string? op, AppBridgeDenial denial)
    {
        LogDenied(logger, slug, op ?? "unknown", denial);
        return AppBridgeOutcome.Dropped;
    }

    private AppBridgeOutcome Refuse(long id, string slug, string? op, AppBridgeDenial denial, string code)
    {
        LogDenied(logger, slug, op ?? "unknown", denial);
        return new AppBridgeOutcome(WriteError(id, code));
    }

    /// <summary>Maps the cluster's closed failure set onto the protocol's; anything unrecognised is a denial.</summary>
    /// <param name="failure">The cluster failure.</param>
    /// <returns>The protocol error code.</returns>
    internal static string MapFailure(AppBridgeFailure failure) => failure switch
    {
        AppBridgeFailure.NotFound => AppFrameProtocol.ErrorNotFound,
        AppBridgeFailure.Invalid => AppFrameProtocol.ErrorInvalid,
        AppBridgeFailure.TooLarge => AppFrameProtocol.ErrorTooLarge,
        AppBridgeFailure.Conflict => AppFrameProtocol.ErrorConflict,
        AppBridgeFailure.Unavailable => AppFrameProtocol.ErrorUnavailable,
        _ => AppFrameProtocol.ErrorDenied,
    };

    /// <summary>The fixed, sanitised message for each error code; nothing from the cluster or the frame reaches it.</summary>
    /// <param name="code">The error code.</param>
    /// <returns>The message.</returns>
    internal static string MessageFor(string code) => code switch
    {
        AppFrameProtocol.ErrorNotFound => "Not found.",
        AppFrameProtocol.ErrorInvalid => "The request was invalid.",
        AppFrameProtocol.ErrorTooLarge => "The request or response was too large.",
        AppFrameProtocol.ErrorRateLimited => "Too many requests.",
        AppFrameProtocol.ErrorUnavailable => "The request could not be served.",
        AppFrameProtocol.ErrorConflict => "The request conflicted with the current state.",
        _ => "The request was denied.",
    };

    private static Envelope ReadEnvelope(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            return default;
        }

        long id = 0;
        string? op = null;
        JsonElement args = default;
        var wellFormed = true;
        var seen = 0;
        foreach (var property in root.EnumerateObject())
        {
            switch (property.Name)
            {
                case "id" when (seen & 1) == 0:
                    seen |= 1;
                    if (property.Value.ValueKind == JsonValueKind.Number
                        && property.Value.TryGetInt64(out var candidate)
                        && candidate is >= 1 and <= AppFrameProtocol.MaxRequestId)
                    {
                        id = candidate;
                    }

                    break;
                case "op" when (seen & 2) == 0:
                    seen |= 2;
                    if (property.Value.ValueKind == JsonValueKind.String)
                    {
                        op = property.Value.GetString();
                    }
                    else
                    {
                        wellFormed = false;
                    }

                    break;
                case "args" when (seen & 4) == 0:
                    seen |= 4;
                    args = property.Value;
                    if (args.ValueKind != JsonValueKind.Object)
                    {
                        wellFormed = false;
                    }

                    break;
                default:
                    wellFormed = false;
                    break;
            }
        }

        return new Envelope(id, op, args, wellFormed && op is not null);
    }

    private static Args ReadArgs(JsonElement element)
    {
        var args = new Args();
        if (element.ValueKind != JsonValueKind.Object)
        {
            return args;
        }

        foreach (var property in element.EnumerateObject())
        {
            var flag = property.Name switch
            {
                "action" => Action,
                "tree" => Tree,
                "key" => Key,
                "prefix" => Prefix,
                "pageSize" => PageSize,
                "continuation" => Continuation,
                "value" => Value,
                "path" => Path,
                "text" => Text,
                _ => 0,
            };

            if (flag == 0)
            {
                args.Unknown = true;
                continue;
            }

            if ((args.Present & flag) != 0)
            {
                args.Invalid = true;
                continue;
            }

            args.Present |= flag;
            var value = property.Value;
            if (flag == PageSize)
            {
                if (value.ValueKind == JsonValueKind.Null)
                {
                    continue;
                }

                if (value.ValueKind != JsonValueKind.Number
                    || !value.TryGetInt32(out var pageSize)
                    || pageSize is < 1 or > AppFrameProtocol.MaxPageSize)
                {
                    args.Invalid = true;
                    continue;
                }

                args.PageSize = pageSize;
                continue;
            }

            if (flag == Continuation && value.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (value.ValueKind != JsonValueKind.String)
            {
                args.Invalid = true;
                continue;
            }

            var text = value.GetString();
            switch (flag)
            {
                case Action: args.Action = text; break;
                case Tree: args.Tree = text; break;
                case Key: args.Key = text; break;
                case Prefix: args.Prefix = text; break;
                case Continuation: args.Continuation = text; break;
                case Value: args.Value = text; break;
                case Path: args.Path = text; break;
                default: args.Text = text; break;
            }
        }

        return args;
    }

    /// <summary>Checks that the arguments carry exactly the members the operation defines, with well-formed text.</summary>
    private static bool HasExactShape(string op, Args args)
    {
        var (required, optional) = op switch
        {
            AppFrameProtocol.ContextRead or AppFrameProtocol.ContextUser => (0, 0),
            AppFrameProtocol.NavSync => (Path, 0),
            AppFrameProtocol.UiNotify => (Text, 0),
            AppFrameProtocol.DataRead when args.Action == AppFrameProtocol.ActionGet => (Action | Tree | Key, 0),
            AppFrameProtocol.DataRead when args.Action == AppFrameProtocol.ActionScan => (Action | Tree | Prefix, PageSize | Continuation),
            AppFrameProtocol.DataWrite when args.Action == AppFrameProtocol.ActionSet => (Action | Tree | Key | Value, 0),
            AppFrameProtocol.DataDelete when args.Action == AppFrameProtocol.ActionDelete => (Action | Tree | Key, 0),
            _ => (-1, 0),
        };

        if (required < 0 || (args.Present & required) != required || (args.Present & ~(required | optional)) != 0)
        {
            return false;
        }

        return op switch
        {
            AppFrameProtocol.NavSync => IsSafePath(args.Path),
            AppFrameProtocol.UiNotify => IsSafeText(args.Text, AppFrameProtocol.MaxNotifyLength),
            _ => true,
        };
    }

    /// <summary>Returns the error code for an argument over its size limit, or <see langword="null"/>.</summary>
    private static string? CheckSizes(Args args)
    {
        if (args.Key is { } key && (key.Length == 0 || key.Length > AppFrameProtocol.MaxKeyLength))
        {
            return key.Length == 0 ? AppFrameProtocol.ErrorInvalid : AppFrameProtocol.ErrorTooLarge;
        }

        if (args.Prefix is { Length: > AppFrameProtocol.MaxKeyLength })
        {
            return AppFrameProtocol.ErrorTooLarge;
        }

        if (args.Continuation is { Length: > AppFrameProtocol.MaxContinuationLength })
        {
            return AppFrameProtocol.ErrorTooLarge;
        }

        if (args.Value is { Length: > MaxValueBase64Length })
        {
            return AppFrameProtocol.ErrorTooLarge;
        }

        return null;
    }

    /// <summary>Decodes a base64 value, or returns <see langword="null"/> when it is not valid base64 or decodes too large.</summary>
    private static ReadOnlyMemory<byte>? DecodeValue(string base64, out bool tooLarge)
    {
        // Allocated per write: the bytes must outlive the await on the cluster call.
        var buffer = new byte[(base64.Length / 4 * 3) + 3];
        tooLarge = false;
        if (!Convert.TryFromBase64String(base64, buffer, out var written))
        {
            return null;
        }

        if (written > AppFrameProtocol.MaxValueBytes)
        {
            tooLarge = true;
            return null;
        }

        return buffer.AsMemory(0, written);
    }

    /// <summary>An in-app path: starts with one <c>/</c>, at most the protocol's length, and free of control, bidi or back-slash characters.</summary>
    private static bool IsSafePath(string? path) =>
        path is { Length: > 0 and <= AppFrameProtocol.MaxPathLength }
        && path[0] == '/'
        && (path.Length == 1 || path[1] != '/')
        && !path.Contains('\\', StringComparison.Ordinal)
        && IsSafeText(path, AppFrameProtocol.MaxPathLength);

    /// <summary>Plain text: 1 to <paramref name="maxLength"/> characters, well-formed UTF-16, with no control or bidirectional-override character.</summary>
    internal static bool IsSafeText(string? text, int maxLength)
    {
        if (text is null || text.Length == 0 || text.Length > maxLength)
        {
            return false;
        }

        var span = text.AsSpan();
        while (!span.IsEmpty)
        {
            if (Rune.DecodeFromUtf16(span, out var rune, out var consumed) != OperationStatus.Done
                || Rune.IsControl(rune)
                || rune.Value is 0x200E or 0x200F or (>= 0x202A and <= 0x202E) or (>= 0x2066 and <= 0x2069))
            {
                return false;
            }

            span = span[consumed..];
        }

        return true;
    }

    private static string WriteEmptyOk(long id) => WriteOk(id, 0, static (_, _) => { });

    private static string WriteOk<TState>(long id, TState state, Action<Utf8JsonWriter, TState> writeResult)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("id", id);
            writer.WriteBoolean("ok", true);
            writer.WriteStartObject("result");
            writeResult(writer, state);
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static string WriteError(long id, string code)
    {
        var buffer = new ArrayBufferWriter<byte>(128);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("id", id);
            writer.WriteBoolean("ok", false);
            writer.WriteStartObject("error");
            writer.WriteString("code", code);
            writer.WriteString("message", MessageFor(code));
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "App bridge request from '{AppSlug}' for '{Operation}' refused: {Denial}.")]
    private static partial void LogDenied(ILogger logger, string appSlug, string operation, AppBridgeDenial denial);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "App bridge relay for '{AppSlug}' '{Operation}' failed ({ExceptionType}).")]
    private static partial void LogRelayFailed(ILogger logger, string appSlug, string operation, string exceptionType);

    private readonly record struct Envelope(long Id, string? Operation, JsonElement Args, bool WellFormed);

    private struct Args
    {
        public int Present;
        public bool Unknown;
        public bool Invalid;
        public string? Action;
        public string? Tree;
        public string? Key;
        public string? Prefix;
        public int PageSize;
        public string? Continuation;
        public string? Value;
        public string? Path;
        public string? Text;
    }
}
