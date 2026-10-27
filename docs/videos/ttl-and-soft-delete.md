---
agent_spec: "docs/agents/concepts.yaml"
---

# TTL and soft delete

Entries can expire on their own, and a deleted tree can be recovered inside its retention window.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="ttl-and-soft-delete" path="build" order="5" length="1:50" cut="391a0143a271" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/ttl-and-soft-delete.html),
> or [download it](../../docs-site/media/ttl-and-soft-delete-391a0143a271.mp4)
> (MP4, 1:50, 5.9 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

### TTL and soft delete

Atomic writes showed how related changes become visible as a whole. Here, we ask how data goes away: on its own, or by mistake.

This is TTL and soft delete: entries can expire on their own, and a deleted tree can be recovered inside its retention window.

### Entries that expire

A session should not live for ever. Write it with a time to live, here thirty minutes.

The silo turns that duration into an absolute expiry time, so a client with a skewed clock does not move it.

Once that time passes, the entry is gone from every read. A get returns nothing, an existence check says false, and scans and counts skip it.

### Refresh and reaping

Expiry follows the write that wins. A later write replaces the entry as usual, and a winning plain write without a time to live clears the expiry.

On a CRDT entry, expiries merge by taking the later one, so a refresh only ever extends it.

An expired entry is not deleted straight away. Compaction reaps it after the tombstone grace period, so a lagging peer cannot bring it back.

### Soft delete

Deleting a whole tree is a soft delete. The tree becomes inaccessible at once: its reads and writes throw.

But its data is not gone. It is kept for a retention window, seventy-two hours by default, before it is purged.

### Recovery

Inside that window, recovering the tree makes it accessible again, with all the data it held before the delete.

Once the purge has started, it is too late; and once the purge is complete, the data is gone.

### Where next

Next on Build: Going durable. The links are on this episode's page in the documentation.

<!-- transcript:end -->

## The code on screen

The episode writes a session with a thirty-minute time to live, then deletes a tree and recovers it inside its retention window. The snippets are compiled with the rest of the documentation.

<!-- video-snippet: ttl-and-soft-delete/set-with-ttl -->
```csharp verify
var session = new User("alice", 1);

await lattice.SetAsync("session:42", session, TimeSpan.FromMinutes(30));
```

<!-- video-snippet: ttl-and-soft-delete/delete -->
```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.DeleteTreeAsync();
```

<!-- video-snippet: ttl-and-soft-delete/recover -->
```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.RecoverTreeAsync();

// Tree is accessible again - all data from before the delete is restored.
byte[]? value = await tree.GetAsync("customer-123");
```

## Where next

<!-- where-next:begin -->

- **Next on Build:** Going durable
- **The pages it introduces:** [TTL](../lattice/ttl.md)

<!-- where-next:end -->
