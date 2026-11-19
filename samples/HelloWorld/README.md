# Hello World

## What it shows

The minimal starting point. The sample hosts a single-silo, in-memory Orleans
cluster with `AddLattice(...)`, in-memory grain storage and in-memory reminders,
then runs an interactive prompt over one tree, `hello-world`. Each command is one
`ILattice` call:

| Command | Call |
|---|---|
| `create`, `update` | `SetAsync(key, value)` - both are the same upsert. |
| `read` | `GetAsync(key)` |
| `delete` | `DeleteAsync(key)` |
| `list` | `ScanEntriesAsync()`, printing every entry in key order. |
| `exit` (or `quit`) | Stops the silo and exits. |

Every operation is timed and reported as `[OK]` (green) or `[FAIL]` (red) with
its elapsed milliseconds. A read or delete of a missing key reports `[FAIL]` with
`<not found>`, and an operation that throws reports `[FAIL]` with the exception
type and message. A blank line is ignored, and an unknown command is reported
with a reminder of the valid commands. The project references the library project directly, so the
sample doubles as a quick sanity check of a local build.

## Run it

```
dotnet run --project samples/HelloWorld
```

## Expected output

An example session (what you type follows each prompt; the timings vary from run
to run):

```
Silo Starting... ready.
Orleans.Lattice HelloWorld sample
Commands: create, read, update, delete, list, exit

Next Operation: create
  key: greeting
  value: hello
  [OK] create 'greeting' = 'hello' (12.34 ms)
Next Operation: read
  key: greeting
  [OK] read 'greeting' = 'hello' (1.02 ms)
Next Operation: list
  greeting = hello
  [OK] list -> 1 entry (3.21 ms)
Next Operation: delete
  key: greeting
  [OK] delete 'greeting' (2.45 ms)
Next Operation: read
  key: greeting
  [FAIL] read 'greeting' -> <not found> (0.87 ms)
Next Operation: exit
```

## When to use

- A first look at the `ILattice` read and write surface, with nothing to set up.
- Checking that a local build of `Orleans.Lattice` behaves before trying a
  feature sample.

## When not to use

- Anything that should outlive the process: storage and reminders are in memory,
  so every run starts from an empty tree.
- Learning a specific capability. The focused feature samples, listed in the
  [samples index](../../docs/lattice/samples.md), each demonstrate one.

## Feature docs

- [Samples](../../docs/lattice/samples.md#helloworld)
- [API reference](../../docs/lattice/api.md)
