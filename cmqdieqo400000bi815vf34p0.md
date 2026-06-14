---
title: "Building a Lightweight Rule-Based Engine"
seoTitle: "Building rule engine"
seoDescription: "Building pub sub architecture rule engine"
datePublished: 2026-06-14T08:15:31.986Z
cuid: cmqdieqo400000bi815vf34p0
slug: building-a-lightweight-rule-based-engine
tags: engineeringstories

---

Real-time alerting sounds simple at first: read logs, match rules, generate alerts. At least that is what I thought when this started.

The actual requirement was fairly straightforward on paper:

*   continuously read JSON logs
    
*   evaluate records against a set of rules
    
*   generate alerts whenever conditions match
    
*   avoid external infrastructure dependencies entirely
    

No Kafka, no Elasticsearch, no queues, nothing fancy. Just a process watching logs and reacting to them.

What started as a very small Python script eventually turned into a lightweight concurrent rule engine in Go once scale started exposing bottlenecks I had not really thought much about initially.

This article is mostly me sharing that evolution, how the system changed as the problems changed. It is not meant to be a production-ready architecture guide, just the core ideas and decisions that shaped the system over time.

## The Problem

The input was a continuously growing JSON log file where each line looked something like this:

```json
{"a": 500, "b": 200}
{"a": 700, "b": 300}
```

And rules looked like:

*   `a > 400 and b < 250`
    
*   `a == 700 or b == 0`
    
*   `b > 350`
    

Whenever a rule matched, an alert needed to be triggered.

An alert itself could mean anything really, sending an email, posting to Slack, invoking some webhook, or writing somewhere else. That part was not particularly interesting. The interesting part ended up being how to represent and evaluate rules efficiently once the number of rules started growing.

## The Constraints

One thing that shaped almost every decision here was the environment itself.

This system needed to:

*   run on a single machine
    
*   avoid external infra dependencies
    
*   recover after restart
    
*   process logs continuously
    
*   stay reasonably close to real time
    

Which meant a lot of common solutions were immediately off the table.

There was definitely a version of this system that could have been built around Kafka or Elasticsearch, but the environment this needed to run in was fairly resource constrained, so operational simplicity mattered much more than architectural elegance.

## Representing Rules

The matching logic itself was not difficult. The difficult part was representing arbitrary boolean logic in a way that was easy to evaluate programmatically.

Something like this gets messy fairly quickly if you try evaluating it directly:

```plaintext
((a == 30 or b > 25) and (b < 40 and a > 35))
or
(c == 12 and a > 40)
```

There are many ways to model this:

*   expression trees
    
*   ASTs
    
*   JSONLogic
    
*   custom DSLs
    

But for simplicity, I ended up using a much flatter structure.

The idea was:

*   AND inside groups
    
*   OR between groups
    

So something like:

```plaintext
(a == 5 and b > 2)
or
(c > 4 and d == 3)
```

became:

```plaintext
[
  { "a.$eq": 5, "b.$gt": 2 },
  { "c.$gt": 4, "d.$eq": 3 }
]
```

This ended up simplifying the runtime logic a lot because the evaluator no longer needed to understand deeply nested boolean expressions.

## Flattening Complex Conditions

More complicated expressions were flattened algebraically.

For example:

```plaintext
(a == 5 or b > 2)
and
(c > 4 or d == 3)
```

could be expanded into:

```plaintext
(a == 5 and c > 4)
or
(a == 5 and d == 3)
or
(b > 2 and c > 4)
or
(b > 2 and d == 3)
```

which then fit neatly into the same structure:

```json
[
  { "a.$eq": 5, "c.$gt": 4 },
  { "a.$eq": 5, "d.$eq": 3 },
  { "b.$gt": 2, "c.$gt": 4 },
  { "b.$gt": 2, "d.$eq": 3 }
]
```

At first this felt slightly awkward, but later it made evaluation logic extremely predictable and easy to reason about.

## Matching Logic

Once the rule structure was normalized, the matching logic itself became pretty small.

```python
def matches(rule, record):
    def eval_condition(key, value):
        field, op = key.split(".$")

        if field not in record:
            return False

        if op == "eq":
            return record[field] == value
        elif op == "gt":
            return record[field] > value

        return False  # unsupported operator

    # OR across groups
    for group in rule:
        # AND within a group
        if all(eval_condition(k, v) for k, v in group.items()):
            return True

    return False
```

For simplicity I only supported a couple operators here, but the overall architecture stayed the same regardless of how many operators were eventually added.

## The First Working Script

Once the matching logic was in place, the rest of the system was honestly pretty small.

The process simply:

*   tailed the log file
    
*   parsed JSON records
    
*   evaluated rules
    
*   generated alerts
    
*   periodically saved offsets for restart recovery
    

The actual script looked roughly like this:

```python
OFFSET_FILE = "offset.txt"


def save_offset(pos):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(pos))


def load_offset():
    try:
        with open(OFFSET_FILE, "r") as f:
            return int(f.read())
    except:
        return 0


def alert(record):
    print("ALERT:", record)


with open("log.jsonl", "r") as f:
    f.seek(load_offset())

    rules = [
        [{"a.$gt": 400, "b.$lt": 250}, {"a.$eq": 700}],
    ]

    last_saved = time.time()

    while True:
        pos = f.tell()
        line = f.readline()

        if not line:
            time.sleep(1)
            continue

        record = json.loads(line)

        for rule in rules:
            if matches(rule, record):
                alert(record)

        if time.time() - last_saved > 1:
            save_offset(pos)
            last_saved = time.time()
```

And honestly, this version worked surprisingly well for a while.

One thing I liked about this approach early on was how little infrastructure or coordination was involved. The system was easy to reason about, easy to debug, and restart recovery was basically just tracking file offsets.

At smaller scale, this was more than enough.

## The Sequential Bottleneck

The problem only started appearing once both the incoming logs and the number of rules started increasing significantly.

Initially this was barely noticeable. Then gradually alerts started becoming delayed. Then very delayed.

Eventually we reached a point where people were spotting issues manually before alerts were even generated, which was a pretty strong sign that something had gone wrong.

The architecture at this point was still fundamentally:

![](https://cdn.hashnode.com/uploads/covers/68bd444a246904ab71032ce8/ba466baf-2344-400a-8f28-6148bc3b2dd3.png align="center")

Every single record needed to sequentially pass through every single rule. That becomes painful very quickly once rule count starts exploding.

And the frustrating part was that each rule evaluation was completely independent from the others, meaning the system was spending a lot of time waiting on sequential work that theoretically could have happened in parallel.

## Considering Parallelism

At that point the next direction became fairly obvious.

Rule evaluations were naturally parallelizable because they were completely independent from one another.

The original implementation was in Python though, and this exposed another issue.

The workload here was CPU-bound rather than I/O-bound, so threads were not particularly useful because of Python’s GIL. Multiple threads still would not truly evaluate rules in parallel.

Multiprocessing was definitely possible, but it started feeling slightly uncomfortable architecturally once I thought about scale. Because now instead of:

> “how do we evaluate rules?”

the problem slowly becomes:

> “how do we manage a growing number of worker processes efficiently? and is spawning hundreds if not thousands of process for this really necessary ?”

And that felt like shifting complexity rather than reducing it.

There was also a version of this architecture where logs could have been pushed into something like Elasticsearch and rules translated into queries. That would have shifted the matching engine from being CPU-bound to largely I/O-bound, making async processing much more viable. But doing that would have introduced external infrastructure dependencies the environment was specifically trying to avoid in the first place.

## Why Go Felt Natural Here

This was the point where I decided to move the system to Go.

Not because Python was bad at all, but because the architecture had fundamentally shifted from sequential processing to concurrent processing, and Go fits that model extremely naturally.

Coming from Python, the concurrency model in Go felt surprisingly natural for this kind of problem. A channel is essentially a communication pipe between goroutines, one goroutine can send data into a channel while another goroutine receives data from it safely without needing to manually manage locks in most cases.

What I found particularly interesting was how well this matched the actual mental model of the system.

The architecture already resembled a pub/sub system conceptually:

*   one component continuously reading logs
    
*   multiple independent rule evaluators processing them
    

Channels mapped almost perfectly onto that flow because now the log reader could simply broadcast records to multiple subscribers, while each subscriber independently processed data at its own pace.

The philosophy behind channels was also something I ended up appreciating a lot while building this:

> “Do not communicate by sharing memory; instead, share memory by communicating.”

In more traditional threaded systems, multiple workers usually share some common state and you spend a lot of effort protecting that shared state with locks, mutexes, synchronization primitives, and so on.

With channels, the communication itself becomes the synchronization mechanism.

Instead of:

*   many workers fighting over shared memory
    

the flow becomes:

*   data moves through channels
    
*   workers react to incoming messages
    

For this use case especially, that model felt extremely clean because rules themselves were independent units of work. Each rule could simply sit there waiting for incoming log lines and evaluate them without needing awareness of what other rules were doing.

Conceptually

![](https://cdn.hashnode.com/uploads/covers/68bd444a246904ab71032ce8/9d55b5db-a366-46b4-9566-b8f8fda15888.png align="center")

And honestly, this was probably the most fun part of the project. Once the architecture shifted into this model, the system suddenly became much easier to scale mentally.

## The Go Script

The migration itself happened more gradually than it may sound.

The first thing I kept unchanged was the actual rule evaluation logic. The matching engine had already become fairly stable, so rewriting the entire logic from scratch would have just introduced unnecessary complexity. Just migrated function definition to that of go, keeping logic unchanged.

The real change was around how rules were executed.

Instead of having one main loop evaluate every rule sequentially, each rule now became its own lightweight worker with:

*   its rule definition
    
*   its communication channel
    
*   its goroutine
    

```go
type RuleSubscriber struct {
	ID      int
	Rule    Rule
	Channel chan string
}
```

Each subscriber would continuously wait for new log lines to arrive through its channel:

```go
func createRuleEngine(id int, rule Rule) *RuleSubscriber {
	ch := make(chan string, 100)

	go func() {
		for line := range ch {
			var record Record

			if err := json.Unmarshal([]byte(line), &record); err != nil {
				continue
			}
			if matches(rule, record) {
				alert(id, record)
			}
		}
	}()

	return &RuleSubscriber{
		ID:      id,
		Rule:    rule,
		Channel: ch,
	}
}
```

This was the point where the system started feeling less like a script and more like a small event-driven engine.

The matching logic itself remained mostly unchanged:

```go
func matches(rule Rule, record Record) bool {
	for _, group := range rule {
		groupMatched := true

		for k, v := range group {
            // just moved python logic to go
			if !evalCondition(k, v, record) {
				groupMatched = false
				break
			}
		}

		if groupMatched {
			return true
		}
	}

	return false
}
```

The main execution loop also became much simpler conceptually.

Instead of evaluating rules itself, the publisher now only needed to:

*   read log lines
    
*   broadcast them to subscribers
    
*   track offsets
    

```go
// --- 4. Main Core & Concurrency Workflow ---

func main() {
	// Raw definitions
	rawRules := []Rule{
		{
			{"a.$gt": float64(400), "b.$lt": float64(250)},
			{"a.$eq": float64(700)},
		},
		{
			{"status.$eq": "error"},
		},
	}

	// Dynamic registration
	subscribers := make([]*RuleSubscriber, len(rawRules))
	for i, rawRule := range rawRules {
		subscribers[i] = createRuleEngine(i, rawRule)
	}
	
	// Open the log file
	file, err := os.Open("log.jsonl")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Seek to last saved checkpoint
	currentPos := loadOffset()
	_, _ = file.Seek(currentPos, io.SeekStart)

	reader := bufio.NewReader(file)
	lastSaved := time.Now()

	// The Infinite Processing Loop (The Publisher)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// Tail mode: sleep briefly and try again if we hit the end of file
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}

		// BroadCast: Send the raw line to ALL rule channels
		for _, sub := range subscribers {
			sub.Channel <- line // Threads-safe channel send
		}

		// Track position offset safely
		currentPos += int64(len(line))

		// Checkpoint tracking (every 1 second)
		if time.Since(lastSaved) > 1*time.Second {
			saveOffset(currentPos)
			lastSaved = time.Now()
		}
	}
}	
```

That architectural shift alone brought alert generation much closer to real time again.

And what I liked most about this version was that despite becoming concurrent, the system still stayed fairly small and understandable. There was still no external queue, no distributed system, no complicated orchestration layer, just a publisher continuously broadcasting log lines to independent workers.

## What this article ignores

Of course once systems start becoming concurrent, an entirely new category of problems starts appearing.

Things like:

*   slow consumers
    
*   backpressure
    
*   memory growth
    

start becoming important very quickly.

In the actual production system, back pressure handling was eventually implemented using a slightly different pull-based approach where subscribers would request more data from the publisher once they finished processing their current workload.

That part became a completely different fun problem because the publisher now needed efficient tracking and replay behavior for multiple subscribers progressing at different speeds. Solving fast backtracking and replay management efficiently ended up being its own rabbit hole altogether, so I will probably keep that for a separate article.

This article was mainly focused on the evolution up to the point where the architecture shifted from sequential processing into concurrent pub/sub style processing.

## Final Thoughts

Looking back, the interesting part of this project was not the alerting itself, but how the system gradually evolved as different bottlenecks started appearing.

What began as a very small sequential script slowly pushed me toward thinking differently about concurrency, communication, and system design as scale increased.

This article was mostly me sharing that evolution and thought process rather than presenting some perfect architecture.