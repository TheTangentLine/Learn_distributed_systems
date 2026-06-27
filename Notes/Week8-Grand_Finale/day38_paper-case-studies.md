# Day 38: Case Studies — The Amazon Dynamo Paper

## Reading Assignment

Read the original paper: **"Dynamo: Amazon's Highly Available Key-Value Store"** (DeCandia et al., 2007).

A freely available version can be found by searching "Dynamo Amazon paper PDF".

This is one of the most important papers in distributed systems. It shaped the entire NoSQL movement and directly influenced Cassandra, Riak, and DynamoDB.

**Estimated reading time:** 2–3 hours for a careful first read.

---

## 6 Focus Questions

Work through the paper with these questions in mind. Write your answers as comments in a markdown annotation file (create `dynamo_annotations.md` in this folder).

### Q1 — Consistent hashing choice

Why did the Dynamo team choose consistent hashing over simple modulo hashing? Quote the section and add your own explanation of why it matters when Amazon has 100+ node clusters.

### Q2 — W/R/N defaults

The paper uses `N=3, W=2, R=2`. What do these values mean concretely for a write operation when 1 of the 3 replica nodes is temporarily unavailable? Does the write succeed or fail? Trace through the quorum math.

### Q3 — Conflict resolution strategy

Dynamo uses vector clocks (syntactic reconciliation) combined with application-level semantic reconciliation. What problem were they solving that simple LWW would not solve? Give an example using a shopping cart.

### Q4 — Gossip's role

How does Dynamo use gossip protocols? What information is gossiped? How does a new node learn about the ring topology when it first joins?

### Q5 — Sloppy quorum rationale

What is a "sloppy quorum"? In what failure scenario does a sloppy quorum allow Dynamo to accept writes that a strict quorum would reject? What is the consistency cost of this decision?

### Q6 — Hinted handoff

Describe the hinted handoff mechanism. What is the "hint"? When and how is the hint "handed off"? What happens if the original replica never recovers?

---

## Hands-on Assignment (Go) — Annotation file

Create `dynamo_annotations.md` in this folder. For each of the 6 questions:

1. Copy the most relevant quote from the paper (2–4 sentences).
2. Write your own explanation in 3–5 sentences.
3. Note any questions or confusions you have — bring them to your next tutor session.

### Template

```markdown
## Q1 — Consistent hashing choice

**Quote from paper:**
> "..."

**My explanation:**
...

**Questions I have:**
...
```

---

## Discussion Notes

After filling in your annotation file, consider these higher-level questions:

1. **CAP classification:** Is Dynamo CP or AP in its default configuration? What specific design decision is responsible for this classification?

2. **Eventual consistency:** The paper describes Dynamo as "eventually consistent." What does "eventually" mean in practice for a shopping cart? Is 100ms eventually? 1 second? 1 day?

3. **Trade-off regret:** Amazon later built DynamoDB which offers more consistency options (strongly consistent reads, transactions). What does this tell us about the original Dynamo trade-offs?

---

## Your Next Step

Now that you have studied how Amazon solved data storage at scale, Day 39 switches gears: we practice applying these concepts in a **system design interview** format. This is where your knowledge becomes a skill.
