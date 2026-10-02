# Milestone 4 results summary

Median over repetitions; brackets show min–max across repetitions.


## E1 – Coordination overhead vs number of replicas

| Mode | Replicas | Clients | Admission (ms) | ↳ commit (ms) | Admissions/s | Messages per admission (incl. its release) | Idle messages/s | Leader CPU % | Follower CPU % |
|---|---|---|---|---|---|---|---|---|---|
| S3 single orchestrator | 1 | 1 | 3.49 (3.32–3.96) | – | 285.9 (252.1–301.1) | 6.0 | 5.3 (5.0–5.3) | 33.4 (33.2–36.0) | – |
| Raft | 1 | 1 | 3.48 (3.23–5.66) | 0.15 (0.14–0.16) | 286.6 (176.5–309.8) | 6.0 | 5.3 | 36.0 (31.8–39.7) | – |
| Raft | 3 | 1 | 6.33 (5.08–11.35) | 1.15 (1.04–2.33) | 157.7 (88.1–196.8) | 13.2 (12.5–13.3) | 92.5 (92.0–93.1) | 36.2 (27.1–42.3) | 9.3 (7.4–9.4) |
| Raft | 5 | 1 | 8.72 (7.68–8.92) | 1.86 (1.70–1.87) | 114.6 (112.1–130.1) | 20.0 (19.9–20.3) | 180.6 (180.4–183.6) | 37.9 (36.0–38.3) | 8.1 (8.1–8.4) |
| Raft | 7 | 1 | 9.49 (9.14–10.88) | 2.41 (2.38–2.84) | 105.3 (91.8–109.4) | 27.0 (26.7–27.0) | 269.3 (267.9–272.3) | 38.1 (37.7–39.3) | 7.9 (7.8–7.9) |
| Raft+fsync | 3 | 1 | 8.16 (7.88–8.50) | 2.58 (2.42–2.60) | 122.5 (117.5–126.8) | 13.1 (13.0–13.1) | 93.1 (93.1–94.5) | 35.8 (35.5–36.5) | 10.5 (9.9–10.5) |
| S3 single orchestrator | 1 | 4 | 12.86 (12.81–16.97) | – | 308.0 (231.5–309.4) | 6.0 | 5.0 (5.0–5.3) | 34.5 (33.6–37.6) | – |
| Raft | 1 | 4 | 10.14 (8.82–21.05) | 0.13 (0.13–0.15) | 390.3 (188.5–448.4) | 6.0 | 5.3 | 34.4 (31.1–36.7) | – |
| Raft | 3 | 4 | 21.26 (15.59–27.07) | 1.89 (1.39–2.40) | 185.9 (145.5–253.4) | 13.0 (12.7–13.3) | 93.1 (92.9–94.4) | 41.2 (38.1–41.3) | 9.9 (9.4–11.1) |
| Raft | 5 | 4 | 22.24 (19.85–25.74) | 2.36 (1.96–2.56) | 177.0 (154.2–199.9) | 19.5 (19.2–20.2) | 181.4 (180.6–181.7) | 38.3 (38.0–39.7) | 8.8 (8.0–9.2) |
| Raft | 7 | 4 | 28.80 (25.47–42.41) | 3.25 (2.69–4.72) | 137.6 (92.5–153.8) | 26.4 (24.5–26.5) | 268.2 (268.0–268.7) | 37.2 (33.9–38.4) | 7.4 (7.3–7.5) |
| Raft+fsync | 3 | 4 | 25.18 (24.91–27.89) | 3.31 (3.27–3.44) | 157.4 (142.5–159.5) | 12.6 (11.7–12.7) | 93.1 (93.1–93.7) | 36.9 (32.0–37.2) | 10.9 (8.7–11.1) |

## E2 – Synchronization delay vs replica placement

| Placement | Replicas | Leader at | One-way delay leader → followers (ms) | Predicted commit (ms) | Measured commit (ms) | Admission (ms) |
|---|---|---|---|---|---|---|
| 3 in core | 3 | core | 0/0 | 0 | 1.1 (0.8–1.1) | 5.5 (4.2–7.1) |
| core + edge-A + cloud | 3 | core | 5/20 | 10 | 12.5 (12.4–12.6) | 18.0 (17.8–18.6) |
| core + edge-A + cloud (leader in cloud) | 3 | cloud | 20/25 | 40 | 43.2 (42.9–43.3) | 49.3 (48.7–50.0) |
| core + 2 x cloud | 3 | core | 20/20 | 40 | 43.0 (43.0–43.4) | 52.0 (48.3–71.8) |
| 5: core, 2 edges, 2 clouds | 5 | core | 5/6/20/20 | 12 | 14.7 (14.5–14.9) | 20.4 (20.1–21.1) |

## E3 – Leader crash: election and unavailability

| Replicas | Election timeout (ms) | Runs | New leader elected after (ms) | Admissions unavailable (ms) | Extra elections (split votes) | Client requests failed | Restarted replica catch-up (ms) | Replicas identical after recovery |
|---|---|---|---|---|---|---|---|---|
| 3 | 150–300 | 10 | 223 (183–257) | 329 (325–337) | 0 | 0 | 181 (150–232) | 10/10 |
| 3 | 300–600 | 10 | 374 (320–562) | 484 (478–637) | 0 | 0 | 229 (158–255) | 10/10 |
| 3 | 600–1200 | 10 | 774 (636–2624) | 873 (786–2771) | 2 | 0 | 181 (159–360) | 10/10 |
| 5 | 150–300 | 10 | 168 (152–357) | 332 (177–481) | 1 | 0 | 185 (153–307) | 10/10 |
| 5 | 300–600 | 10 | 334 (304–396) | 483 (332–488) | 0 | 0 | 186 (159–278) | 10/10 |
| 5 | 600–1200 | 10 | 755 (648–972) | 790 (784–1098) | 0 | 0 | 183 (155–250) | 10/10 |

## E4 – Consistency: uncoordinated orchestrators vs Raft

| Setup | Runs | Requests | Real capacity (instances) | Acknowledged to clients | Instances on sites | Overbooked sites (of 6) | Max site load % | Same id for different instances | Acked but lost | Same request admitted twice | Replica fingerprints identical |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2 uncoordinated orchestrators | 3 | 150 | 60 | 120 | 120 | 6 | 200 | 60 | – | – | – |
| 3 uncoordinated orchestrators | 3 | 150 | 60 | 150 | 150 | 6 | 270 | 100 | – | – | – |
| Raft cluster (2 leader crashes) | 3 | 150 | 60 | 60 | 60 | 0 | 100 | 0 | 0 | 0 | 3/3 |

## E5 – Event ordering: skewed wall clocks vs Lamport clocks

| Max clock error per node (± ms) | Runs | Cause→effect pairs checked | Wrong order by wall clock | Wrong order by Lamport clock | Apparent send→receive gap by wall clock (ms, median) | Wrong by kind (all runs) |
|---|---|---|---|---|---|---|
| ±0 | 10 | 3613 | 0 (0.0 %) | 0 | 0.59 (0.46–0.76) | INSTANCE_EXIT 0/600, AppendEntries 0/2413, ALLOCATE 0/600 |
| ±1 | 10 | 3613 | 544 (15.1 %) | 0 | 0.83 (0.05–1.71) | INSTANCE_EXIT 34/600, AppendEntries 380/2413, ALLOCATE 130/600 |
| ±5 | 10 | 3601 | 1914 (53.2 %) | 0 | -0.69 (-5.83–4.64) | INSTANCE_EXIT 206/600, AppendEntries 1502/2401, ALLOCATE 206/600 |
| ±20 | 10 | 3608 | 1505 (41.7 %) | 0 | 3.92 (-19.01–17.12) | INSTANCE_EXIT 240/600, AppendEntries 944/2408, ALLOCATE 321/600 |
| ±50 | 10 | 3610 | 1792 (49.6 %) | 0 | 3.03 (-22.24–26.58) | INSTANCE_EXIT 420/600, AppendEntries 1192/2410, ALLOCATE 180/600 |
