## Part 1: Cron Expression Exercises

## Cron Expression Answers
 
| # | Requirement | Cron Expression | Explanation |
|---|-------------|-----------------|-------------|
| 1 | Daily at 2 AM |0 2 * * * | |
| 2 | Hourly at minute 15 |15 */1 * * *| |
| 3 | Monday 6 AM | 0 6 * * 0 | |
| 4 | 1st of month midnight | 0 0 1 * *| |
| 5 | Every 15 min | */15 * * * * | |
| 6 | Weekdays 8 AM | 0 8 * * 5-6| |


## Part 2: Pipeline Scheduling Design 

### Pipeline  1: Nightly Warehouse Refresh
 
**Scheduling Strategy:**  Dependency
 
**Schedule/Trigger:** Time in which backup completes varies
 
**Justification:** Source backup must be completed
 
**Failure handling:** retry upstream before retrying downstream. Save to re-run


### Pipeline 2: Hourly Clickstream Aggregation
 
**Scheduling Strategy:** event
 
**Schedule/Trigger:** files appears
 
**Justification:**
1. Why this strategy? time is not defined exactly
2. Why not the alternatives? file must be there
 
**Failure handling:**
- What if it fails? retry
- Is it safe to re-run? yes


### Pipeline  3: Financial Close Pipeline
 
**Scheduling Strategy:** dependency
 
**Schedule/Trigger:** each step depends on the previous one
 
**Justification:**
1. Why this strategy? it is a pipeline
2. Why not the alternatives? previous step must be completed
 
**Failure handling:**
- What if it fails? retry previous step
- Is it safe to re-run? yes

### Pipeline 4: Partner File Ingestion
 
**Scheduling Strategy:** Event-based
 
**Schedule/Trigger:** once the file exits, it can be processed
 
**Justification:**
1. Why this strategy? depends on the existance of the file
2. Why not the alternatives? unpredictable times
 
**Failure handling:**
- What if it fails? [recovery plan]
- Is it safe to re-run? [idempotency strategy]


### Pipeline 5: ML Feature Pipeline
 
**Scheduling Strategy:** Event-based
 
**Schedule/Trigger:** warehouse refresh
 
**Justification:**
1. Why this strategy? when the warehouse refresh is completed data can be processed
2. Why not the alternatives? can be dependency too
 
**Failure handling:**
- What if it fails? [recovery plan]
- Is it safe to re-run? [idempotency strategy]


### Pipeline 6: Data Quality Checks
 
**Scheduling Strategy:** Dependency-aware
 
**Schedule/Trigger:**
- If dependency-aware: pipeline completed
 
**Justification:**
1. Why this strategy? depends on the completition of the pipeline
2. Why not the alternatives? does not depend on time or on a specific event
 
**Failure handling:**
- What if it fails? [recovery plan]
- Is it safe to re-run? [idempotency strategy]


## Part 3: Anti-Pattern Identification
### Anti-Pattern Setup 1
 
**What's wrong:** pipeline b runs evein if pipeline a fails
**Risk:** previous step is not completed on time
**Fix:** use dependency-aware

### Anti-Pattern 2
 
**What's wrong:** unnecessary load
**Risk:** file is not found
**Fix:** check if the file exists once per week

### Anti-Pattern 3
 
**What's wrong:** refresh occurs in the middle of the backup
**Risk:** incomplete data
**Fix:** change time of refresh