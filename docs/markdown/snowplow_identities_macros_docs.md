{% docs macro_get_cluster_by_values %}
{% raw %}

A macro to manage the cluster by fields for various models in the package.

#### Returns

The field to cluster by based on model name and target type.

{% endraw %}
{% enddocs %}

{% docs macro_get_incremental_filter %}
Emits the incremental `where` predicate for a self-watermarking model.

Each model asks its own table for the newest `load_tstamp` it already holds and processes forward from there in day-aligned windows, so there is no shared progress manifest. A startup check guards upgrades from 0.1.0. A failed run leaves the affected model slightly behind, and it catches up on the next run.

The window re-scans `snowplow__lookback_days` of overlap for late-arriving data; combined with each model's `unique_key` upsert, that makes re-running a batch a no-op, which is what replaces the manifest's crash-recovery guarantee.

The normal window advances by `snowplow__backfill_limit_days`. Atomic readers extend it through the first eligible day after a gap; derived models extend it through their upstream watermark. The first-day lookup applies application and identity-context filters and can read source timestamps beyond the batch. See MIGRATION.md for scan-cost and late-arrival limits.
{% enddocs %}
