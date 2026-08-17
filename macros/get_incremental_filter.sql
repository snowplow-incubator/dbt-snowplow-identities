{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Self-watermarking incremental filter. Each model asks its own table "what is the newest
  data I already have?" (max load_tstamp) and processes forward from there, in day-aligned
  windows. No shared manifest, no run hooks: if a run fails, the affected model is simply
  behind and catches up next run.

  Arguments:
    relation_alias       alias of the source relation in the query (e.g. 'e'), or none.
    use_atomic_partition when true (staging models reading atomic.events), also emit a
                         predicate on atomic's physical partition column so the warehouse
                         can prune. Set snowplow__partition_tstamp to the column atomic is
                         actually partitioned by (often collector_tstamp) and
                         snowplow__partition_tstamp_type to that column's type, so the
                         bounds are emitted in a matching type -- required for pruning to
                         engage. The partition column itself is never wrapped in a cast,
                         which would defeat pruning; only the literal bounds are typed.

  Window (day-aligned):
    lower = watermark day - (lookback_days - 1)   re-scan overlap for late-arriving data
    upper = watermark day + (backfill_days + 1)   throttle: N new days processed per run

  On the first run there is no {{ this }}, so the window starts at snowplow__start_date.

  Idempotency: re-running re-reads the lookback overlap, and each model's unique_key
  upsert makes the re-processed rows a no-op. This is what replaces the manifest's
  crash-recovery guarantee, and is why every incremental batch is run repeatedly in CI.

  Note: a gap between events larger than snowplow__backfill_limit_days stalls progress,
  because the window can never reach the next event. Keep the backfill limit comfortably
  above the largest gap you expect. This was equally true of the manifest framework.
#}

{% macro get_incremental_filter(relation_alias=none, use_atomic_partition=false) %}
  {%- set alias = (relation_alias ~ '.') if relation_alias is not none else '' -%}
  {%- set start_date = var('snowplow__start_date') -%}
  {%- set lookback = var('snowplow__lookback_days', 1) -%}
  {%- set backfill = var('snowplow__backfill_limit_days', 30) -%}
  {%- set partition_col = var('snowplow__partition_tstamp', 'load_tstamp') -%}
  {%- set partition_type = var('snowplow__partition_tstamp_type', 'timestamp') | lower -%}
  {%- set partition_buffer = var('snowplow__partition_buffer_days', 2) -%}
  {%- set start_ts = "cast('" ~ start_date ~ "' as timestamp)" -%}

  {%- if partition_type not in ['timestamp', 'date'] -%}
    {{ exceptions.raise_compiler_error(
      "snowplow__partition_tstamp_type must be 'timestamp' or 'date', got '" ~ partition_type ~ "'."
    ) }}
  {%- endif -%}

  {%- if is_incremental() -%}
    {%- set wm_day -%}
      (select coalesce(max(cast(load_tstamp as date)), cast({{ start_ts }} as date)) from {{ this }})
    {%- endset -%}
    {%- set lower_day = dbt.dateadd('day', 1 - lookback, wm_day) -%}
    {%- set upper_day = dbt.dateadd('day', 1 + backfill, wm_day) -%}
    {%- do snowplow_identities.log_incremental_window(start_date, lookback, backfill) -%}
  {%- else -%}
    {%- set lower_day = "cast(" ~ start_ts ~ " as date)" -%}
    {%- set upper_day = dbt.dateadd('day', backfill, "cast(" ~ start_ts ~ " as date)") -%}
    {%- do snowplow_identities.log_full_refresh_window(start_date, backfill) -%}
  {%- endif -%}

  {{ alias }}load_tstamp >= cast({{ lower_day }} as timestamp)
  and {{ alias }}load_tstamp < cast({{ upper_day }} as timestamp)
  {%- if use_atomic_partition and partition_col != 'load_tstamp' %}
  and {{ alias }}{{ partition_col }} >= cast({{ dbt.dateadd('day', -partition_buffer, lower_day) }} as {{ partition_type }})
  and {{ alias }}{{ partition_col }} < cast({{ dbt.dateadd('day', partition_buffer, upper_day) }} as {{ partition_type }})
  {%- endif -%}
{% endmacro %}


{#
  Log the window a model is about to process, so a run's console output answers "what is
  being processed?" the way the manifest framework's print_run_limits used to.

  The bounds in the predicate are a scalar subquery, so the actual dates are not known at
  compile time -- this resolves them with one cheap max(load_tstamp) query against the
  model's own table. Restricted to `dbt run` / `dbt build`: during unit tests
  is_incremental() is overridden while {{ this }} may not exist, and issuing a query there
  would break the test rather than inform anyone.
#}

{% macro log_incremental_window(start_date, lookback, backfill) %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set which = flags.WHICH | default('', true) -%}
  {%- if which not in ['run', 'build'] -%}{{ return('') }}{%- endif -%}

  {%- set start_ts = "cast('" ~ start_date ~ "' as timestamp)" -%}
  {%- set wm_day -%}
    (select coalesce(max(cast(load_tstamp as date)), cast({{ start_ts }} as date)) from {{ this }})
  {%- endset -%}

  {%- set query -%}
    select
      cast({{ dbt.dateadd('day', 1 - lookback, wm_day) }} as {{ dbt.type_string() }}) as lower_bound,
      cast({{ dbt.dateadd('day', 1 + backfill, wm_day) }} as {{ dbt.type_string() }}) as upper_bound,
      cast({{ wm_day }} as {{ dbt.type_string() }}) as watermark
  {%- endset -%}

  {%- set results = run_query(query) -%}
  {%- if results and results.rows | length > 0 -%}
    {%- set row = results.rows[0] -%}
    {{ snowplow_utils.log_message(
      "Snowplow: " ~ this.identifier ~ " at watermark " ~ row[2]
      ~ " -- processing load_tstamp from " ~ row[0] ~ " to " ~ row[1]
    ) }}
  {%- endif -%}
{% endmacro %}


{% macro log_full_refresh_window(start_date, backfill) %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set which = flags.WHICH | default('', true) -%}
  {%- if which not in ['run', 'build'] -%}{{ return('') }}{%- endif -%}
  {{ snowplow_utils.log_message(
    "Snowplow: " ~ this.identifier ~ " building from scratch -- processing "
    ~ backfill ~ " day(s) of load_tstamp from snowplow__start_date " ~ start_date
    ~ ". Re-run to advance further."
  ) }}
{% endmacro %}
