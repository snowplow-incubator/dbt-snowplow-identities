{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Self-watermarking incremental filter: each model processes forward from the newest
  load_tstamp in its own table, in day-aligned windows.

    relation_alias        alias of the source relation in the query (e.g. 'e'), or none.
    use_atomic_partition  also emit a predicate on atomic's physical partition column so the
                          warehouse can prune. Configure with snowplow__partition_tstamp and
                          snowplow__partition_tstamp_type.
    anchor                extend the upper bound to stg_identity_events' watermark. True
                          for derived models; mapping follows merge_events.
    atomic_event          'identity' or 'merge': seek the first eligible day across gaps.

  Window is watermark - (lookback_days - 1) to the later of watermark + backfill_days + 1
  and the anchor's watermark. The lookback overlap plus each model's unique_key upsert
  makes re-running a batch a no-op.
#}

{% macro get_incremental_filter(relation_alias=none, use_atomic_partition=false, anchor=true, atomic_event=none, anchor_model='snowplow_identities_stg_identity_events') %}
  {%- set alias = (relation_alias ~ '.') if relation_alias is not none else '' -%}
  {%- set start_date = var('snowplow__start_date') -%}
  {%- set lookback = var('snowplow__lookback_days', 1) -%}
  {%- set backfill = var('snowplow__backfill_limit_days', 30) -%}
  {%- set partition_col = var('snowplow__partition_tstamp', 'load_tstamp') -%}
  {%- set partition_type = var('snowplow__partition_tstamp_type', 'timestamp') | lower -%}
  {%- set partition_buffer = var('snowplow__partition_buffer_days', 2) -%}
  {%- set start_ts = "cast('" ~ start_date ~ "' as timestamp)" -%}

  {#- Outside the is_incremental() branch so the ref registers as a DAG edge at parse time. -#}
  {%- set anchor_relation = ref(anchor_model) if anchor else none -%}

  {%- if lookback < 1 or backfill < 1 or partition_buffer < 0 -%}
    {{ exceptions.raise_compiler_error('lookback_days and backfill_limit_days must be positive; partition_buffer_days must be non-negative.') }}
  {%- endif -%}

  {%- if partition_type not in ['timestamp', 'date'] -%}
    {{ exceptions.raise_compiler_error(
      "snowplow__partition_tstamp_type must be 'timestamp' or 'date', got '" ~ partition_type ~ "'."
    ) }}
  {%- endif -%}

  {%- if is_incremental() -%}
    {%- set lower_day = dbt.dateadd('day', 1 - lookback, snowplow_identities.watermark_day(this, start_ts)) -%}
    {%- set upper_day = snowplow_identities.upper_bound_day(this, anchor_relation, start_ts, backfill, atomic_event) -%}
    {%- do snowplow_identities.log_incremental_window(start_date, lookback, backfill, anchor_relation, atomic_event) -%}
  {%- else -%}
    {%- set lower_day = "cast(" ~ start_ts ~ " as date)" -%}
    {%- set upper_day = dbt.dateadd('day', backfill, "cast(" ~ start_ts ~ " as date)") -%}
    {%- set upper_day = snowplow_identities.extend_window(upper_day, anchor_relation, start_ts, atomic_event, "cast(" ~ start_ts ~ " as date)") -%}
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
  Newest load_tstamp day held by a relation, falling back to snowplow__start_date when empty.
#}

{% macro watermark_day(relation, start_ts) %}
  {%- set sql -%}
    (select coalesce(max(cast(load_tstamp as date)), cast({{ start_ts }} as date)) from {{ relation }})
  {%- endset -%}
  {{ return(sql) }}
{% endmacro %}


{#
  Upper bound of the window: the later of the model's own watermark + backfill_limit_days
  and the anchor's watermark.
#}

{% macro upper_bound_day(relation, anchor_relation, start_ts, backfill, atomic_event=none) %}
  {%- set own = dbt.dateadd('day', 1 + backfill, snowplow_identities.watermark_day(relation, start_ts)) -%}
  {%- set next_day = dbt.dateadd('day', 1, snowplow_identities.watermark_day(relation, start_ts)) -%}
  {{ return(snowplow_identities.extend_window(own, anchor_relation, start_ts, atomic_event, next_day)) }}
{% endmacro %}

{% macro extend_window(upper_day, anchor_relation, start_ts, atomic_event, next_day) %}
  {%- if anchor_relation is not none -%}
    {%- set anchored = dbt.dateadd('day', 1, snowplow_identities.watermark_day(anchor_relation, start_ts)) -%}
    {%- set upper_day = 'greatest(' ~ upper_day ~ ', ' ~ anchored ~ ')' -%}
  {%- endif -%}
  {%- if atomic_event is not none -%}
    {# Seek the FIRST eligible day, never the source maximum: a gap must not turn
       a bounded backfill into a scan of every remaining populated day. #}
    {%- set available = snowplow_identities.next_atomic_day(atomic_event, next_day, upper_day) -%}
    {%- set available_end = dbt.dateadd('day', 1, available) -%}
    {%- set upper_day = 'greatest(' ~ upper_day ~ ', coalesce(' ~ available_end ~ ', ' ~ upper_day ~ '))' -%}
  {%- endif -%}
  {{ return(upper_day) }}
{% endmacro %}

{% macro next_atomic_day(event_kind, lower_day, upper_day) %}
  {# On normal populated backfill windows, avoid searching the entire remaining
     source history. Unit tests render the equivalent SQL lookup without issuing
     compile-time queries against live warehouse relations. #}
  {%- if execute and flags.WHICH in ['run', 'build'] -%}
    {%- set probe = snowplow_identities.atomic_day_query(event_kind, lower_day, upper_day) -%}
    {%- set result = run_query('select ' ~ probe ~ ' as next_day') -%}
    {%- if result and result.rows[0][0] is not none -%}
      {{ return("cast('" ~ result.rows[0][0] ~ "' as date)") }}
    {%- endif -%}
  {%- endif -%}
  {{ return(snowplow_identities.atomic_day_query(event_kind, lower_day)) }}
{% endmacro %}

{% macro atomic_day_query(event_kind, lower_day, upper_day=none) %}
  {%- set partition_col = var('snowplow__partition_tstamp', 'load_tstamp') -%}
  {%- set partition_type = var('snowplow__partition_tstamp_type', 'timestamp') | lower -%}
  {%- set sql -%}
    (select min(cast(load_tstamp as date)) from (
      select {% if event_kind == 'identity' %}{{ snowplow_identities.get_identity_fields() }}{% endif %} load_tstamp
      from {{ source('atomic', 'events') }}
      where load_tstamp >= cast({{ lower_day }} as timestamp)
      {% if upper_day is not none %}
      and load_tstamp < cast({{ upper_day }} as timestamp)
      {% endif %}
      {% if partition_col != 'load_tstamp' %}
      and {{ partition_col }} >= cast({{ dbt.dateadd('day', -var('snowplow__partition_buffer_days', 2), lower_day) }} as {{ partition_type }})
      {% if upper_day is not none %}
      and {{ partition_col }} < cast({{ dbt.dateadd('day', var('snowplow__partition_buffer_days', 2), upper_day) }} as {{ partition_type }})
      {% endif %}
      {% endif %}
      {% if event_kind == 'identity' %}
      and {{ snowplow_utils.app_id_filter(var('snowplow__app_id', [])) }}
      {% elif event_kind == 'merge' %}
      and event_name = 'identity_merge'
      and {{ snowplow_utils.app_id_filter(var('snowplow__identities_app_id', [])) }}
      {% else %}
      {{ exceptions.raise_compiler_error('Unknown atomic event kind: ' ~ event_kind) }}
      {% endif %}
    ) eligible_events
    {% if event_kind == 'identity' %}where snowplow_id is not null{% endif %})
  {%- endset -%}
  {{ return(sql) }}
{% endmacro %}


{#
  Log the window a model is about to process, replacing the manifest framework's
  print_run_limits. Bounds in the predicate are subqueries, so this resolves the real dates
  with one query. Restricted to `dbt run` / `dbt build`: during unit tests is_incremental() is
  overridden while {{ this }} may not exist.
#}

{% macro log_incremental_window(start_date, lookback, backfill, anchor_relation=none, atomic_event=none) %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set which = flags.WHICH | default('', true) -%}
  {%- if which not in ['run', 'build'] -%}{{ return('') }}{%- endif -%}

  {%- set start_ts = "cast('" ~ start_date ~ "' as timestamp)" -%}
  {%- set wm_day = snowplow_identities.watermark_day(this, start_ts) -%}

  {%- set query -%}
    select
      cast({{ dbt.dateadd('day', 1 - lookback, wm_day) }} as {{ dbt.type_string() }}) as lower_bound,
      cast({{ snowplow_identities.upper_bound_day(this, anchor_relation, start_ts, backfill, atomic_event) }} as {{ dbt.type_string() }}) as upper_bound,
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
    ~ ". Source gaps or an upstream anchor may extend this initial window. Re-run to advance further."
  ) }}
{% endmacro %}
