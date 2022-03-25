{% macro log_run_completion(results) -%}

  {% if execute %}
    {{ log("Executing On Run End",info=True) }}
    {% if flags.WHICH == 'run' %}
      {%- set run_model = namespace(model=model.name) -%}

      {% for res in results -%}

        {%- set run_status = namespace(status='COMPLETE') -%}
        {%- set run_model.model = res.node.name -%}
        {{ log("Running log_run_completion for model: " ~ run_model.model,info=True) }}

        {%- set exec_state = res.status -%}
        {% if exec_state == 'error' %}
          {{ log("An error ocurred in model: " ~ run_model.model,info=True) }}
          {{ log("Error trace: " ~ res.message,info=True) }}
          {%- set run_status.status = 'ERROR' -%}
        {% endif %}
        {% if exec_state == 'skipped' %}
          {{ log("A model was Skipped: " ~ run_model.model,info=True) }}
          {%- set run_status.status = 'ERROR' -%}
        {% endif %}

        INSERT INTO {{ source('util_hub', 'dbt_run_status_log_f') }} (
        SELECT 
          '{{ run_model.model }}'
          , {{ var('run_id') }} AS run_id 
          , '{{ run_status.status }}' AS status 
          , CURRENT_TIMESTAMP AS elt_ts
          , 'On-run-end'
          FROM DUAL
      );  

      {% endfor %}
    {% endif %}
  {% endif %}
{%- endmacro %}