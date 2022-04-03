{% materialization sql_function, default %}

    CREATE OR REPLACE FUNCTION {{ relation }}(
        {% for var, _type in model["args"].items() %}
            {{ var }} {{ _type }} {{ ", " if not loop.last else "" }}
        {% endfor %}
    )
    RETURNS TABLE(
        {% for var, _type in model["return"].items() %}
            {{ var }} {{ _type }} {{ ", " if not loop.last else "" }}
        {% endfor %}
    )
    LANGUAGE sql
    STABLE
        
    AS $function$

        {{ sql }}

    $function$;

{% endmaterialization %}
