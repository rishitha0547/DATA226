{% snapshot final_f1_snapshot %}

{{
    config(
        target_database = 'USER_DB_FERRET',
        target_schema   = 'SNAPSHOTS',
        strategy        = 'check',
        unique_key      = 'meeting_key || session_key || driver_number || lap_number',
        check_cols = [
            'best_lap_time',
            'avg_lap_time',
            'best_position',
            'worst_position',
            'avg_psi',
            'avg_degradation',
            'avg_performance_score',
            'pit_stop_count',
            'lap_start_time',
            'lap_time',
            'race_position',
            'pace_momentum',
            'pace_stability_index',
            'degradation_index',
            'position_momentum',
            'performance_score_raw',
            'pace_state',
            'track_position_state'
        ]
    )
}}

SELECT *
FROM {{ ref('final_f1') }}

{% endsnapshot %}