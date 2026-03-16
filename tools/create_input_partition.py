from sqlalchemy import Engine, text


def create_input_partition(databaseEngine: Engine) -> None:
    """Create the PostgreSQL function that automatically creates the next
    monthly inputs partition based on acquiredTime."""

    with databaseEngine.connect() as connection:

        connection.execute(text("""
            CREATE OR REPLACE FUNCTION create_next_inputs_partition()
            RETURNS void
            LANGUAGE plpgsql
            AS $$
            DECLARE
                start_date DATE;
                end_date DATE;
                partition_name TEXT;
            BEGIN

                -- start of next month
                start_date := date_trunc('month', CURRENT_DATE + interval '1 month');

                -- end of next month
                end_date := start_date + interval '1 month';

                -- partition name
                partition_name := 'inputs_' || to_char(start_date, 'YYYY_MM');

                -- create partition if it does not exist
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS public.%I
                     PARTITION OF public.inputs
                     FOR VALUES FROM (%L) TO (%L)',
                    partition_name,
                    start_date,
                    end_date
                );

            END;
            $$;
        """))

        connection.commit()