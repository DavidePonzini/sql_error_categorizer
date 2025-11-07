def UNIQUE_COLUMNS(schema_name: str = '%') -> str:
    return f'''
    SELECT
        kcu.table_schema AS schema_name,
        kcu.table_name,
        tc.constraint_type,
        array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.constraint_schema = kcu.constraint_schema
    WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
    AND kcu.table_schema LIKE '{schema_name}'
    GROUP BY
        kcu.table_schema,
        kcu.table_name,
        kcu.constraint_name,
        tc.constraint_type;
'''

def COLUMNS(schema_name: str = '%') -> str:
    return f'''
    SELECT
        cols.table_schema AS schema_name,
        cols.table_name,
        cols.column_name,
        cols.data_type AS column_type,
        cols.numeric_precision,
        cols.numeric_scale,
        (cols.is_nullable = 'YES') AS is_nullable,
        fk.foreign_table_schema AS foreign_key_schema,
        fk.foreign_table_name AS foreign_key_table,
        fk.foreign_column_name AS foreign_key_column
    FROM information_schema.columns AS cols

    -- Foreign Key
    LEFT JOIN (
        SELECT
            kcu.table_schema,
            kcu.table_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name   AS foreign_table_name,
            ccu.column_name  AS foreign_column_name
        FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.constraint_schema = kcu.constraint_schema
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
            JOIN information_schema.constraint_column_usage AS ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
    ) fk ON fk.table_schema = cols.table_schema
        AND fk.table_name = cols.table_name
        AND fk.column_name = cols.column_name

    WHERE cols.table_schema LIKE '{schema_name}'
'''