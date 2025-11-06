CREATE TABLE host_activity_reduced (
    month date NOT NULL,
    host TEXT NOT NULL,
    hit_array INT[] NOT NULL,
    unique_visitors INT[] NOT NULL,
    curr_date DATE NOT NULL,
    PRIMARY KEY (month, host, curr_date)
)