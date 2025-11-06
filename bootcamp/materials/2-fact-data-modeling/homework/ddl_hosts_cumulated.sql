CREATE TABLE hosts_cumulated (
    host TEXT NOT NULL,
    url TEXT NOT NULL,
    host_activity_datelist DATE[] NOT NULL DEFAULT ARRAY[]::DATE[],
    curr_date DATE NOT NULL,
    PRIMARY KEY (host, url, curr_date)
)

-- drop table hosts_cumulated;
