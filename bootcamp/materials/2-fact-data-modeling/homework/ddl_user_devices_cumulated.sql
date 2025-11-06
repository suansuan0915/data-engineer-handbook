CREATE TABLE user_devices_cumulated (
    user_id TEXT NOT NULL,
    browser_type TEXT NOT NULL,
    dates_active DATE[] NOT NULL DEFAULT ARRAY[]::date[],
    curr_date DATE NOT NULL,
    PRIMARY KEY (user_id, browser_type, curr_date)
)

