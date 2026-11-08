CREATE TABLE IF NOT EXISTS reservation_kernel_models (
    project_slug   TEXT PRIMARY KEY,
    model_json     TEXT NOT NULL,
    updated_ts_us  INTEGER NOT NULL
);
