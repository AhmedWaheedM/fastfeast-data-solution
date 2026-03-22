## Pipeline Notes

* **Profiling layer**:
  There is no profiling layer yet. It should contain **data profiling** and **validation scripts**.

* **Check-point**:
  Used to track the **last processed files**. This can be stored either as:

  * A **text file** (`check_point.txt`)
  * Or a **table in the database**

* **Poll interval**:

  * Used to set how often the pipeline **checks for new files** in the directory.
  * Best practice is to choose a **small interval** to reduce latency.

* **External files folder**:
  We need a folder to store **external files** such as `check_point.txt` and `pipeline.log`.

* **@dataclass**:
  This module provides a decorator and functions to automatically add special methods such as `__init__()` and `__repr__()` to user-defined classes.

* **config.py**:
  Contains the `load()` function which **reads all settings** from `config.yaml` and returns them as Python objects.

* **Logging level**:

  * `INFO` → logs all information and errors (but not debug messages).
  * `DEBUG` → logs detailed debug messages.

---

## Config.yaml Sections

* **database**:

  * Using **DuckDB**, we only need **database name** and **file name**.
  * It is **not related to any server**.

* **paths**:

  * Create paths for files that are **frequently used** in the code.

* **pipeline**:

  * Contains parameters such as **batch size**.
  * These values may **change after testing** with real data to improve performance.

* **format**:

  * Ensures all files are **converted to the same format**.

* **datetime_handling**:

  * Needed to unify all dates.
  * Example: ingestion stopped on `resturant.json` because it had **two different date formats**.

* **logging**:

  * We need to **rotate logs** after a certain period.
  * Best practice: define rotation in the config file.

* **alerts**:

  * Send alerts **only on failure** to the specified email.

* **stream**:

  * Uses `poll_interval` to **check for new files** every 30 seconds.

* **batch**:

  * Checks for new files **once per day**.


